// Package proxy is the always-on MySQL proxy (slice 7). It terminates client
// connections, TLS and authentication, finds the tenant from the user name or
// TLS hostname, and holds client sockets while query engines and KV owners
// sleep: it answers pings itself, tracks session state and replays it onto a
// new query engine, and manages a pool of generic query-engine processes.
package proxy

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/dolthub/vitess/go/mysql"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
)

// Tenant is one tenant: a partition and its MySQL users.
type Tenant struct {
	Name      string            `json:"name"`
	Partition uint64            `json:"partition"`
	Users     map[string]string `json:"users"` // user -> password
}

// Tenants is the proxy's tenant table. A client names its tenant with the
// user name "user@tenant", or by connecting over TLS to "tenant.<Domain>".
type Tenants struct {
	byName map[string]*Tenant
	Domain string
}

// NewTenants builds a tenant table.
func NewTenants(domain string, ts ...Tenant) *Tenants {
	t := &Tenants{byName: map[string]*Tenant{}, Domain: domain}
	for i := range ts {
		tt := ts[i]
		t.byName[strings.ToLower(tt.Name)] = &tt
	}
	return t
}

// LoadTenants reads a JSON array of tenants.
func LoadTenants(path, domain string) (*Tenants, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ts []Tenant
	if err := json.Unmarshal(b, &ts); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return NewTenants(domain, ts...), nil
}

// principal is an authenticated client: the conn's UserData.
type principal struct {
	tenant *Tenant
	user   string
}

func (p *principal) Get() *querypb.VTGateCallerID {
	return &querypb.VTGateCallerID{Username: p.user, Groups: []string{p.tenant.Name}}
}

// lookup splits "user@tenant" and finds the tenant and password.
func (t *Tenants) lookup(login string) (*principal, string, bool) {
	at := strings.LastIndex(login, "@")
	if at <= 0 {
		return nil, "", false
	}
	user, name := login[:at], login[at+1:]
	tn, ok := t.byName[strings.ToLower(name)]
	if !ok {
		return nil, "", false
	}
	pw, ok := tn.Users[user]
	if !ok {
		return nil, "", false
	}
	return &principal{tenant: tn, user: user}, pw, true
}

// sniTenant returns the tenant named by the TLS hostname, if any.
func (t *Tenants) sniTenant(c *mysql.Conn) string {
	tc, ok := c.Conn.(*tls.Conn)
	if !ok || t.Domain == "" {
		return ""
	}
	host := strings.ToLower(tc.ConnectionState().ServerName)
	suffix := "." + strings.ToLower(t.Domain)
	if !strings.HasSuffix(host, suffix) {
		return ""
	}
	return strings.TrimSuffix(host, suffix)
}

// qualify adds the TLS hostname's tenant to a bare user name.
func (t *Tenants) qualify(c *mysql.Conn, user string) string {
	if strings.Contains(user, "@") {
		return user
	}
	if tn := t.sniTenant(c); tn != "" {
		return user + "@" + tn
	}
	return user
}

// authServer authenticates against the tenant table with
// caching_sha2_password (MySQL 8's default; its fast path works without TLS
// because the proxy knows the password hash) and mysql_native_password.
type authServer struct {
	t       *Tenants
	methods []mysql.AuthMethod
}

func newAuthServer(t *Tenants) *authServer {
	a := &authServer{t: t}
	a.methods = []mysql.AuthMethod{
		&qualifying{mysql.NewSha2CachingAuthMethod(a, a, a), t},
		&qualifying{mysql.NewMysqlNativeAuthMethod(a, a), t},
	}
	return a
}

func (a *authServer) AuthMethods() []mysql.AuthMethod { return a.methods }
func (a *authServer) DefaultAuthMethodDescription() mysql.AuthMethodDescription {
	return mysql.CachingSha2Password
}

// HandleUser implements mysql.UserValidator.
func (a *authServer) HandleUser(user string, _ net.Addr) bool {
	_, _, ok := a.t.lookup(user)
	return ok
}

// UserEntryWithCacheHash is the caching_sha2_password fast path.
func (a *authServer) UserEntryWithCacheHash(_ []*x509.Certificate, salt []byte, user string, reply []byte, _ net.Addr) (mysql.Getter, mysql.CacheState, error) {
	p, pw, ok := a.t.lookup(user)
	if !ok {
		return nil, mysql.AuthRejected, errAccessDenied(user)
	}
	h1 := sha256.Sum256([]byte(pw))
	h2 := sha256.Sum256(h1[:])
	if !mysql.VerifyHashedCachingSha2Password(reply, salt, h2[:]) {
		return nil, mysql.AuthRejected, errAccessDenied(user)
	}
	return p, mysql.AuthAccepted, nil
}

// UserEntryWithPassword is the caching_sha2_password full path (over TLS).
func (a *authServer) UserEntryWithPassword(_ []*x509.Certificate, user, password string, _ net.Addr) (mysql.Getter, error) {
	p, pw, ok := a.t.lookup(user)
	if !ok || subtle.ConstantTimeCompare([]byte(pw), []byte(password)) != 1 {
		return nil, errAccessDenied(user)
	}
	return p, nil
}

// UserEntryWithHash is mysql_native_password.
func (a *authServer) UserEntryWithHash(_ []*x509.Certificate, salt []byte, user string, reply []byte, _ net.Addr) (mysql.Getter, error) {
	p, pw, ok := a.t.lookup(user)
	if !ok {
		return nil, errAccessDenied(user)
	}
	h1 := sha1.Sum([]byte(pw))
	h2 := sha1.Sum(h1[:])
	if !mysql.VerifyHashedMysqlNativePassword(reply, salt, h2[:]) {
		return nil, errAccessDenied(user)
	}
	return p, nil
}

func errAccessDenied(user string) error {
	return mysql.NewSQLError(mysql.ERAccessDeniedError, mysql.SSAccessDeniedError, "Access denied for user '%s'", user)
}

// qualifying wraps an auth method so a bare user name connecting to
// "tenant.<domain>" over TLS authenticates as "user@tenant".
type qualifying struct {
	mysql.AuthMethod
	t *Tenants
}

func (q *qualifying) HandleUser(c *mysql.Conn, user string) bool {
	return q.AuthMethod.HandleUser(c, q.t.qualify(c, user))
}

func (q *qualifying) HandleAuthPluginData(c *mysql.Conn, user string, server, client []byte, addr net.Addr) (mysql.Getter, error) {
	return q.AuthMethod.HandleAuthPluginData(c, q.t.qualify(c, user), server, client, addr)
}
