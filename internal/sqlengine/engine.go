package sqlengine

import (
	"context"
	"fmt"
	"sync"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
)

// Engine is a query engine: for one tenant (Store set), or generic, serving
// any tenant named by the connecting user (slice 7).
type Engine struct {
	Store    *Store
	Provider *Provider
	SQL      *sqle.Engine

	// Generic engines: tenants by partition, created on first connect.
	router   Router
	createDB string
	mu       sync.Mutex
	tenants  map[uint64]*Store
}

var registerVarsOnce sync.Once

// registerVars replaces go-mysql-server's placeholder definition of
// innodb_lock_wait_timeout (global-only, fixed at 1) with MySQL's: session
// and global, 1 to 1073741824 seconds, default 50 (D5).
func registerVars() {
	registerVarsOnce.Do(func() {
		sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
			&sql.MysqlSystemVariable{
				Name:    "innodb_lock_wait_timeout",
				Scope:   sql.GetMysqlScope(sql.SystemVariableScope_Both),
				Dynamic: true,
				Type:    types.NewSystemIntType("innodb_lock_wait_timeout", 1, 1073741824, false),
				Default: int64(50),
			},
		})
	})
}

// NewEngine builds a go-mysql-server engine over the store.
func NewEngine(store *Store) *Engine {
	registerVars()
	pro := NewProvider(store)
	return &Engine{Store: store, Provider: pro, SQL: sqle.NewDefault(pro)}
}

// NewGenericEngine builds an engine that serves any tenant. A connection
// picks its tenant with the user name TenantUser(partition), which the proxy
// uses; createDB, if set, is created in each tenant on its first connect.
func NewGenericEngine(r Router, createDB string) *Engine {
	registerVars()
	pro := NewProvider(nil)
	return &Engine{Provider: pro, SQL: sqle.NewDefault(pro), router: r, createDB: createDB, tenants: map[uint64]*Store{}}
}

// TenantUser is the user name that selects a tenant on a generic engine.
func TenantUser(partition uint64) string { return fmt.Sprintf("s2z_p%d", partition) }

func parseTenantUser(user string) (uint64, bool) {
	var p uint64
	if n, err := fmt.Sscanf(user, "s2z_p%d", &p); err != nil || n != 1 || TenantUser(p) != user {
		return 0, false
	}
	return p, true
}

// tenant returns the store for a partition on a generic engine.
func (e *Engine) tenant(ctx context.Context, partition uint64) (*Store, error) {
	e.mu.Lock()
	s, ok := e.tenants[partition]
	if !ok {
		s = NewRoutedStore(e.router, partition)
		e.tenants[partition] = s
	}
	e.mu.Unlock()
	if !ok && e.createDB != "" {
		if err := ensureDatabase(ctx, s, e.createDB); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// SessionBuilder creates a Session for each MySQL connection.
func (e *Engine) SessionBuilder() server.SessionBuilder {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host, user := "", conn.User
		if u, ok := conn.UserData.(sql.MysqlConnectionUser); ok {
			host, user = u.Host, u.User
		}
		store := e.Store
		if e.tenants != nil {
			pid, ok := parseTenantUser(user)
			if !ok {
				return nil, fmt.Errorf("generic query engine: user %q names no tenant", user)
			}
			var err error
			if store, err = e.tenant(ctx, pid); err != nil {
				return nil, err
			}
		}
		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		return NewSession(sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID), store), nil
	}
}

// NewServer returns a MySQL protocol server for the engine.
func (e *Engine) NewServer(addr string) (*server.Server, error) {
	cfg := server.Config{Protocol: "tcp", Address: addr}
	return server.NewServer(cfg, e.SQL, sql.NewContext, e.SessionBuilder(), nil)
}

// EnsureDatabase creates a database in a single-tenant engine's tenant.
func (e *Engine) EnsureDatabase(ctx context.Context, name string) error {
	return ensureDatabase(ctx, e.Store, name)
}

func ensureDatabase(ctx context.Context, s *Store, name string) error {
	d, err := s.catalog.Database(ctx, name)
	if err != nil || d != nil {
		return err
	}
	err = s.catalog.CreateDatabase(ctx, name, sql.Collation_Default)
	if sql.ErrDatabaseExists.Is(err) {
		return nil
	}
	return err
}
