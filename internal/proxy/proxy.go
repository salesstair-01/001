package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	querypb "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// Config configures a Proxy.
type Config struct {
	Tenants *Tenants
	Pool    *Pool
	// TenantUser names the backend user that selects a tenant on a generic
	// query engine (sqlengine.TenantUser).
	TenantUser func(partition uint64) string
	// BackendIdle closes a session's query-engine connection after this long
	// without statements, unless the session is in a transaction or pinned,
	// so query engines can scale to zero (default 30 s).
	BackendIdle time.Duration
	TLS         *tls.Config // nil = no TLS
	RequireTLS  bool
	Logger      *slog.Logger
}

// Proxy is the MySQL-protocol front end.
type Proxy struct {
	cfg Config
	l   *mysql.Listener
	log *slog.Logger

	mu       sync.Mutex
	sessions map[uint32]*session
	stop     chan struct{}
}

// session is one client connection's state, kept while its backend comes and goes.
type session struct {
	mu        sync.Mutex
	principal *principal
	db        string
	sets      []setStmt // replayed in order onto every new backend
	backend   *backend
	inTxn     bool   // the backend reported SERVER_STATUS_IN_TRANS
	pinned    string // why the backend may not be released ("" = it may)
	lastUsed  time.Time
	// coldStart is when the statement that opened the current backend began;
	// the statement's result records the cold-start latency.
	coldStart  time.Time
	coldEngine string
}

type setStmt struct {
	key  string // what it sets, so a later SET of the same variable replaces it
	text string
}

type backend struct {
	conn   *mysql.Conn
	engine *Engine
}

// New starts a proxy listening on addr.
func New(addr string, cfg Config) (*Proxy, error) {
	if cfg.BackendIdle == 0 {
		cfg.BackendIdle = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	p := &Proxy{cfg: cfg, log: cfg.Logger, sessions: map[uint32]*session{}, stop: make(chan struct{})}
	l, err := mysql.NewListenerWithConfig(mysql.ListenerConfig{
		Protocol: "tcp", Address: addr, AuthServer: newAuthServer(cfg.Tenants), Handler: p,
		ConnReadBufferSize: mysql.DefaultConnBufferSize,
	})
	if err != nil {
		return nil, err
	}
	l.ServerVersion = "8.0.33-s2z"
	l.TLSConfig = cfg.TLS
	l.RequireSecureTransport = cfg.RequireTLS
	p.l = l
	go l.Accept()
	go p.reaper()
	return p, nil
}

// Addr returns the listen address.
func (p *Proxy) Addr() string { return p.l.Addr().String() }

// Close stops accepting connections and closes every backend.
func (p *Proxy) Close() {
	close(p.stop)
	p.l.Close()
	p.mu.Lock()
	ss := make([]*session, 0, len(p.sessions))
	for _, s := range p.sessions {
		ss = append(ss, s)
	}
	p.mu.Unlock()
	for _, s := range ss {
		s.mu.Lock()
		p.dropBackend(s)
		s.mu.Unlock()
	}
}

// Backends returns how many sessions currently hold a query-engine connection.
func (p *Proxy) Backends() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, s := range p.sessions {
		s.mu.Lock()
		if s.backend != nil {
			n++
		}
		s.mu.Unlock()
	}
	return n
}

// reaper closes idle backends so query engines can scale to zero.
func (p *Proxy) reaper() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
		}
		p.mu.Lock()
		ss := make([]*session, 0, len(p.sessions))
		for _, s := range p.sessions {
			ss = append(ss, s)
		}
		p.mu.Unlock()
		for _, s := range ss {
			if !s.mu.TryLock() { // busy running a statement
				continue
			}
			if s.backend != nil && !s.inTxn && s.pinned == "" && time.Since(s.lastUsed) > p.cfg.BackendIdle {
				p.dropBackend(s)
			}
			s.mu.Unlock()
		}
	}
}

// ---- mysql.Handler ------------------------------------------------------------

func (p *Proxy) NewConnection(c *mysql.Conn) {
	p.mu.Lock()
	p.sessions[c.ConnectionID] = &session{lastUsed: time.Now()}
	p.mu.Unlock()
}

func (p *Proxy) ConnectionClosed(c *mysql.Conn) {
	p.mu.Lock()
	s := p.sessions[c.ConnectionID]
	delete(p.sessions, c.ConnectionID)
	p.mu.Unlock()
	if s != nil {
		s.mu.Lock()
		p.dropBackend(s)
		s.mu.Unlock()
	}
}

func (p *Proxy) ConnectionAborted(*mysql.Conn, string) error { return nil }

func (p *Proxy) session(c *mysql.Conn) (*session, error) {
	p.mu.Lock()
	s := p.sessions[c.ConnectionID]
	p.mu.Unlock()
	if s == nil {
		return nil, errors.New("proxy: unknown connection")
	}
	if s.principal == nil {
		pr, ok := c.UserData.(*principal)
		if !ok {
			return nil, errAccessDenied(c.User)
		}
		s.principal = pr
	}
	return s, nil
}

// ComInitDB records the database; it is applied when a backend is next
// needed, so connecting with a database doesn't wake compute.
func (p *Proxy) ComInitDB(c *mysql.Conn, db string) error {
	s, err := p.session(c)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.backend != nil {
		if _, _, err := s.backend.conn.ExecuteFetchMulti(context.Background(), "USE "+sqlparser.String(sqlparser.NewTableIdent(db)), 1, false); err != nil {
			return p.backendErr(s, err)
		}
	}
	s.db = db
	return nil
}

func (p *Proxy) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	s, err := p.session(c)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := p.run(ctx, c, s, query)
	if err != nil {
		return err
	}
	return callback(res, false)
}

func (p *Proxy) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	pieces, err := sqlparser.SplitStatementToPieces(query)
	if err != nil || len(pieces) <= 1 {
		return "", p.ComQuery(ctx, c, query, callback)
	}
	rest := strings.Join(pieces[1:], ";")
	s, err := p.session(c)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	res, err := p.run(ctx, c, s, pieces[0])
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	return rest, callback(res, true)
}

// ComPrepare keeps the statement at the proxy: there is no server-side
// statement to lose when the backend changes, so nothing has to be
// re-prepared on reconnect. Executes run the statement as text.
func (p *Proxy) ComPrepare(_ context.Context, c *mysql.Conn, _ string, _ *mysql.PrepareData) ([]*querypb.Field, error) {
	_, err := p.session(c)
	return nil, err
}

func (p *Proxy) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	s, err := p.session(c)
	if err != nil {
		return err
	}
	stmt, err := sqlparser.Parse(prepare.PrepareStmt)
	if err != nil {
		return err
	}
	query, err := sqlparser.NewParsedQuery(stmt).GenerateQuery(prepare.BindVars, nil)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := p.run(ctx, c, s, query)
	if err != nil {
		return err
	}
	return callback(res)
}

func (p *Proxy) WarningCount(*mysql.Conn) uint16 { return 0 }

func (p *Proxy) ComResetConnection(c *mysql.Conn) error {
	s, err := p.session(c)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p.dropBackend(s)
	s.sets, s.pinned, s.inTxn = nil, "", false
	return nil
}

func (p *Proxy) ParserOptionsForConnection(*mysql.Conn) (sqlparser.ParserOptions, error) {
	return sqlparser.ParserOptions{}, nil
}

// ---- statements -------------------------------------------------------------

// Health checks that pools send; answered here so they never wake compute.
var pingQuery = regexp.MustCompile(`(?is)^\s*(/\*.*?\*/\s*)*select\s+1\s*(from\s+dual\s*)?;?\s*$`)

func oneRow() *sqltypes.Result {
	return &sqltypes.Result{
		Fields: []*querypb.Field{{Name: "1", Type: querypb.Type_INT64, Charset: mysql.CharacterSetBinary, ColumnLength: 1}},
		Rows:   [][]sqltypes.Value{{sqltypes.NewInt64(1)}},
	}
}

// run executes one statement for a session; s.mu is held.
func (p *Proxy) run(ctx context.Context, c *mysql.Conn, s *session, query string) (*sqltypes.Result, error) {
	s.lastUsed = time.Now()
	if pingQuery.MatchString(query) {
		return oneRow(), nil
	}
	stmt, perr := sqlparser.Parse(query)
	if perr == nil {
		switch st := stmt.(type) {
		case *sqlparser.Set:
			return p.runSet(ctx, c, s, st, query)
		case *sqlparser.Use:
			db := st.DBName.String()
			if s.backend == nil {
				// Checked when a backend is next needed.
				s.db = db
				return &sqltypes.Result{}, nil
			}
			res, err := p.forward(ctx, c, s, query)
			if err == nil {
				s.db = db
			}
			return res, err
		}
	}
	if reason := pins(query); reason != "" && s.pinned == "" {
		s.pinned = reason
	}
	res, err := p.forward(ctx, c, s, query)
	if err != nil && isConnErr(err) && !s.inTxn && readOnly(stmt) {
		// The query engine went away between statements: a read is safe to
		// send again on a new one, with the session replayed.
		p.log.Info("query engine lost; retrying read on a new one", "err", err)
		p.dropBackend(s)
		res, err = p.forward(ctx, c, s, query)
	}
	return res, err
}

// runSet records a SET so it can be replayed onto future backends. With no
// backend it is only recorded (drivers send SETs right after connecting); a
// SET that the query engine then rejects fails the next statement.
func (p *Proxy) runSet(ctx context.Context, c *mysql.Conn, s *session, st *sqlparser.Set, query string) (*sqltypes.Result, error) {
	key := setKey(st)
	if strings.Contains(strings.ToLower(query), "autocommit") || s.backend != nil {
		// autocommit changes transaction behavior, so it is applied at once.
		res, err := p.forward(ctx, c, s, query)
		if err != nil {
			return nil, err
		}
		s.record(key, query)
		return res, nil
	}
	s.record(key, query)
	return &sqltypes.Result{}, nil
}

func (s *session) record(key, text string) {
	for i, old := range s.sets {
		if old.key == key && key != "" {
			s.sets = append(s.sets[:i], s.sets[i+1:]...)
			break
		}
	}
	s.sets = append(s.sets, setStmt{key: key, text: text})
}

func setKey(st *sqlparser.Set) string {
	var names []string
	for _, e := range st.Exprs {
		names = append(names, strings.ToLower(fmt.Sprint(e.Scope)+"."+sqlparser.String(e.Name)))
	}
	return strings.Join(names, ",")
}

// pins reports statements whose effects live in the query engine's memory:
// the session must keep its backend (and so its engine) from then on.
var pinPatterns = []struct {
	re     *regexp.Regexp
	reason string
}{
	{regexp.MustCompile(`(?i)\bcreate\s+temporary\s+table\b`), "temporary table"},
	{regexp.MustCompile(`(?i)\bget_lock\s*\(`), "GET_LOCK"},
	{regexp.MustCompile(`(?i)^\s*lock\s+tables?\b`), "LOCK TABLES"},
	{regexp.MustCompile(`(?i)^\s*prepare\s+\w+\s+from\b`), "SQL PREPARE"},
	{regexp.MustCompile(`(?i)select\b.*@\w+\s*:=`), "user variable assigned in a query"},
}

func pins(query string) string {
	for _, p := range pinPatterns {
		if p.re.MatchString(query) {
			return p.reason
		}
	}
	return ""
}

func readOnly(stmt sqlparser.Statement) bool {
	switch stmt.(type) {
	case *sqlparser.Select, *sqlparser.Show, *sqlparser.Explain:
		return true
	}
	return false
}

// forward sends a statement to the session's backend, connecting one first.
func (p *Proxy) forward(ctx context.Context, c *mysql.Conn, s *session, query string) (*sqltypes.Result, error) {
	if err := p.ensureBackend(ctx, s); err != nil {
		return nil, err
	}
	res, status, err := s.backend.conn.ExecuteFetchMulti(ctx, query, 1<<30, true)
	if err != nil {
		return nil, p.backendErr(s, err)
	}
	s.inTxn = uint16(status)&mysql.ServerInTransaction != 0
	c.StatusFlags = uint16(status) & (mysql.ServerInTransaction | mysql.ServerStatusAutocommit)
	s.lastUsed = time.Now()
	queriesTotal.Inc(s.principal.tenant.Name)
	if !s.coldStart.IsZero() {
		coldStartSeconds.ObserveSince(s.coldStart, s.coldEngine)
		s.coldStart = time.Time{}
	}
	return res, nil
}

// ensureBackend connects the session to a query engine: the client socket is
// held while one starts, and the session (database, SETs) is replayed.
func (p *Proxy) ensureBackend(ctx context.Context, s *session) error {
	if s.backend != nil {
		return nil
	}
	start := time.Now()
	engine := "running"
	if p.cfg.Pool.Running() == 0 {
		engine = "started"
	}
	e, err := p.cfg.Pool.Acquire(ctx)
	if err != nil {
		return err
	}
	host, portStr, _ := net.SplitHostPort(e.Addr)
	port, _ := strconv.Atoi(portStr)
	params := &mysql.ConnParams{Host: host, Port: port, Uname: p.cfg.TenantUser(s.principal.tenant.Partition), Charset: "utf8mb4"}
	cctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	conn, err := mysql.Connect(cctx, params)
	cancel()
	if err != nil {
		p.cfg.Pool.Release(e)
		if isConnErr(err) {
			p.cfg.Pool.Failed(e)
		}
		return fmt.Errorf("proxy: connect to query engine %s: %w", e.Addr, err)
	}
	b := &backend{conn: conn, engine: e}
	replay := make([]string, 0, len(s.sets)+1)
	if s.db != "" {
		replay = append(replay, "USE "+sqlparser.String(sqlparser.NewTableIdent(s.db)))
	}
	for _, st := range s.sets {
		replay = append(replay, st.text)
	}
	for i, q := range replay {
		if _, _, err := conn.ExecuteFetchMulti(ctx, q, 1, false); err != nil {
			conn.Close()
			p.cfg.Pool.Release(e)
			if i == 0 && s.db != "" {
				s.db = "" // don't fail every statement on a database that is gone
			} else {
				s.dropSet(q) // a SET the query engine rejects is reported once
			}
			return err
		}
	}
	s.backend = b
	s.inTxn = false
	s.coldStart, s.coldEngine = start, engine
	return nil
}

func (s *session) dropSet(text string) {
	for i, st := range s.sets {
		if st.text == text {
			s.sets = append(s.sets[:i], s.sets[i+1:]...)
			return
		}
	}
}

// backendErr passes SQL errors through and drops a broken connection. A
// transaction on a lost backend is gone, reported as MySQL does for a
// connection-level rollback.
func (p *Proxy) backendErr(s *session, err error) error {
	if !isConnErr(err) {
		return err
	}
	inTxn := s.inTxn
	p.dropBackend(s)
	if inTxn {
		return mysql.NewSQLError(mysql.ERLockDeadlock, mysql.SSLockDeadlock,
			"query engine lost; the transaction was rolled back, try restarting transaction")
	}
	return err
}

func (p *Proxy) dropBackend(s *session) {
	if s.backend == nil {
		return
	}
	s.backend.conn.Close()
	p.cfg.Pool.Release(s.backend.engine)
	s.backend = nil
	s.inTxn = false
	s.pinned = ""
}

func isConnErr(err error) bool {
	var se *mysql.SQLError
	if errors.As(err, &se) {
		return se.Num == mysql.CRServerGone || se.Num == mysql.CRServerLost || se.Num == mysql.CRConnectionError ||
			se.Num == mysql.CRConnHostError
	}
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, net.ErrClosed)
}
