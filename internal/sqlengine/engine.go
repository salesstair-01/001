package sqlengine

import (
	"context"
	"sync"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
)

// Engine is a query engine for one tenant.
type Engine struct {
	Store    *Store
	Provider *Provider
	SQL      *sqle.Engine
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

// SessionBuilder creates a Session for each MySQL connection.
func (e *Engine) SessionBuilder() server.SessionBuilder {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host, user := "", ""
		if u, ok := conn.UserData.(sql.MysqlConnectionUser); ok {
			host, user = u.Host, u.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		return NewSession(sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID), e.Store), nil
	}
}

// NewServer returns a MySQL protocol server for the engine.
func (e *Engine) NewServer(addr string) (*server.Server, error) {
	cfg := server.Config{Protocol: "tcp", Address: addr}
	return server.NewServer(cfg, e.SQL, sql.NewContext, e.SessionBuilder(), nil)
}

// EnsureDatabase creates a database if it does not exist.
func (e *Engine) EnsureDatabase(ctx context.Context, name string) error {
	d, err := e.Provider.catalog.Database(ctx, name)
	if err != nil || d != nil {
		return err
	}
	err = e.Provider.catalog.CreateDatabase(ctx, name, sql.Collation_Default)
	if sql.ErrDatabaseExists.Is(err) {
		return nil
	}
	return err
}
