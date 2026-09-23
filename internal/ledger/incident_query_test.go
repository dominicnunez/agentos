package ledger

import (
	"context"
	"database/sql/driver"
	"fmt"
	"sync/atomic"

	"modernc.org/sqlite"
)

// Observe every statement of the public operation, including its preflights.
// Omitting the optional fast query interfaces makes database/sql use Prepare;
// the real SQLite statements and transaction implementation remain in use.
type incidentCountConn struct {
	driver.Conn
	count *atomic.Int64
}

func (c *incidentCountConn) Prepare(query string) (driver.Stmt, error) {
	c.count.Add(1)
	return c.Conn.Prepare(query)
}

func (c *incidentCountConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	begin, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, fmt.Errorf("SQLite connection lacks context transactions")
	}
	return begin.BeginTx(ctx, opts)
}

type incidentCountConnector struct {
	path  string
	count *atomic.Int64
	inner sqlite.Driver
}

func (c *incidentCountConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.inner.Open(c.path)
	if err != nil {
		return nil, err
	}
	return &incidentCountConn{Conn: conn, count: c.count}, nil
}

func (c *incidentCountConnector) Driver() driver.Driver { return &c.inner }
