// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"os"
)

func init() {
	sql.Register("sqlite3", &Driver{})
	if os.Getenv("SQLITE_LOG") != "" {
		ConfigLog(func(d interface{}, err error, msg string) {
			log.Printf("%s: %s, %s\n", d, err, msg)
		}, "SQLITE")
	}
}

// Adapter to database/sql/driver
type Driver struct {
}
type connImpl struct {
	c *Conn
}
type stmtImpl struct {
	s            *Stmt
	rowsRef      bool // true if there is a rowsImpl associated to this statement that has not been closed.
	pendingClose bool
}
type rowsImpl struct {
	s           *stmtImpl
	columnNames []string // cache
}

// Open opens a new database connection.
// ":memory:" for memory db,
// "" for temp file db
// TODO How to specify open flags?
func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := Open(name)
	if err != nil {
		return nil, err
	}
	c.BusyTimeout(500)
	return &connImpl{c}, nil
}

// PRAGMA schema_version may be used to detect when the database schema is altered

func (c *connImpl) Exec(query string, args []driver.Value) (driver.Result, error) {
	// http://code.google.com/p/go-wiki/wiki/InterfaceSlice
	tmp := make([]interface{}, len(args))
	for i, arg := range args {
		tmp[i] = arg
	}
	if err := c.c.Exec(query, tmp...); err != nil {
		return nil, err
	}
	return c, nil // FIXME RowAffected/noRows
}

// TODO How to know that the last Stmt has done an INSERT? An authorizer?
func (c *connImpl) LastInsertId() (int64, error) {
	return c.c.LastInsertRowid(), nil
}

// TODO How to know that the last Stmt has done a DELETE/INSERT/UPDATE? An authorizer?
func (c *connImpl) RowsAffected() (int64, error) {
	return int64(c.c.Changes()), nil
}

func (c *connImpl) Prepare(query string) (driver.Stmt, error) {
	s, err := c.c.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmtImpl{s: s}, nil
}

func (c *connImpl) Close() error {
	return c.c.Close()
}

func (c *connImpl) Begin() (driver.Tx, error) {
	if err := c.c.Begin(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *connImpl) Commit() error {
	return c.c.Commit()
}
func (c *connImpl) Rollback() error {
	return c.c.Rollback()
}

func (s *stmtImpl) Close() error {
	if s.rowsRef { // Currently, it never happens because the sql.Stmt doesn't call driver.Stmt in this case
		s.pendingClose = true
		return nil
	}
	return s.s.Finalize()
}

func (s *stmtImpl) NumInput() int {
	return s.s.BindParameterCount()
}

func (s *stmtImpl) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.bind(args); err != nil {
		return nil, err
	}
	if err := s.s.exec(); err != nil {
		return nil, err
	}
	return s, nil // FIXME RowAffected/noRows
}

// TODO How to know that this Stmt has done an INSERT? An authorizer?
func (s *stmtImpl) LastInsertId() (int64, error) {
	return s.s.c.LastInsertRowid(), nil
}

// TODO How to know that this Stmt has done a DELETE/INSERT/UPDATE? An authorizer?
func (s *stmtImpl) RowsAffected() (int64, error) {
	return int64(s.s.c.Changes()), nil
}

func (s *stmtImpl) Query(args []driver.Value) (driver.Rows, error) {
	if s.rowsRef {
		return nil, errors.New("Previously returned Rows still not closed")
	}
	if err := s.bind(args); err != nil {
		return nil, err
	}
	s.rowsRef = true
	return &rowsImpl{s, nil}, nil
}

func (s *stmtImpl) bind(args []driver.Value) error {
	for i, v := range args {
		if err := s.s.BindByIndex(i+1, v); err != nil {
			return err
		}
	}
	return nil
}

func (r *rowsImpl) Columns() []string {
	if r.columnNames == nil {
		r.columnNames = r.s.s.ColumnNames()
	}
	return r.columnNames
}

func (r *rowsImpl) Next(dest []driver.Value) error {
	ok, err := r.s.s.Next()
	if err != nil {
		return err
	}
	if !ok {
		return io.EOF
	}
	for i := range dest {
		value := r.s.s.ScanValue(i)
		switch value := value.(type) {
		case string: // "All string values must be converted to []byte."
			dest[i] = []byte(value)
		default:
			dest[i] = value
		}
	}
	return nil
}

func (r *rowsImpl) Close() error {
	r.s.rowsRef = false
	if r.s.pendingClose {
		return r.s.Close()
	}
	return r.s.s.Reset()
}
