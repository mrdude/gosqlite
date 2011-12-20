// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"exp/sql"
	"exp/sql/driver"
	"io"
)

func init() {
	sql.Register("sqlite3", &Driver{})
}

type Driver struct {

}
type connImpl struct {
	c *Conn
}
type stmtImpl struct {
	s *Stmt
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	c, err := Open(name)
	if err != nil {
		return nil, err
	}
	return &connImpl{c}, nil
}

func (c *connImpl) Exec(query string, args []interface{}) (driver.Result, error) {
	if err := c.c.Exec(query, args...); err != nil {
		return nil, err
	}
	return c, nil // FIXME RowAffected/ddlSuccess
}

// TODO How to know that the last Stmt did an INSERT?
func (c *connImpl) LastInsertId() (int64, error) {
	return c.c.LastInsertRowid(), nil
}

// TODO How to know that the last Stmt did an DELETE/INSERT/UPDATE?
func (c *connImpl) RowsAffected() (int64, error) {
	return int64(c.c.Changes()), nil
}

func (c *connImpl) Prepare(query string) (driver.Stmt, error) {
	s, err := c.c.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmtImpl{s}, nil
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
	return s.s.Finalize()
}

func (s *stmtImpl) NumInput() int {
	return s.s.BindParameterCount()
}

func (s *stmtImpl) Exec(args []interface{}) (driver.Result, error) {
	err := s.s.Exec(args...)
	if err != nil {
		return nil, err
	}
	return s, nil // FIXME RowAffected/ddlSuccess
}

// TODO How to know that this Stmt did an INSERT?
func (s *stmtImpl) LastInsertId() (int64, error) {
	return s.s.c.LastInsertRowid(), nil
}

// TODO How to know that this Stmt did an DELETE/INSERT/UPDATE?
func (s *stmtImpl) RowsAffected() (int64, error) {
	return int64(s.s.c.Changes()), nil
}

func (s *stmtImpl) Query(args []interface{}) (driver.Rows, error) {
	if err := s.s.Bind(args...); err != nil {
		return nil, err
	}
	return s, nil
}

// TODO Cache result?
func (s *stmtImpl) Columns() []string {
	return s.s.ColumnNames()
}

func (s *stmtImpl) Next(dest []interface{}) error {
	ok, err := s.s.Next()
	if err != nil {
		return err
	}
	if !ok {
		return io.EOF
	}
	s.s.ScanValues(dest)
	return nil
}
