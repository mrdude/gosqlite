// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"errors"
	"fmt"
)

// IntArray is the Go-language interface definition for the "intarray" or
// integer array virtual table for SQLite.
//
// The intarray virtual table is designed to facilitate using an
// aray of integers as the right-hand side of an IN operator. So
// instead of doing a prepared statement like this:
//
//	SELECT * FROM table WHERE x IN (?,?,?,...,?);
//
// And then binding indivdual integers to each of ? slots, a Go-language
// application can create an intarray object (named "ex1" in the following
// example), prepare a statement like this:
//
//	SELECT * FROM table WHERE x IN ex1;
//
// Then bind an ordinary Go slice of integer values to the ex1 object
// to run the statement.
//
// USAGE:
//
// One or more intarray objects can be created as follows:
//
//	var p1, p2, p3 IntArray
//	p1, err = db.CreateIntArray("ex1")
//	p2, err = db.CreateIntArray("ex2")
//	p3, err = db.CreateIntArray("ex3")
//
// Each call to CreateIntArray() generates a new virtual table
// module and a singleton of that virtual table module in the TEMP
// database.  Both the module and the virtual table instance use the
// name given by the second parameter.  The virtual tables can then be
// used in prepared statements:
//
//	SELECT * FROM t1, t2, t3
//	 WHERE t1.x IN ex1
//	  AND t2.y IN ex2
//	  AND t3.z IN ex3;
//
// Each integer array is initially empty.  New arrays can be bound to
// an integer array as follows:
//
//	p1.Bind([]int64{ 1, 2, 3, 4 })
//	p2.Bind([]int64{ 5, 6, 7, 8, 9, 10, 11 })
//	a3 := make([]int64, 100)
//	// Fill in content of a3
//	p3.Bind(a3)
//
// A single intarray object can be rebound multiple times.  But do not
// attempt to change the bindings of an intarray while it is in the middle
// of a query.
//
// The application must not change the intarray values while an intarray is in
// the middle of a query.
//
// The intarray object is automatically destroyed when its corresponding
// virtual table is dropped.  Since the virtual tables are created in the
// TEMP database, they are automatically dropped when the database connection
// closes so the application does not normally need to take any special
// action to free the intarray objects (except if connections are pooled...).
type IntArray interface {
	Bind(elements []int64)
	Drop() error
}

type intArray struct {
	c       *Conn
	name    string
	content []int64
}

func (m *intArray) Create(c *Conn, args []string) (VTab, error) {
	err := c.DeclareVTab("CREATE TABLE x(value INTEGER PRIMARY KEY)")
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (m *intArray) Connect(c *Conn, args []string) (VTab, error) {
	return m.Create(c, args)
}

func (m *intArray) DestroyModule() {
}

func (m *intArray) BestIndex() error {
	return nil
}
func (m *intArray) Disconnect() error {
	return nil
}
func (m *intArray) Destroy() error {
	return nil
}
func (m *intArray) Open() (VTabCursor, error) {
	return &intArrayVTabCursor{m, 0}, nil
}

type intArrayVTabCursor struct {
	vTab *intArray
	i    int /* Current cursor position */
}

func (vc *intArrayVTabCursor) Close() error {
	return nil
}
func (vc *intArrayVTabCursor) Filter() error {
	vc.i = 0
	return nil
}
func (vc *intArrayVTabCursor) Next() error {
	vc.i++
	return nil
}
func (vc *intArrayVTabCursor) EOF() bool {
	return vc.i >= len(vc.vTab.content)
}
func (vc *intArrayVTabCursor) Column(c *Context, col int) error {
	if col != 0 {
		return fmt.Errorf("column index out of bounds: %d", col)
	}
	c.ResultInt64(vc.vTab.content[vc.i])
	return nil
}
func (vc *intArrayVTabCursor) Rowid() (int64, error) {
	return int64(vc.i), nil
}

// CreateIntArray create a specific instance of an intarray object.
//
// Each intarray object corresponds to a virtual table in the TEMP table
// with the specified name.
//
// Destroy the intarray object by dropping the virtual table.  If not done
// explicitly by the application, the virtual table will be dropped implicitly
// by the system when the database connection is closed.
func (c *Conn) CreateIntArray(name string) (IntArray, error) {
	module := &intArray{c: c, name: name}
	if err := c.CreateModule(name, module); err != nil {
		return nil, err
	}
	name = escapeQuote(name)
	if err := c.FastExec(fmt.Sprintf(`CREATE VIRTUAL TABLE temp."%s" USING "%s"`, name, name)); err != nil {
		return nil, err
	}
	return module, nil
}

// Bind a new array of integers to a specific intarray object.
//
// The array of integers bound must be unchanged for the duration of
// any query against the corresponding virtual table.  If the integer
// array does change or is deallocated undefined behavior will result.
func (m *intArray) Bind(elements []int64) {
	m.content = elements
}

// Drop underlying virtual table.
func (m *intArray) Drop() error {
	if m == nil {
		return errors.New("nil sqlite intarray")
	}
	if m.c == nil {
		return nil
	}
	err := m.c.FastExec(fmt.Sprintf(`DROP TABLE temp."%s"`, escapeQuote(m.name)))
	if err != nil {
		return err
	}
	m.c = nil
	return nil
}
