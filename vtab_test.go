// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

type testModule struct {
	t *testing.T
}

type testVTab struct {
}

type testVTabCursor struct {
}

func (m testModule) Create(c *Conn, args []string) (VTab, error) {
	//println("testVTab.Create")
	assert(m.t, "Six arguments expected", len(args) == 6)
	assertEquals(m.t, "Expected '%s' but got '%s' as module name", "test", args[0])
	assertEquals(m.t, "Expected '%s' but got '%s' as db name", "main", args[1])
	assertEquals(m.t, "Expected '%s' but got '%s' as table name", "vtab", args[2])
	assertEquals(m.t, "Expected '%s' but got '%s' as first arg", "'1'", args[3])
	assertEquals(m.t, "Expected '%s' but got '%s' as first arg", "2", args[4])
	assertEquals(m.t, "Expected '%s' but got '%s' as first arg", "three", args[5])
	c.DeclareVTab("CREATE TABLE x(test TEXT)")
	return testVTab{}, nil
}
func (m testModule) Connect(c *Conn, args []string) (VTab, error) {
	println("testVTab.Connect")
	return m.Create(c, args)
}

func (m testModule) Destroy() {
	//println("testModule.Destroy")
}

func (v testVTab) BestIndex() error {
	println("testVTab.BestIndex")
	return nil
}
func (v testVTab) Disconnect() error {
	//println("testVTab.Disconnect")
	return nil
}
func (v testVTab) Destroy() error {
	//println("testVTab.Destroy")
	return nil
}
func (v testVTab) Open() (VTabCursor, error) {
	//println("testVTab.Open")
	return testVTabCursor{}, nil
}

func (v testVTabCursor) Close() error {
	//println("testVTabCursor.Close")
	return nil
}
func (v testVTabCursor) Filter( /*idxNum int, idxStr string, int argc, sqlite3_value **argv*/) error {
	println("testVTabCursor.Filter")
	return nil
}
func (v testVTabCursor) Next() error {
	println("testVTabCursor.Next")
	return nil
}
func (v testVTabCursor) Eof() bool {
	println("testVTabCursor.Eof")
	return true
}
func (v testVTabCursor) Column(c *Context, col int) error {
	println("testVTabCursor.Column")
	return nil
}
func (v testVTabCursor) Rowid() (int64, error) {
	println("testVTabCursor.Rowid")
	return 1, nil
}

func TestCreateModule(t *testing.T) {
	db := open(t)
	defer db.Close()
	err := db.CreateModule("test", testModule{t})
	checkNoError(t, err, "couldn't create module: %s")
	err = db.Exec("CREATE VIRTUAL TABLE vtab USING test('1', 2, three)")
	checkNoError(t, err, "couldn't create virtual table: %s")
	//var value *string
	//err = db.OneValue("SELECT * from vtab", &value)
	//checkNoError(t, err, "couldn't select from virtual table: %s")
	//assert(t, "Not null value expected", value != nil)
	//assertEquals(t, "Expected '%s' but got '%s'", "test", *value)
	err = db.Exec("DROP TABLE vtab")
	checkNoError(t, err, "couldn't drop virtual table: %s")
}
