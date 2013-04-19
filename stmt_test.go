// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"reflect"
	"testing"
)

func checkFinalize(s *Stmt, t *testing.T) {
	checkNoError(t, s.Finalize(), "Error finalizing statement: %s")
}

func TestInsertWithStatement(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (:f, :i, :s)")
	checkNoError(t, serr, "prepare error: %s")
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer checkFinalize(s, t)

	assert(t, "update statement should not be readonly", !s.ReadOnly())

	paramCount := s.BindParameterCount()
	assertEquals(t, "bind parameter count error: expected %d but got %d", 3, paramCount)
	firstParamName, berr := s.BindParameterName(1)
	checkNoError(t, berr, "error binding: %s")
	assertEquals(t, "bind parameter name error: expected %s but got %s", ":f", firstParamName /*, berr*/)
	lastParamIndex, berr := s.BindParameterIndex(":s")
	checkNoError(t, berr, "error binding: %s")
	assertEquals(t, "bind parameter index error: expected %d but got %d", 3, lastParamIndex /*, berr*/)
	columnCount := s.ColumnCount()
	assertEquals(t, "column count error: expected %d but got %d", 0, columnCount)

	db.Begin()
	for i := 0; i < 1000; i++ {
		c, ierr := s.ExecDml(float64(i)*float64(3.14), i, "hello")
		checkNoError(t, ierr, "insert error: %s")
		assertEquals(t, "insert error: expected %d changes but got %d", 1, c)
		assert(t, "Statement not reset", !s.Busy())
	}

	checkNoError(t, db.Commit(), "Error: %s")

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer checkFinalize(cs, t)
	assert(t, "select statement should be readonly", cs.ReadOnly())
	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	assertEquals(t, "count should be %d, but it is %d", 1000, i)

	rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test where a_string like ? ORDER BY int_num LIMIT 2", "hel%")
	defer checkFinalize(rs, t)
	columnCount = rs.ColumnCount()
	assertEquals(t, "column count error: expected %d but got %d", 3, columnCount)
	secondColumnName := rs.ColumnName(1)
	assertEquals(t, "column name error: expected %s but got %s", "int_num", secondColumnName)

	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.Scan(&fnum, &inum, &sstr)
		assertEquals(t, "expected %f but got %f", float64(0), fnum)
		assertEquals(t, "expected %d but got %d", int64(0), inum)
		assertEquals(t, "expected %q but got %q", "hello", sstr)
	}
	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.NamedScan("a_string", &sstr, "float_num", &fnum, "int_num", &inum)
		assertEquals(t, "expected %f but got %f", float64(3.14), fnum)
		assertEquals(t, "expected %d but got %d", int64(1), inum)
		assertEquals(t, "expected %q but got %q", "hello", sstr)
	}
	assert(t, "expected full scan", 999 == rs.Status(StmtStatusFullScanStep, false))
	assert(t, "expected one sort", 1 == rs.Status(StmtStatusSort, false))
	assert(t, "expected no auto index", 0 == rs.Status(StmtStatusAutoIndex, false))
}

func TestScanColumn(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select 1, null, 0")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByIndex(0, &i1))
	assert(t, "expected not null value", !null)
	assertEquals(t, "expected %d but got %d", 1, i1)
	null = Must(s.ScanByIndex(1, &i2))
	assert(t, "expected null value", null)
	assertEquals(t, "expected %d but got %d", 0, i2)
	null = Must(s.ScanByIndex(2, &i3))
	assert(t, "expected not null value", !null)
	assertEquals(t, "expected %d but got %d", 0, i3)
}

func TestNamedScanColumn(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select 1 as i1, null as i2, 0 as i3")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByName("i1", &i1))
	assert(t, "expected not null value", !null)
	assertEquals(t, "expected %d but got %d", 1, i1)
	null = Must(s.ScanByName("i2", &i2))
	assert(t, "expected null value", null)
	assertEquals(t, "expected %d but got %d", 0, i2)
	null = Must(s.ScanByName("i3", &i3))
	assert(t, "expected not null value", !null)
	assertEquals(t, "expected %d but got %d", 0, i3)
}

func TestScanCheck(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select 'hello'")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i int
	_, err = s.ScanByIndex(0, &i)
	if serr, ok := err.(*StmtError); ok {
		assertEquals(t, "expected %q but got %q", "", serr.Filename())
		assertEquals(t, "expected %q but got %q", ErrSpecific, serr.Code())
		assertEquals(t, "expected %q but got %q", s.SQL(), serr.SQL())
	} else {
		t.Errorf("Expected StmtError but got %s", reflect.TypeOf(err))
	}
}

func TestScanNull(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select null")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var pi *int = new(int)
	null := Must(s.ScanByIndex(0, &pi))
	assert(t, "expected null value", null)
	assertEquals(t, "expected nil (%p) but got %p", (*int)(nil), pi)
	var ps *string = new(string)
	null = Must(s.ScanByIndex(0, &ps))
	assert(t, "expected null value", null)
	assertEquals(t, "expected nil (%p) but got %p", (*string)(nil), ps)
}

func TestScanNotNull(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select 1")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var pi *int = new(int)
	null := Must(s.ScanByIndex(0, &pi))
	assert(t, "expected not null value", !null)
	assertEquals(t, "expected %d but got %d", 1, *pi)
	var ps *string = new(string)
	null = Must(s.ScanByIndex(0, &ps))
	assert(t, "expected not null value", !null)
	assertEquals(t, "expected %s but got %s", "1", *ps)
}

/*
func TestScanError(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select 1")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var pi *int
	null, err := s.ScanByIndex(0, &pi)
	t.Errorf("(%t,%s)", null, err)
}*/

func TestCloseTwice(t *testing.T) {
	db := open(t)
	s, err := db.Prepare("SELECT 1")
	checkNoError(t, err, "prepare error: %s")
	err = s.Finalize()
	checkNoError(t, err, "finalize error: %s")
	err = s.Finalize()
	checkNoError(t, err, "finalize error: %s")
	err = db.Close()
	checkNoError(t, err, "close error: %s")
	err = db.Close()
	checkNoError(t, err, "close error: %s")
}

func TestStmtMisuse(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("MISUSE")
	assert(t, "error expected", s == nil && err != nil)
	err = s.Finalize()
	assert(t, "error expected", err != nil)
}

func TestStmtWithClosedDb(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	db.SetCacheSize(0)

	s, err := db.Prepare("select 1")
	checkNoError(t, err, "prepare error: %s")
	assertEquals(t, "expected Conn: %p, actual: %p", db, s.Conn())
	defer s.Finalize()

	err = db.Close()
	checkNoError(t, err, "close error: %s")

	err = s.Finalize()
	assert(t, "error expected", err != nil)
	//println(err.Error())
}

func TestStmtExecWithSelect(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("select 1")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()

	err = s.Exec()
	assert(t, "error expected", err != nil)
	if serr, ok := err.(*StmtError); ok {
		assertEquals(t, "expected %q but got %q", Row, serr.Code())
	} else {
		t.Errorf("Expected StmtError but got %s", reflect.TypeOf(err))
	}
}

func TestStmtSelectWithInsert(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (data TEXT)")
	checkNoError(t, err, "exec error: %s")

	s, err := db.Prepare("INSERT INTO test VALUES ('...')")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()

	exists, err := s.SelectOneRow()
	checkNoError(t, err, "select error: %s")
	assert(t, "no row expected", !exists)
}

func TestNamedBind(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (data BLOB, byte INT)")
	checkNoError(t, err, "exec error: %s")

	is, err := db.Prepare("INSERT INTO test (data, byte) VALUES (:blob, :b)")
	checkNoError(t, err, "prepare error: %s")
	bc := is.BindParameterCount()
	assertEquals(t, "expected %d parameters but got %d", 2, bc)
	for i := 1; i <= bc; i++ {
		_, err := is.BindParameterName(i)
		checkNoError(t, err, "bind parameter name error: %s")
	}

	blob := []byte{'h', 'e', 'l', 'l', 'o'}
	var byt byte = '!'
	err = is.NamedBind(":b", byt, ":blob", blob)
	checkNoError(t, err, "named bind error: %s")
	_, err = is.Next()
	checkNoError(t, err, "named bind step error: %s")
	checkFinalize(is, t)

	s, err := db.Prepare("SELECT data as bs, byte as b FROM test")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var bs []byte
	var b byte
	err = s.NamedScan("b", &b, "bs", &bs)
	checkNoError(t, err, "named scan error: %s")
	assertEquals(t, "expected blob: %v, actual: %s", len(blob), len(bs))
	assertEquals(t, "expected byte: %c, actual: %c", byt, b)
}
