// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"reflect"
	"strings"
	"testing"
)

func checkNoError(t *testing.T, err error, format string) {
	if err != nil {
		t.Fatalf(format, err)
	}
}

func open(t *testing.T) *Conn {
	db, err := Open(":memory:", OpenReadWrite, OpenCreate, OpenFullMutex)
	checkNoError(t, err, "couldn't open database file: %s")
	if db == nil {
		t.Fatal("opened database is nil")
	}
	//db.Profile(profile, t)
	//db.Trace(trace, t)
	//db.SetAuthorizer(authorizer, t)
	return db
}

func createTable(db *Conn, t *testing.T) {
	err := db.Exec("DROP TABLE IF EXISTS test;" +
		"CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL," +
		" float_num REAL, int_num INTEGER, a_string TEXT); -- bim")
	checkNoError(t, err, "error creating table: %s")
}

func TestVersion(t *testing.T) {
	v := Version()
	if !strings.HasPrefix(v, "3") {
		t.Fatalf("unexpected library version: %s", v)
	}
}

func TestOpen(t *testing.T) {
	db := open(t)
	checkNoError(t, db.Close(), "Error closing database: %s")
}

func TestEnableFKey(t *testing.T) {
	db := open(t)
	defer db.Close()
	b := Must(db.IsFKeyEnabled())
	if !b {
		b = Must(db.EnableFKey(true))
		assert(t, "cannot enabled FK", b)
	}
}

func TestEnableTriggers(t *testing.T) {
	db := open(t)
	defer db.Close()
	b := Must(db.AreTriggersEnabled())
	if !b {
		b = Must(db.EnableTriggers(true))
		assert(t, "cannot enabled triggers", b)
	}
}

func TestEnableExtendedResultCodes(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.EnableExtendedResultCodes(true), "cannot enabled extended result codes: %s")
}

func TestCreateTable(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
}

func TestTransaction(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.Begin(), "Error while beginning transaction: %s")
	if err := db.Begin(); err == nil {
		t.Fatalf("Error expected (transaction cannot be nested)")
	}
	checkNoError(t, db.Commit(), "Error while commiting transaction: %s")
}

func TestSavepoint(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.Savepoint("1"), "Error while creating savepoint: %s")
	checkNoError(t, db.Savepoint("2"), "Error while creating savepoint: %s")
	checkNoError(t, db.RollbackSavepoint("2"), "Error while creating savepoint: %s")
	checkNoError(t, db.ReleaseSavepoint("1"), "Error while creating savepoint: %s")
}

func TestExists(t *testing.T) {
	db := open(t)
	defer db.Close()
	b := Must(db.Exists("SELECT 1 where 1 = 0"))
	assert(t, "No row expected", !b)
	b = Must(db.Exists("SELECT 1 where 1 = 1"))
	assert(t, "One row expected", b)
}

func TestInsert(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	db.Begin()
	for i := 0; i < 1000; i++ {
		ierr := db.Exec("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)", float64(i)*float64(3.14), i, "hello")
		checkNoError(t, ierr, "insert error: %s")
		c := db.Changes()
		assertEquals(t, "insert error: expected %d changes but got %d", 1, c)
	}
	checkNoError(t, db.Commit(), "Error: %s")

	lastId := db.LastInsertRowid()
	assertEquals(t, "last insert row id error: expected %d but got %d", int64(1000), lastId)

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()

	paramCount := cs.BindParameterCount()
	assertEquals(t, "bind parameter count error: expected %d but got %d", 0, paramCount)
	columnCount := cs.ColumnCount()
	assertEquals(t, "column count error: expected %d but got %d", 1, columnCount)

	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	assertEquals(t, "count should be %d, but it is %d", 1000, i)
	if Must(cs.Next()) {
		t.Fatal("Only one row expected")
	}
	assert(t, "Statement not reset", !cs.Busy())
}

func TestInsertWithStatement(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (:f, :i, :s)")
	checkNoError(t, serr, "prepare error: %s")
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()

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
	defer cs.Finalize()
	assert(t, "select statement should be readonly", cs.ReadOnly())
	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	assertEquals(t, "count should be %d, but it is %d", 1000, i)

	rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test where a_string like ? ORDER BY int_num LIMIT 2", "hel%")
	defer rs.Finalize()
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
	defer db.Close()

	s, err := db.Prepare("select 1, null, 0")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
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
	defer db.Close()

	s, err := db.Prepare("select 1 as i1, null as i2, 0 as i3")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
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
	defer db.Close()

	s, err := db.Prepare("select 'hello'")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
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

/*
func TestLoadExtension(t *testing.T) {
	db := open(t)

	db.EnableLoadExtension(true)

	err := db.LoadExtension("/tmp/myext.so")
	checkNoError(t, err, "load extension error: %s")
}
*/

func TestScanNull(t *testing.T) {
	db := open(t)
	defer db.Close()

	s, err := db.Prepare("select null")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
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
	defer db.Close()

	s, err := db.Prepare("select 1")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
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
	defer db.Close()

	s, err := db.Prepare("MISUSE")
	assert(t, "error expected", s == nil && err != nil)
	err = s.Finalize()
	assert(t, "error expected", err != nil)
}

func TestOpenSameMemoryDb(t *testing.T) {
	db1, err := Open("file:dummy.db?mode=memory&cache=shared", OpenUri, OpenReadWrite, OpenCreate, OpenFullMutex)
	checkNoError(t, err, "open error: %s")
	defer db1.Close()
	err = db1.Exec("CREATE TABLE test (data TEXT)")
	checkNoError(t, err, "exec error: %s")

	db2, err := Open("file:dummy.db?mode=memory&cache=shared", OpenUri, OpenReadWrite, OpenCreate, OpenFullMutex)
	checkNoError(t, err, "open error: %s")
	defer db2.Close()
	_, err = db2.Exists("SELECT 1 from test")
	checkNoError(t, err, "exists error: %s")
}

func assertEquals(t *testing.T, format string, expected, actual interface{}) {
	if expected != actual {
		t.Errorf(format, expected, actual)
	}
}
func assert(t *testing.T, msg string, actual bool) {
	if !actual {
		t.Error(msg)
	}
}
