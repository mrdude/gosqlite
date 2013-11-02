// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
	"reflect"
	"testing"
	"time"
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

	assert.T(t, !s.ReadOnly(), "update statement should not be readonly")

	paramCount := s.BindParameterCount()
	assert.Equal(t, 3, paramCount, "bind parameter count")
	firstParamName, berr := s.BindParameterName(1)
	checkNoError(t, berr, "error binding: %s")
	assert.Equal(t, ":f", firstParamName, "bind parameter name")
	lastParamIndex, berr := s.BindParameterIndex(":s")
	checkNoError(t, berr, "error binding: %s")
	assert.Equal(t, 3, lastParamIndex, "bind parameter index")
	columnCount := s.ColumnCount()
	assert.Equal(t, 0, columnCount, "column count")

	db.Begin()
	for i := 0; i < 1000; i++ {
		c, ierr := s.ExecDml(float64(i)*float64(3.14), i, "hello")
		checkNoError(t, ierr, "insert error: %s")
		assert.Equal(t, 1, c, "changes")
		assert.T(t, !s.Busy(), "statement not busy")
	}

	checkNoError(t, db.Commit(), "Error: %s")

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer checkFinalize(cs, t)
	assert.T(t, cs.ReadOnly(), "SELECT statement should be readonly")
	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	assert.Equal(t, 1000, i, "count")

	rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test WHERE a_string LIKE ? ORDER BY int_num LIMIT 2", "hel%")
	defer checkFinalize(rs, t)
	columnCount = rs.ColumnCount()
	assert.Equal(t, 3, columnCount, "column count")
	secondColumnName := rs.ColumnName(1)
	assert.Equal(t, "int_num", secondColumnName, "column name")

	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.Scan(&fnum, &inum, &sstr)
		assert.Equal(t, float64(0), fnum)
		assert.Equal(t, int64(0), inum)
		assert.Equal(t, "hello", sstr)
	}
	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.NamedScan("a_string", &sstr, "float_num", &fnum, "int_num", &inum)
		assert.Equal(t, float64(3.14), fnum)
		assert.Equal(t, int64(1), inum)
		assert.Equal(t, "hello", sstr)
	}
	assert.T(t, 999 == rs.Status(StmtStatusFullScanStep, false), "expected full scan")
	assert.T(t, 1 == rs.Status(StmtStatusSort, false), "expected one sort")
	assert.T(t, 0 == rs.Status(StmtStatusAutoIndex, false), "expected no auto index")
}

func TestScanColumn(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 1, null, 0")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByIndex(0, &i1))
	assert.T(t, !null, "expected not null value")
	assert.Equal(t, 1, i1)
	null = Must(s.ScanByIndex(1, &i2))
	assert.T(t, null, "expected null value")
	assert.Equal(t, 0, i2)
	null = Must(s.ScanByIndex(2, &i3))
	assert.T(t, !null, "expected not null value")
	assert.Equal(t, 0, i3)
}

func TestNamedScanColumn(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 1 AS i1, null AS i2, 0 AS i3")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByName("i1", &i1))
	assert.T(t, !null, "expected not null value")
	assert.Equal(t, 1, i1)
	null = Must(s.ScanByName("i2", &i2))
	assert.T(t, null, "expected null value")
	assert.Equal(t, 0, i2)
	null = Must(s.ScanByName("i3", &i3))
	assert.T(t, !null, "expected not null value")
	assert.Equal(t, 0, i3)

	_, err = s.ScanByName("invalid", &i1)
	assert.T(t, err != nil, "expected invalid name")
}

func TestScanCheck(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 'hello'")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i int
	_, err = s.ScanByIndex(0, &i)
	if serr, ok := err.(*StmtError); ok {
		assert.Equal(t, "", serr.Filename())
		assert.Equal(t, ErrSpecific, serr.Code())
		assert.Equal(t, s.SQL(), serr.SQL())
	} else {
		t.Errorf("Expected StmtError but got %s", reflect.TypeOf(err))
	}
}

func TestScanNull(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT null")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var pi = new(int)
	null := Must(s.ScanByIndex(0, &pi))
	assert.T(t, null, "expected null value")
	assert.Equal(t, (*int)(nil), pi, "expected nil")
	var ps = new(string)
	null = Must(s.ScanByIndex(0, &ps))
	assert.T(t, null, "expected null value")
	assert.Equal(t, (*string)(nil), ps, "expected nil")
}

func TestScanNotNull(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 1")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var pi = new(int)
	null := Must(s.ScanByIndex(0, &pi))
	assert.T(t, !null, "expected not null value")
	assert.Equal(t, 1, *pi)
	var ps = new(string)
	null = Must(s.ScanByIndex(0, &ps))
	assert.T(t, !null, "expected not null value")
	assert.Equal(t, "1", *ps)
}

/*
func TestScanError(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 1")
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
	assert.T(t, s == nil && err != nil, "error expected")
	//println(err.Error())
	err = s.Finalize()
	assert.T(t, err != nil, "error expected")
}

func TestStmtWithClosedDb(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	db.SetCacheSize(0)

	s, err := db.Prepare("SELECT 1")
	checkNoError(t, err, "prepare error: %s")
	assert.Equal(t, db, s.Conn(), "conn")
	defer s.Finalize()

	err = db.Close()
	checkNoError(t, err, "close error: %s")

	err = s.Finalize()
	assert.T(t, err != nil, "error expected")
	//println(err.Error())
}

func TestStmtExecWithSelect(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 1")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()

	err = s.Exec()
	assert.T(t, err != nil, "error expected")
	if serr, ok := err.(*StmtError); ok {
		assert.Equal(t, Row, serr.Code())
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
	checkNoError(t, err, "SELECT error: %s")
	assert.T(t, !exists, "no row expected")
}

func TestNamedBind(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (data BLOB, byte INT)")
	checkNoError(t, err, "exec error: %s")

	is, err := db.Prepare("INSERT INTO test (data, byte) VALUES (:blob, :b)")
	checkNoError(t, err, "prepare error: %s")
	bc := is.BindParameterCount()
	assert.Equal(t, 2, bc, "parameter count")
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

	err = is.NamedBind(":b", byt, ":invalid", nil)
	assert.T(t, err != nil, "invalid param name expected")
	err = is.NamedBind(":b")
	assert.T(t, err != nil, "missing params")
	err = is.NamedBind(byt, ":b")
	assert.T(t, err != nil, "invalid param name")
	checkFinalize(is, t)

	s, err := db.Prepare("SELECT data AS bs, byte AS b FROM test")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var bs []byte
	var b byte
	err = s.NamedScan("b", &b, "bs", &bs)
	checkNoError(t, err, "named scan error: %s")
	assert.Equal(t, len(blob), len(bs), "blob length")
	assert.Equal(t, byt, b, "byte")

	err = s.NamedScan("b")
	assert.T(t, err != nil, "missing params")
	err = s.NamedScan(&b, "b")
	assert.T(t, err != nil, "invalid param name")
}

func TestBind(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (data TEXT, bool INT)")
	checkNoError(t, err, "exec error: %s")

	is, err := db.Prepare("INSERT INTO test (data, bool) VALUES (?, ?)")
	defer checkFinalize(is, t)
	checkNoError(t, err, "prepare error: %s")
	err = is.Bind(nil, true)
	checkNoError(t, err, "bind error: %s")
	_, err = is.Next()
	checkNoError(t, err, "step error: %s")

	err = is.Bind(nil, db)
	assert.T(t, err != nil, "unsupported type error expected")
}

func TestInsertMisuse(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (data TEXT, bool INT)")
	checkNoError(t, err, "exec error: %s")

	is, err := db.Prepare("INSERT INTO test (data, bool) VALUES (?, ?)")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(is, t)

	_, err = is.Insert()
	assert.T(t, err != nil, "missing bind parameters expected")
}

func TestScanValues(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 1, null, 0")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	values := make([]interface{}, 3)
	s.ScanValues(values)
	assert.Equal(t, int64(1), values[0])
	assert.Equal(t, nil, values[1])
	assert.Equal(t, int64(0), values[2])
}

func TestScanBytes(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	s, err := db.Prepare("SELECT 'test'")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	blob, _ := s.ScanBlob(0)
	assert.Equal(t, "test", string(blob))
}

func TestBindEmptyZero(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	var zero time.Time
	s, err := db.Prepare("SELECT ?, ?", "", zero)
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}

	var ps *string
	var zt time.Time
	err = s.Scan(&ps, &zt)
	checkNoError(t, err, "scan error: %s")
	assert.T(t, ps == nil && zt.IsZero(), "null pointers expected")
	_, null := s.ScanValue(0, false)
	assert.T(t, null, "null string expected")
	_, null = s.ScanValue(1, false)
	assert.T(t, null, "null time expected")
}

func TestBindEmptyZeroNotTransformedToNull(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	NullIfEmptyString = false
	NullIfZeroTime = false
	defer func() {
		NullIfEmptyString = true
		NullIfZeroTime = true
	}()

	var zero time.Time
	s, err := db.Prepare("SELECT ?, ?", "", zero)
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)
	if !Must(s.Next()) {
		t.Fatal("no result")
	}

	var st string
	var zt time.Time
	err = s.Scan(&st, &zt)
	checkNoError(t, err, "scan error: %s")
	assert.T(t, len(st) == 0 && zt.IsZero(), "null pointers expected")
	_, null := s.ScanValue(0, false)
	assert.T(t, !null, "empty string expected")
	_, null = s.ScanValue(1, false)
	assert.T(t, !null, "zero time expected")
}

func TestColumnType(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	createTable(db, t)
	s, err := db.Prepare("SELECT * from test")
	checkNoError(t, err, "prepare error: %s")
	defer checkFinalize(s, t)

	for col := 0; col < s.ColumnCount(); col++ {
		//println(col, s.ColumnName(col), s.ColumnOriginName(col), s.ColumnType(col), s.ColumnDeclaredType(col))
		assert.Equal(t, Null, s.ColumnType(col), "column type")
	}
}
