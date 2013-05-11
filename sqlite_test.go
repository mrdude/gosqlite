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
	db, err := Open(":memory:", OpenReadWrite, OpenCreate, OpenFullMutex /*OpenNoMutex*/)
	checkNoError(t, err, "couldn't open database file: %s")
	if db == nil {
		t.Fatal("opened database is nil")
	}
	//db.SetLockingMode("", "exclusive")
	//db.SetSynchronous("", 0)
	//db.Profile(profile, t)
	//db.Trace(trace, t)
	if false /*testing.Verbose()*/ { // Go 1.1
		db.SetAuthorizer(authorizer, t)
	}
	return db
}

func checkClose(db *Conn, t *testing.T) {
	checkNoError(t, db.Close(), "Error closing database: %s")
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

func TestOpenFailure(t *testing.T) {
	db, err := Open("doesnotexist.sqlite", OpenReadOnly)
	assert(t, "open failure expected", db == nil && err != nil)
}

func TestEnableFKey(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	b := Must(db.IsFKeyEnabled())
	if !b {
		b = Must(db.EnableFKey(true))
		assert(t, "cannot enabled FK", b)
	}
}

func TestEnableTriggers(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	b := Must(db.AreTriggersEnabled())
	if !b {
		b = Must(db.EnableTriggers(true))
		assert(t, "cannot enabled triggers", b)
	}
}

func TestEnableExtendedResultCodes(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	checkNoError(t, db.EnableExtendedResultCodes(true), "cannot enabled extended result codes: %s")
}

func TestCreateTable(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
}

func TestManualTransaction(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	checkNoError(t, db.Begin(), "Error while beginning transaction: %s")
	if err := db.Begin(); err == nil {
		t.Fatalf("Error expected (transaction cannot be nested)")
	}
	checkNoError(t, db.Commit(), "Error while commiting transaction: %s")
	checkNoError(t, db.BeginTransaction(Immediate), "Error while beginning immediate transaction: %s")
	checkNoError(t, db.Commit(), "Error while commiting transaction: %s")
	checkNoError(t, db.BeginTransaction(Exclusive), "Error while beginning immediate transaction: %s")
	checkNoError(t, db.Commit(), "Error while commiting transaction: %s")
}

func TestSavepoint(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	checkNoError(t, db.Savepoint("1"), "Error while creating savepoint: %s")
	checkNoError(t, db.Savepoint("2"), "Error while creating savepoint: %s")
	checkNoError(t, db.RollbackSavepoint("2"), "Error while creating savepoint: %s")
	checkNoError(t, db.ReleaseSavepoint("1"), "Error while creating savepoint: %s")
}

func TestExists(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	b := Must(db.Exists("SELECT 1 WHERE 1 = 0"))
	assert(t, "No row expected", !b)
	b = Must(db.Exists("SELECT 1 WHERE 1 = 1"))
	assert(t, "One row expected", b)
}

func TestInsert(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
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
	defer checkFinalize(cs, t)

	paramCount := cs.BindParameterCount()
	assertEquals(t, "bind parameter count error: expected %d but got %d", 0, paramCount)
	columnCount := cs.ColumnCount()
	assertEquals(t, "column count error: expected %d but got %d", 1, columnCount)

	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	assertEquals(t, "column & data count not equal: %d versus %d", columnCount, cs.DataCount())
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	assertEquals(t, "count should be %d, but it is %d", 1000, i)
	if Must(cs.Next()) {
		t.Fatal("Only one row expected")
	}
	assert(t, "Statement not reset", !cs.Busy())
}

/*
func TestLoadExtension(t *testing.T) {
	db := open(t)

	db.EnableLoadExtension(true)

	err := db.LoadExtension("/tmp/myext.so")
	checkNoError(t, err, "load extension error: %s")
}
*/

func TestOpenSameMemoryDb(t *testing.T) {
	db1, err := Open("file:dummy.db?mode=memory&cache=shared", OpenUri, OpenReadWrite, OpenCreate, OpenFullMutex)
	checkNoError(t, err, "open error: %s")
	defer checkClose(db1, t)
	err = db1.Exec("CREATE TABLE test (data TEXT)")
	checkNoError(t, err, "exec error: %s")

	db2, err := Open("file:dummy.db?mode=memory&cache=shared", OpenUri, OpenReadWrite, OpenCreate, OpenFullMutex)
	checkNoError(t, err, "open error: %s")
	defer checkClose(db2, t)
	_, err = db2.Exists("SELECT 1 FROM test")
	checkNoError(t, err, "exists error: %s")
}

func TestConnExecWithSelect(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	err := db.Exec("SELECT 1")
	assert(t, "error expected", err != nil)
	if serr, ok := err.(*StmtError); ok {
		assertEquals(t, "expected %q but got %q", Row, serr.Code())
	} else {
		t.Errorf("Expected StmtError but got %s", reflect.TypeOf(err))
	}
}

func TestConnInitialState(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	autoCommit := db.GetAutocommit()
	assert(t, "autocommit expected to be active by default", autoCommit)
	totalChanges := db.TotalChanges()
	assertEquals(t, "expected total changes: %d, actual: %d", 0, totalChanges)
	err := db.LastError()
	assertEquals(t, "expected last error: %v, actual: %v", nil, err)
	readonly, err := db.Readonly("main")
	checkNoError(t, err, "Readonly status error: %s")
	assert(t, "readonly expected to be unset by default", !readonly)
}

func TestReadonlyMisuse(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	_, err := db.Readonly("doesnotexist")
	assert(t, "error expected", err != nil)
	err.Error()
}

func TestConnSettings(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.EnableLoadExtension(false)
	checkNoError(t, err, "EnableLoadExtension error: %s")
	err = db.SetRecursiveTriggers("main", true)
	checkNoError(t, err, "SetRecursiveTriggers error: %s")
}

func TestComplete(t *testing.T) {
	assert(t, "expected complete statement", Complete("SELECT 1;"))
}

func TestExecMisuse(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
	err := db.Exec("INSERT INTO test VALUES (?, ?, ?, ?); INSERT INTO test VALUES (?, ?, ?, ?)", 0, 273.1, 1, "test")
	assert(t, "exec misuse expected", err != nil)
}

func TestTransaction(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	createTable(db, t)
	gerr, serr := db.Transaction(Immediate, func(_ *Conn) error {
		err, nerr := db.Transaction(Immediate, func(__ *Conn) error {
			return db.Exec("INSERT INTO test VALUES (?, ?, ?, ?)", 0, 273.1, 1, "test")
		})
		checkNoError(t, err, "Applicative error: %s")
		checkNoError(t, nerr, "SQLite error: %s")
		return err
	})
	checkNoError(t, gerr, "Applicative error: %s")
	checkNoError(t, serr, "SQLite error: %s")
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
