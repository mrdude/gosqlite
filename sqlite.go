// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.
package sqlite

/*
#cgo LDFLAGS: -lsqlite3

#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>

// These wrappers are necessary because SQLITE_TRANSIENT
// is a pointer constant, and cgo doesn't translate them correctly.
// The definition in sqlite3.h is:
//
// typedef void (*sqlite3_destructor_type)(void*);
// #define SQLITE_STATIC      ((sqlite3_destructor_type)0)
// #define SQLITE_TRANSIENT   ((sqlite3_destructor_type)-1)

static int my_bind_text(sqlite3_stmt *stmt, int n, char *p, int np) {
	return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}
static int my_bind_blob(sqlite3_stmt *stmt, int n, void *p, int np) {
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}

// cgo doesn't support varargs
static int my_db_config(sqlite3 *db, int op, int v, int *ok) {
	return sqlite3_db_config(db, op, v, ok);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"reflect"
	"unsafe"
)

type Errno int

func (e Errno) Error() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

var (
	ErrError      error = Errno(1)   //    /* SQL error or missing database */
	ErrInternal   error = Errno(2)   //    /* Internal logic error in SQLite */
	ErrPerm       error = Errno(3)   //    /* Access permission denied */
	ErrAbort      error = Errno(4)   //    /* Callback routine requested an abort */
	ErrBusy       error = Errno(5)   //    /* The database file is locked */
	ErrLocked     error = Errno(6)   //    /* A table in the database is locked */
	ErrNoMem      error = Errno(7)   //    /* A malloc() failed */
	ErrReadOnly   error = Errno(8)   //    /* Attempt to write a readonly database */
	ErrInterrupt  error = Errno(9)   //    /* Operation terminated by sqlite3_interrupt()*/
	ErrIOErr      error = Errno(10)  //    /* Some kind of disk I/O error occurred */
	ErrCorrupt    error = Errno(11)  //    /* The database disk image is malformed */
	ErrFull       error = Errno(13)  //    /* Insertion failed because database is full */
	ErrCantOpen   error = Errno(14)  //    /* Unable to open the database file */
	ErrEmpty      error = Errno(16)  //    /* Database is empty */
	ErrSchema     error = Errno(17)  //    /* The database schema changed */
	ErrTooBig     error = Errno(18)  //    /* String or BLOB exceeds size limit */
	ErrConstraint error = Errno(19)  //    /* Abort due to constraint violation */
	ErrMismatch   error = Errno(20)  //    /* Data type mismatch */
	ErrMisuse     error = Errno(21)  //    /* Library used incorrectly */
	ErrNolfs      error = Errno(22)  //    /* Uses OS features not supported on host */
	ErrAuth       error = Errno(23)  //    /* Authorization denied */
	ErrFormat     error = Errno(24)  //    /* Auxiliary database format error */
	ErrRange      error = Errno(25)  //    /* 2nd parameter to sqlite3_bind out of range */
	ErrNotDB      error = Errno(26)  //    /* File opened that is not a database file */
	Row                 = Errno(100) //   /* sqlite3_step() has another row ready */
	Done                = Errno(101) //   /* sqlite3_step() has finished executing */
)

var errText = map[Errno]string{
	1:   "SQL error or missing database",
	2:   "Internal logic error in SQLite",
	3:   "Access permission denied",
	4:   "Callback routine requested an abort",
	5:   "The database file is locked",
	6:   "A table in the database is locked",
	7:   "A malloc() failed",
	8:   "Attempt to write a readonly database",
	9:   "Operation terminated by sqlite3_interrupt()",
	10:  "Some kind of disk I/O error occurred",
	11:  "The database disk image is malformed",
	12:  "NOT USED. Table or record not found",
	13:  "Insertion failed because database is full",
	14:  "Unable to open the database file",
	15:  "NOT USED. Database lock protocol error",
	16:  "Database is empty",
	17:  "The database schema changed",
	18:  "String or BLOB exceeds size limit",
	19:  "Abort due to constraint violation",
	20:  "Data type mismatch",
	21:  "Library used incorrectly",
	22:  "Uses OS features not supported on host",
	23:  "Authorization denied",
	24:  "Auxiliary database format error",
	25:  "2nd parameter to sqlite3_bind out of range",
	26:  "File opened that is not a database file",
	100: "sqlite3_step() has another row ready",
	101: "sqlite3_step() has finished executing",
}

func (c *Conn) error(rv C.int) error {
	if c == nil || c.db == nil {
		return errors.New("nil sqlite database")
	}
	if rv == C.SQLITE_OK {
		return nil
	}
	if rv == 21 { // misuse
		return Errno(rv)
	}
	return errors.New(Errno(rv).Error() + ": " + C.GoString(C.sqlite3_errmsg(c.db)))
}

// Calls http://sqlite.org/c3ref/errcode.html
func (c *Conn) Error() error {
	if c == nil || c.db == nil {
		return errors.New("nil sqlite database")
	}
	return c.error(C.sqlite3_errcode(c.db))
}

// Connection
type Conn struct {
	db              *C.sqlite3
	authorizer      *sqliteAuthorizer
	busyHandler     *sqliteBusyHandler
	profile         *sqliteProfile
	progressHandler *sqliteProgressHandler
	trace           *sqliteTrace
	commitHook      *sqliteCommitHook
	rollbackHook    *sqliteRollbackHook
	updateHook      *sqliteUpdateHook
}

// Calls http://sqlite.org/c3ref/libversion.html
func Version() string {
	p := C.sqlite3_libversion()
	return C.GoString(p)
}

// See Open
type OpenFlag int

const (
	OPEN_READONLY     OpenFlag = C.SQLITE_OPEN_READONLY
	OPEN_READWRITE    OpenFlag = C.SQLITE_OPEN_READWRITE
	OPEN_CREATE       OpenFlag = C.SQLITE_OPEN_CREATE
	OPEN_URI          OpenFlag = C.SQLITE_OPEN_URI
	OPEN_NOMUTEX      OpenFlag = C.SQLITE_OPEN_NOMUTEX
	OPEN_FULLMUTEX    OpenFlag = C.SQLITE_OPEN_FULLMUTEX
	OPEN_SHAREDCACHE  OpenFlag = C.SQLITE_OPEN_SHAREDCACHE
	OPEN_PRIVATECACHE OpenFlag = C.SQLITE_OPEN_PRIVATECACHE
)

// ":memory:" for memory db
// "" for temp file db
// 
// Example:
//	db, err := sqlite.Open(":memory:")
//	if err != nil {
//		...
//	}
//	defer db.Close()
//
// Calls sqlite3_open_v2: http://sqlite.org/c3ref/open.html
func Open(filename string, flags ...OpenFlag) (*Conn, error) {
	if C.sqlite3_threadsafe() == 0 {
		return nil, errors.New("sqlite library was not compiled for thread-safe operation")
	}
	var openFlags int
	if len(flags) > 0 {
		for _, flag := range flags {
			openFlags |= int(flag)
		}
	} else {
		openFlags = C.SQLITE_OPEN_FULLMUTEX | C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE
	}

	var db *C.sqlite3
	name := C.CString(filename)
	defer C.free(unsafe.Pointer(name))
	rv := C.sqlite3_open_v2(name, &db, C.int(openFlags), nil)
	if rv != C.SQLITE_OK {
		if db != nil {
			C.sqlite3_close(db)
		}
		return nil, Errno(rv)
	}
	if db == nil {
		return nil, errors.New("sqlite succeeded without returning a database")
	}
	return &Conn{db: db}, nil
}

// Calls http://sqlite.org/c3ref/busy_timeout.html
func (c *Conn) BusyTimeout(ms int) error {
	rv := C.sqlite3_busy_timeout(c.db, C.int(ms))
	if rv == C.SQLITE_OK {
		return nil
	}
	return Errno(rv)
}

// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, b)
// Another way is PRAGMA foreign_keys = boolean;
//
// http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html
func (c *Conn) EnableFKey(b bool) (bool, error) {
	return c.queryOrSetEnableFKey(btocint(b))
}
// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, -1)
// Another way is PRAGMA foreign_keys; 
// 
// http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html
func (c *Conn) IsFKeyEnabled() (bool, error) {
	return c.queryOrSetEnableFKey(-1)
}
func (c *Conn) queryOrSetEnableFKey(i C.int) (bool, error) {
	var ok C.int
	rv := C.my_db_config(c.db, C.SQLITE_DBCONFIG_ENABLE_FKEY, i, &ok)
	if rv == C.SQLITE_OK {
		return (ok == 1), nil
	}
	return false, Errno(rv)
}

// Don't use it with SELECT or anything that returns data.
//
// Example:
//	err := db.Exec("CREATE TABLE test(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
//
// Calls sqlite3_prepare_v2, sqlite3_bind_*, sqlite3_step and sqlite3_finalize
// http://sqlite.org/c3ref/prepare.html, http://sqlite.org/c3ref/bind_blob.html,
// http://sqlite.org/c3ref/step.html and http://sqlite.org/c3ref/finalize.html
func (c *Conn) Exec(cmd string, args ...interface{}) error {
	for len(cmd) > 0 {
		s, err := c.Prepare(cmd)
		if err != nil {
			return err
		} else if s.stmt == nil {
			// this happens for a comment or white-space
			cmd = s.tail
			if err = s.Finalize(); err != nil {
				return err
			}
			continue
		}
		err = s.Exec(args...)
		if err != nil {
			s.Finalize()
			return err
		}
		if len(s.tail) > 0 {
			if len(args) > 0 {
				s.Finalize()
				return errors.New("Cannot execute multiple statements when args are specified")
			}
		}
		cmd = s.tail
		if err = s.Finalize(); err != nil {
			return err
		}
	}
	return nil
}

// Returns true if the specified query returns at least one row.
func (c *Conn) Exists(query string, args ...interface{}) (bool, error) {
	s, err := c.Prepare(query, args...)
	if err != nil {
		return false, err
	}
	defer s.Finalize()
	return s.Next()
}

// Calls http://sqlite.org/c3ref/changes.html
func (c *Conn) Changes() int {
	return int(C.sqlite3_changes(c.db))
}
// Calls http://sqlite.org/c3ref/total_changes.html
func (c *Conn) TotalChanges() int {
	return int(C.sqlite3_total_changes(c.db))
}

// Calls http://sqlite.org/c3ref/last_insert_rowid.html
func (c *Conn) LastInsertRowid() int64 {
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Calls http://sqlite.org/c3ref/interrupt.html
func (c *Conn) Interrupt() {
	C.sqlite3_interrupt(c.db)
}

// Calls http://sqlite.org/c3ref/get_autocommit.html
func (c *Conn) GetAutocommit() bool {
	return C.sqlite3_get_autocommit(c.db) != 0
}

// See Conn.BeginTransaction
type TransactionType int

const (
	DEFERRED  TransactionType = 0
	IMMEDIATE TransactionType = 1
	EXCLUSIVE TransactionType = 2
)

func (c *Conn) Begin() error {
	return c.BeginTransaction(DEFERRED)
}

func (c *Conn) BeginTransaction(t TransactionType) error {
	if t == DEFERRED {
		return c.exec("BEGIN")
	} else if t == IMMEDIATE {
		return c.exec("BEGIN IMMEDIATE")
	} else if t == EXCLUSIVE {
		return c.exec("BEGIN EXCLUSIVE")
	}
	panic(fmt.Sprintf("Unsupported transaction type: '%#v'", t))
	return nil
}

func (c *Conn) Commit() error {
	// TODO Check autocommit?
	return c.exec("COMMIT")
}
func (c *Conn) Rollback() error {
	// TODO Check autocommit?
	return c.exec("ROLLBACK")
}

func (c *Conn) exec(cmd string) error {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))
	rv := C.sqlite3_exec(c.db, cmdstr, nil, nil, nil)
	if rv != C.SQLITE_OK {
		return c.error(rv)
	}
	return nil
}

// Prepared Statement (sqlite3_stmt)
type Stmt struct {
	c    *Conn
	stmt *C.sqlite3_stmt
	tail string
	cols map[string]int // cached columns index by name
	// Enable NULL value check in Scan methods
	CheckNull bool
	// Enable type check in Scan methods
	CheckTypeMismatch bool
}

// Example:
//	stmt, err := db.Prepare("SELECT 1 where 1 = ?", 1)
//	if err != nil {
//		...
//	}
//	defer stmt.Finalize()
//
// Calls sqlite3_prepare_v2 and sqlite3_bind_*
// http://sqlite.org/c3ref/prepare.html, http://sqlite.org/c3ref/bind_blob.html,
func (c *Conn) Prepare(cmd string, args ...interface{}) (*Stmt, error) {
	if c == nil || c.db == nil {
		return nil, errors.New("nil sqlite database")
	}
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))
	var stmt *C.sqlite3_stmt
	var tail *C.char
	rv := C.sqlite3_prepare_v2(c.db, cmdstr, -1, &stmt, &tail)
	if rv != C.SQLITE_OK {
		return nil, c.error(rv)
	}
	var t string
	if tail != nil && C.strlen(tail) > 0 {
		t = C.GoString(tail)
	}
	s := &Stmt{c: c, stmt: stmt, tail: t, CheckNull: true, CheckTypeMismatch: true}
	if len(args) > 0 {
		err := s.Bind(args...)
		if err != nil {
			return s, err
		}
	}
	return s, nil
}

// Don't use it with SELECT or anything that returns data.
// Calls sqlite3_bind_* and sqlite3_step
// http://sqlite.org/c3ref/bind_blob.html, http://sqlite.org/c3ref/step.html
func (s *Stmt) Exec(args ...interface{}) error {
	err := s.Bind(args...)
	if err != nil {
		return err
	}
	rv := C.sqlite3_step(s.stmt)
	if Errno(rv) != Done {
		return s.c.error(rv)
	}
	return nil
}

// Don't use it with SELECT or anything that returns data.
// Like Exec but returns the number of rows that were changed or inserted or deleted.
func (s *Stmt) ExecUpdate(args ...interface{}) (int, error) {
	err := s.Exec(args...)
	if err != nil {
		return -1, err
	}
	return s.c.Changes(), nil
}

// Calls http://sqlite.org/c3ref/bind_parameter_count.html
func (s *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(s.stmt))
}

// Calls http://sqlite.org/c3ref/bind_parameter_index.html
func (s *Stmt) BindParameterIndex(name string) int {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return int(C.sqlite3_bind_parameter_index(s.stmt, cname))
}

// The first host parameter has an index of 1, not 0.
// Calls http://sqlite.org/c3ref/bind_parameter_name.html
func (s *Stmt) BindParameterName(i int) string {
	return C.GoString(C.sqlite3_bind_parameter_name(s.stmt, C.int(i)))
}

// Calls sqlite3_bind_parameter_count and sqlite3_bind_(blob|double|int|int64|null|text) depending on args type.
// http://sqlite.org/c3ref/bind_blob.html
func (s *Stmt) Bind(args ...interface{}) error {
	err := s.Reset() // TODO sqlite3_clear_bindings?
	if err != nil {
		return err
	}

	n := s.BindParameterCount()
	if n != len(args) { // What happens when the number of arguments is less than the number of parameters?
		return errors.New(fmt.Sprintf("incorrect argument count for Stmt.Bind: have %d want %d", len(args), n))
	}

	for i, v := range args {
		var rv C.int
		index := C.int(i + 1)
		switch v := v.(type) {
		case nil:
			rv = C.sqlite3_bind_null(s.stmt, index)
		case string:
			cstr := C.CString(v)
			rv = C.my_bind_text(s.stmt, index, cstr, C.int(len(v)))
			C.free(unsafe.Pointer(cstr))
		case int:
			rv = C.sqlite3_bind_int(s.stmt, index, C.int(v))
		case int64:
			rv = C.sqlite3_bind_int64(s.stmt, index, C.sqlite3_int64(v))
		case byte:
			rv = C.sqlite3_bind_int(s.stmt, index, C.int(v))
		case bool:
			rv = C.sqlite3_bind_int(s.stmt, index, btocint(v))
		case float32:
			rv = C.sqlite3_bind_double(s.stmt, index, C.double(v))
		case float64:
			rv = C.sqlite3_bind_double(s.stmt, index, C.double(v))
		case []byte:
			var p *byte
			if len(v) > 0 {
				p = &v[0]
			}
			rv = C.my_bind_blob(s.stmt, index, unsafe.Pointer(p), C.int(len(v)))
		case ZeroBlobLength:
			rv = C.sqlite3_bind_zeroblob(s.stmt, index, C.int(v))
		default:
			return errors.New("unsupported type in Bind: " + reflect.TypeOf(v).String())
		}
		if rv != C.SQLITE_OK {
			return s.c.error(rv)
		}
	}
	return nil
}

// With custom error handling:
//	var ok bool
//	var err os.Error
// 	for ok, err = s.Next(); ok; ok, err = s.Next() {
//		err = s.Scan(&fnum, &inum, &sstr)
//	}
//	if err != nil {
//		...
//	}
// With panic on error:
// 	for Must(s.Next()) {
//		err := s.Scan(&fnum, &inum, &sstr)
//	}
//
// Calls sqlite3_step
// http://sqlite.org/c3ref/step.html
func (s *Stmt) Next() (bool, error) {
	rv := C.sqlite3_step(s.stmt)
	err := Errno(rv)
	if err == Row {
		return true, nil
	}
	if err != Done {
		return false, s.c.error(rv)
	}
	return false, nil
}

// Calls http://sqlite.org/c3ref/reset.html
func (s *Stmt) Reset() error {
	rv := C.sqlite3_reset(s.stmt)
	if rv != C.SQLITE_OK {
		return s.c.error(rv)
	}
	return nil
}

// Calls http://sqlite.org/c3ref/clear_bindings.html
func (s *Stmt) ClearBindings() error {
	rv := C.sqlite3_clear_bindings(s.stmt)
	if rv != C.SQLITE_OK {
		return s.c.error(rv)
	}
	return nil
}

// Calls http://sqlite.org/c3ref/column_count.html
func (s *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(s.stmt))
}
// Calls http://sqlite.org/c3ref/data_count.html
func (s *Stmt) DataCount() int {
	return int(C.sqlite3_data_count(s.stmt))
}

// The leftmost column is number 0.
// Calls http://sqlite.org/c3ref/column_name.html
func (s *Stmt) ColumnName(index int) string {
	// If there is no AS clause then the name of the column is unspecified and may change from one release of SQLite to the next.
	return C.GoString(C.sqlite3_column_name(s.stmt, C.int(index)))
}

func (s *Stmt) ColumnNames() []string {
	count := s.ColumnCount()
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = s.ColumnName(i)
	}
	return names
}

// See Stmt.ColumnType
type Type int

func (t Type) String() string {
	return typeText[t]
}

var (
	Integer Type = Type(C.SQLITE_INTEGER)
	Float   Type = Type(C.SQLITE_FLOAT)
	Blob    Type = Type(C.SQLITE_BLOB)
	Null    Type = Type(C.SQLITE_NULL)
	Text    Type = Type(C.SQLITE3_TEXT)
)

var typeText = map[Type]string{
	Integer: "Integer",
	Float:   "Float",
	Blob:    "Blob",
	Null:    "Null",
	Text:    "Text",
}

// The leftmost column is number 0.
// After a type conversion, the value returned by sqlite3_column_type() is undefined.
// Calls sqlite3_column_type
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ColumnType(index int) Type {
	return Type(C.sqlite3_column_type(s.stmt, C.int(index)))
}

// Example:
//	stmt, err := db.Prepare("SELECT 1 as id, 'test' as name")
//	defer stmt.Finalize()
//	var id int
//	var name string
//	for sqlite.Must(stmt.Next()) {
//		stmt.NamedScan("name", &name, "id", &id)
//		// TODO error handling
//		fmt.Println(id, name)
//	}
//
// NULL value is converted to 0 if arg type is *int,*int64,*float,*float64, to "" for *string, to []byte{} for *[]byte and to false for *bool.
// Calls sqlite3_column_count, sqlite3_column_name and sqlite3_column_(blob|double|int|int64|text) depending on args type.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) NamedScan(args ...interface{}) error {
	if len(args)%2 != 0 {
		return errors.New("Expected an even number of arguments")
	}
	for i := 0; i < len(args); i += 2 {
		name, ok := args[i].(string)
		if !ok {
			return errors.New("non-string field name field")
		}
		index, err := s.ColumnIndex(name) // How to look up only once for one statement ?
		if err != nil {
			return err
		}
		ptr := args[i+1]
		_, err = s.ScanColumn(index, ptr)
		if err != nil {
			return err
		}
	}
	return nil
}

// Example:
//	stmt, err := db.Prepare("SELECT 1, 'test'")
//	defer stmt.Finalize()
//	var id int
//	var name string
//	for sqlite.Must(stmt.Next()) {
//		err = stmt.Scan(&id, &name)
//		// TODO error handling
//		fmt.Println(id, name)
//	}
//
// NULL value is converted to 0 if arg type is *int,*int64,*float,*float64, to "" for *string, to []byte{} for *[]byte and to false for *bool.
// TODO How to avoid NULL conversion?
// Calls sqlite3_column_count and sqlite3_column_(blob|double|int|int64|text) depending on args type.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) Scan(args ...interface{}) error {
	n := s.ColumnCount()
	if n != len(args) { // What happens when the number of arguments is less than the number of columns?
		return errors.New(fmt.Sprintf("incorrect argument count for Stmt.Scan: have %d want %d", len(args), n))
	}

	for i, v := range args {
		_, err := s.ScanColumn(i, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// Calls http://sqlite.org/c3ref/sql.html
func (s *Stmt) SQL() string {
	return C.GoString(C.sqlite3_sql(s.stmt))
}

// Must scan all columns (but result is cached).
// Calls sqlite3_column_count, sqlite3_column_name
// http://sqlite.org/c3ref/column_name.html
func (s *Stmt) ColumnIndex(name string) (int, error) {
	if s.cols == nil {
		count := s.ColumnCount()
		s.cols = make(map[string]int, count)
		for i := 0; i < count; i++ {
			s.cols[s.ColumnName(i)] = i
		}
	}
	index, ok := s.cols[name]
	if ok {
		return index, nil
	}
	return 0, errors.New("invalid column name: " + name)
}

// Set nullable to false to skip NULL type test.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_count, sqlite3_column_name and sqlite3_column_(blob|double|int|int64|text) depending on arg type.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) NamedScanColumn(name string, value interface{}) (bool, error) {
	index, err := s.ColumnIndex(name)
	if err != nil {
		return false, err
	}
	return s.ScanColumn(index, value)
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_(blob|double|int|int64|text) depending on arg type.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanColumn(index int, value interface{}) (bool, error) {
	var isNull bool
	var err error
	switch value := value.(type) {
	case nil:
	case *string:
		*value, isNull, err = s.ScanText(index)
	case *int:
		*value, isNull, err = s.ScanInt(index)
	case *int64:
		*value, isNull, err = s.ScanInt64(index)
	case *byte:
		*value, isNull, err = s.ScanByte(index)
	case *bool:
		*value, isNull, err = s.ScanBool(index)
	case *float64:
		*value, isNull, err = s.ScanFloat64(index)
	case *[]byte:
		*value, isNull, err = s.ScanBlob(index)
	case *interface{}:
		*value = s.ScanValue(index)
		isNull = *value == nil
	default:
		return false, errors.New("unsupported type in Scan: " + reflect.TypeOf(value).String())
	}
	return isNull, err
}

// The leftmost column/index is number 0.
// Calls sqlite3_column_(blob|double|int|int64|text) depending on columns type.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanValue(index int) (value interface{}) {
	switch s.ColumnType(index) {
	case Null:
		value = nil
	case Text:
		p := C.sqlite3_column_text(s.stmt, C.int(index))
		n := C.sqlite3_column_bytes(s.stmt, C.int(index))
		value = C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
	case Integer:
		value = int64(C.sqlite3_column_int64(s.stmt, C.int(index)))
	case Float:
		value = float64(C.sqlite3_column_double(s.stmt, C.int(index)))
	case Blob:
		p := C.sqlite3_column_blob(s.stmt, C.int(index))
		n := C.sqlite3_column_bytes(s.stmt, C.int(index))
		value = (*[1 << 30]byte)(unsafe.Pointer(p))[0:n]
	default:
		panic("The column type is not one of SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB, or SQLITE_NULL")
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null.
// Calls sqlite3_column_text.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanText(index int) (value string, isNull bool, err error) {
	p := C.sqlite3_column_text(s.stmt, C.int(index))
	if p == nil {
		isNull = true
	} else {
		n := C.sqlite3_column_bytes(s.stmt, C.int(index))
		value = C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_int.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanInt(index int) (value int, isNull bool, err error) {
	var ctype Type
	if s.CheckNull || s.CheckTypeMismatch {
		ctype = s.ColumnType(index)
	}
	if s.CheckNull && ctype == Null {
		isNull = true
	} else {
		if s.CheckTypeMismatch {
			err = s.checkTypeMismatch(ctype, Integer)
		}
		value = int(C.sqlite3_column_int(s.stmt, C.int(index)))
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_int64.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanInt64(index int) (value int64, isNull bool, err error) {
	var ctype Type
	if s.CheckNull || s.CheckTypeMismatch {
		ctype = s.ColumnType(index)
	}
	if s.CheckNull && ctype == Null {
		isNull = true
	} else {
		if s.CheckTypeMismatch {
			err = s.checkTypeMismatch(ctype, Integer)
		}
		value = int64(C.sqlite3_column_int64(s.stmt, C.int(index)))
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_int.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanByte(index int) (value byte, isNull bool, err error) {
	var ctype Type
	if s.CheckNull || s.CheckTypeMismatch {
		ctype = s.ColumnType(index)
	}
	if s.CheckNull && ctype == Null {
		isNull = true
	} else {
		if s.CheckTypeMismatch {
			err = s.checkTypeMismatch(ctype, Integer)
		}
		value = byte(C.sqlite3_column_int(s.stmt, C.int(index)))
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_int.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanBool(index int) (value bool, isNull bool, err error) {
	var ctype Type
	if s.CheckNull || s.CheckTypeMismatch {
		ctype = s.ColumnType(index)
	}
	if s.CheckNull && ctype == Null {
		isNull = true
	} else {
		if s.CheckTypeMismatch {
			err = s.checkTypeMismatch(ctype, Integer)
		}
		value = C.sqlite3_column_int(s.stmt, C.int(index)) == 1
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_double.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanFloat64(index int) (value float64, isNull bool, err error) {
	var ctype Type
	if s.CheckNull || s.CheckTypeMismatch {
		ctype = s.ColumnType(index)
	}
	if s.CheckNull && ctype == Null {
		isNull = true
	} else {
		if s.CheckTypeMismatch {
			err = s.checkTypeMismatch(ctype, Float)
		}
		value = float64(C.sqlite3_column_double(s.stmt, C.int(index)))
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null.
// Calls sqlite3_column_bytes.
// http://sqlite.org/c3ref/column_blob.html
func (s *Stmt) ScanBlob(index int) (value []byte, isNull bool, err error) {
	p := C.sqlite3_column_blob(s.stmt, C.int(index))
	if p == nil {
		isNull = true
	} else {
		n := C.sqlite3_column_bytes(s.stmt, C.int(index))
		value = (*[1 << 30]byte)(unsafe.Pointer(p))[0:n]
	}
	return
}

// Only lossy conversion is reported as error.
func (s *Stmt) checkTypeMismatch(source, target Type) error {
	switch target {
	case Integer:
		switch source {
		case Float:
			fallthrough
		case Text:
			fallthrough
		case Blob:
			return s.c.error(20)
		}
	case Float:
		switch source {
		case Text:
			fallthrough
		case Blob:
			return s.c.error(20)
		}
	}
	return nil
}

// Calls http://sqlite.org/c3ref/finalize.html
func (s *Stmt) Finalize() error {
	rv := C.sqlite3_finalize(s.stmt)
	if rv != C.SQLITE_OK {
		return s.c.error(rv)
	}
	s.stmt = nil
	return nil
}

// Like http://sqlite.org/c3ref/db_handle.html
func (s *Stmt) Conn() *Conn {
	return s.c
}

// Calls http://sqlite.org/c3ref/close.html
func (c *Conn) Close() error {
	if c == nil {
		return errors.New("nil sqlite database")
	}
	// Dangling statements
	stmt := C.sqlite3_next_stmt(c.db, nil)
	for stmt != nil {
		Log(21, "Dangling statement")
		C.sqlite3_finalize(stmt)
		stmt = C.sqlite3_next_stmt(c.db, stmt)
	}

	rv := C.sqlite3_close(c.db)
	if rv != C.SQLITE_OK {
		return c.error(rv)
	}
	c.db = nil
	return nil
}

// Calls http://sqlite.org/c3ref/stmt_readonly.html
func (s *Stmt) ReadOnly() bool {
	return C.sqlite3_stmt_readonly(s.stmt) == 1
}

// Calls http://sqlite.org/c3ref/enable_load_extension.html
func (c *Conn) EnableLoadExtension(b bool) {
	C.sqlite3_enable_load_extension(c.db, btocint(b))
}
// Calls http://sqlite.org/c3ref/load_extension.html
func (c *Conn) LoadExtension(file string, proc ...string) error {
	cfile := C.CString(file)
	defer C.free(unsafe.Pointer(cfile))
	var cproc *C.char
	if len(proc) > 0 {
		cproc = C.CString(proc[0])
		defer C.free(unsafe.Pointer(cproc))
	}
	var errMsg *C.char
	rv := C.sqlite3_load_extension(c.db, cfile, cproc, &errMsg)
	if rv != C.SQLITE_OK {
		defer C.sqlite3_free(unsafe.Pointer(errMsg))
		return errors.New(Errno(rv).Error() + ": " + C.GoString(errMsg))
	}
	return nil
}

// Calls http://sqlite.org/c3ref/enable_shared_cache.html
func EnableSharedCache(b bool) {
	C.sqlite3_enable_shared_cache(btocint(b))
}

// Must is a helper that wraps a call to a function returning (bool, os.Error)
// and panics if the error is non-nil.
func Must(b bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return b
}

func btocint(b bool) C.int {
	if b {
		return 1
	}
	return 0
}
