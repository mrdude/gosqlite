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

static int my_bind_text(sqlite3_stmt *stmt, int n, const char *p, int np) {
	return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}
static int my_bind_blob(sqlite3_stmt *stmt, int n, void *p, int np) {
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}

// just to get ride of "warning: passing argument 5 of ‘sqlite3_prepare_v2’ from incompatible pointer type [...] ‘const char **’ but argument is of type ‘char **’"
static int my_prepare_v2(sqlite3 *db, const char *zSql, int nByte, sqlite3_stmt **ppStmt, char **pzTail) {
	return sqlite3_prepare_v2(db, zSql, nByte, ppStmt, (const char**)pzTail);
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
	"io"
	"reflect"
	"unsafe"
)

type ConnError struct {
	c       *Conn
	code    Errno
	msg     string
	details string
}

func (e *ConnError) Code() Errno {
	return e.code
}

// FIXME  it might be the case that a second error occurs on a separate thread in between the time of the first error and the call to this method.
func (e *ConnError) ExtendedCode() int {
	return int(C.sqlite3_extended_errcode(e.c.db))
}

// Return Database file name from which the error comes from.
func (e *ConnError) Filename() string {
	return e.c.Filename
}

func (e *ConnError) Error() string {
	if len(e.details) > 0 {
		return fmt.Sprintf("%s: %s (%s)", e.code.Error(), e.msg, e.details)
	} else if len(e.msg) > 0 {
		return fmt.Sprintf("%s: %s", e.code.Error(), e.msg)
	}
	return e.code.Error()
}

type StmtError struct {
	ConnError
	s *Stmt
}

func (e *StmtError) SQL() string {
	return e.s.SQL()
}

// Result codes
type Errno int

func (e Errno) Error() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

var (
	ErrError      error = Errno(C.SQLITE_ERROR)      /* SQL error or missing database */
	ErrInternal   error = Errno(C.SQLITE_INTERNAL)   /* Internal logic error in SQLite */
	ErrPerm       error = Errno(C.SQLITE_PERM)       /* Access permission denied */
	ErrAbort      error = Errno(C.SQLITE_ABORT)      /* Callback routine requested an abort */
	ErrBusy       error = Errno(C.SQLITE_BUSY)       /* The database file is locked */
	ErrLocked     error = Errno(C.SQLITE_LOCKED)     /* A table in the database is locked */
	ErrNoMem      error = Errno(C.SQLITE_NOMEM)      /* A malloc() failed */
	ErrReadOnly   error = Errno(C.SQLITE_READONLY)   /* Attempt to write a readonly database */
	ErrInterrupt  error = Errno(C.SQLITE_INTERRUPT)  /* Operation terminated by sqlite3_interrupt()*/
	ErrIOErr      error = Errno(C.SQLITE_IOERR)      /* Some kind of disk I/O error occurred */
	ErrCorrupt    error = Errno(C.SQLITE_CORRUPT)    /* The database disk image is malformed */
	ErrNotFound   error = Errno(C.SQLITE_NOTFOUND)   /* Unknown opcode in sqlite3_file_control() */
	ErrFull       error = Errno(C.SQLITE_FULL)       /* Insertion failed because database is full */
	ErrCantOpen   error = Errno(C.SQLITE_CANTOPEN)   /* Unable to open the database file */
	ErrProtocol   error = Errno(C.SQLITE_PROTOCOL)   /* Database lock protocol error */
	ErrEmpty      error = Errno(C.SQLITE_EMPTY)      /* Database is empty */
	ErrSchema     error = Errno(C.SQLITE_SCHEMA)     /* The database schema changed */
	ErrTooBig     error = Errno(C.SQLITE_TOOBIG)     /* String or BLOB exceeds size limit */
	ErrConstraint error = Errno(C.SQLITE_CONSTRAINT) /* Abort due to constraint violation */
	ErrMismatch   error = Errno(C.SQLITE_MISMATCH)   /* Data type mismatch */
	ErrMisuse     error = Errno(C.SQLITE_MISUSE)     /* Library used incorrectly */
	ErrNolfs      error = Errno(C.SQLITE_NOLFS)      /* Uses OS features not supported on host */
	ErrAuth       error = Errno(C.SQLITE_AUTH)       /* Authorization denied */
	ErrFormat     error = Errno(C.SQLITE_FORMAT)     /* Auxiliary database format error */
	ErrRange      error = Errno(C.SQLITE_RANGE)      /* 2nd parameter to sqlite3_bind out of range */
	ErrNotDB      error = Errno(C.SQLITE_NOTADB)     /* File opened that is not a database file */
	Row                 = Errno(C.SQLITE_ROW)        /* sqlite3_step() has another row ready */
	Done                = Errno(C.SQLITE_DONE)       /* sqlite3_step() has finished executing */
	ErrSpecific         = Errno(-1)                  /* Gosqlite specific error */
)

var errText = map[Errno]string{
	C.SQLITE_ERROR:      "SQL error or missing database",
	C.SQLITE_INTERNAL:   "Internal logic error in SQLite",
	C.SQLITE_PERM:       "Access permission denied",
	C.SQLITE_ABORT:      "Callback routine requested an abort",
	C.SQLITE_BUSY:       "The database file is locked",
	C.SQLITE_LOCKED:     "A table in the database is locked",
	C.SQLITE_NOMEM:      "A malloc() failed",
	C.SQLITE_READONLY:   "Attempt to write a readonly database",
	C.SQLITE_INTERRUPT:  "Operation terminated by sqlite3_interrupt()",
	C.SQLITE_IOERR:      "Some kind of disk I/O error occurred",
	C.SQLITE_CORRUPT:    "The database disk image is malformed",
	C.SQLITE_NOTFOUND:   "Unknown opcode in sqlite3_file_control()",
	C.SQLITE_FULL:       "Insertion failed because database is full",
	C.SQLITE_CANTOPEN:   "Unable to open the database file",
	C.SQLITE_PROTOCOL:   "Database lock protocol error",
	C.SQLITE_EMPTY:      "Database is empty",
	C.SQLITE_SCHEMA:     "The database schema changed",
	C.SQLITE_TOOBIG:     "String or BLOB exceeds size limit",
	C.SQLITE_CONSTRAINT: "Abort due to constraint violation",
	C.SQLITE_MISMATCH:   "Data type mismatch",
	C.SQLITE_MISUSE:     "Library used incorrectly",
	C.SQLITE_NOLFS:      "Uses OS features not supported on host",
	C.SQLITE_AUTH:       "Authorization denied",
	C.SQLITE_FORMAT:     "Auxiliary database format error",
	C.SQLITE_RANGE:      "2nd parameter to sqlite3_bind out of range",
	C.SQLITE_NOTADB:     "File opened that is not a database file",
	Row:                 "sqlite3_step() has another row ready",
	Done:                "sqlite3_step() has finished executing",
	ErrSpecific:         "Gosqlite specific error",
}

func (c *Conn) error(rv C.int, details ...string) error {
	if c == nil {
		return errors.New("nil sqlite database")
	}
	if rv == C.SQLITE_OK {
		return nil
	}
	err := &ConnError{c: c, code: Errno(rv), msg: C.GoString(C.sqlite3_errmsg(c.db))}
	if len(details) > 0 {
		err.details = details[0]
	}
	return err
}

func (s *Stmt) error(rv C.int, details ...string) error {
	if s == nil {
		return errors.New("nil sqlite statement")
	}
	if rv == C.SQLITE_OK {
		return nil
	}
	err := ConnError{c: s.c, code: Errno(rv), msg: C.GoString(C.sqlite3_errmsg(s.c.db))}
	if len(details) > 0 {
		err.details = details[0]
	}
	return &StmtError{err, s}
}

func (c *Conn) specificError(msg string, a ...interface{}) error {
	return &ConnError{c: c, code: ErrSpecific, msg: fmt.Sprintf(msg, a...)}
}

func (s *Stmt) specificError(msg string, a ...interface{}) error {
	return &StmtError{ConnError{c: s.c, code: ErrSpecific, msg: fmt.Sprintf(msg, a...)}, s}
}

func (c *Conn) LastError() error {
	if c == nil {
		return errors.New("nil sqlite database")
	}
	return &ConnError{c: c, code: Errno(C.sqlite3_errcode(c.db)), msg: C.GoString(C.sqlite3_errmsg(c.db))}
}

// Database connection handle
type Conn struct {
	db              *C.sqlite3
	Filename        string
	authorizer      *sqliteAuthorizer
	busyHandler     *sqliteBusyHandler
	profile         *sqliteProfile
	progressHandler *sqliteProgressHandler
	trace           *sqliteTrace
	commitHook      *sqliteCommitHook
	rollbackHook    *sqliteRollbackHook
	updateHook      *sqliteUpdateHook
	udfs            map[string]*sqliteFunction
}

// Run-time library version number
// (See http://sqlite.org/c3ref/libversion.html)
func Version() string {
	p := C.sqlite3_libversion()
	return C.GoString(p)
}

// Flags for file open operations
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

// Open a new database connection.
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
// (See sqlite3_open_v2: http://sqlite.org/c3ref/open.html)
func Open(filename string, flags ...OpenFlag) (*Conn, error) {
	return OpenVfs(filename, "", flags...)
}

// Open a new database with a specified virtual file system.
func OpenVfs(filename string, vfsname string, flags ...OpenFlag) (*Conn, error) {
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
	var vfs *C.char
	if len(vfsname) > 0 {
		vfs = C.CString(vfsname)
		defer C.free(unsafe.Pointer(vfs))
	}
	rv := C.sqlite3_open_v2(name, &db, C.int(openFlags), vfs)
	if rv != C.SQLITE_OK {
		if db != nil {
			C.sqlite3_close(db)
		}
		return nil, Errno(rv)
	}
	if db == nil {
		return nil, errors.New("sqlite succeeded without returning a database")
	}
	return &Conn{db: db, Filename: filename}, nil
}

// Set a busy timeout
// (See http://sqlite.org/c3ref/busy_timeout.html)
func (c *Conn) BusyTimeout(ms int) error {
	return c.error(C.sqlite3_busy_timeout(c.db, C.int(ms)))
}

// Enable or disable the enforcement of foreign key constraints
// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, b)
// Another way is PRAGMA foreign_keys = boolean;
//
// (See http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html)
func (c *Conn) EnableFKey(b bool) (bool, error) {
	return c.queryOrSetEnableFKey(btocint(b))
}

// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, -1)
// Another way is PRAGMA foreign_keys;
//
// (See http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html)
func (c *Conn) IsFKeyEnabled() (bool, error) {
	return c.queryOrSetEnableFKey(-1)
}
func (c *Conn) queryOrSetEnableFKey(i C.int) (bool, error) {
	var ok C.int
	rv := C.my_db_config(c.db, C.SQLITE_DBCONFIG_ENABLE_FKEY, i, &ok)
	if rv == C.SQLITE_OK {
		return (ok == 1), nil
	}
	return false, c.error(rv)
}

// Enable or disable the extended result codes feature of SQLite.
// (See http://sqlite.org/c3ref/extended_result_codes.html)
func (c *Conn) EnableExtendedResultCodes(b bool) error {
	return c.error(C.sqlite3_extended_result_codes(c.db, btocint(b)))
}

// Prepare and execute one parameterized statement or many statements (separated by semi-colon).
// Don't use it with SELECT or anything that returns data.
//
// Example:
//	err := db.Exec("CREATE TABLE test(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
//
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
				return c.specificError("Cannot execute multiple statements when args are specified")
			}
		}
		cmd = s.tail
		if err = s.Finalize(); err != nil {
			return err
		}
	}
	return nil
}

// Return true if the specified query returns at least one row.
func (c *Conn) Exists(query string, args ...interface{}) (bool, error) {
	s, err := c.Prepare(query, args...)
	if err != nil {
		return false, err
	}
	defer s.Finalize()
	return s.Next()
}

// Use it with SELECT that returns only one row with only one column.
// Returns io.EOF when there is no row.
func (c *Conn) OneValue(query string, args ...interface{}) (interface{}, error) {
	s, err := c.Prepare(query, args...)
	if err != nil {
		return nil, err
	}
	defer s.Finalize()
	b, err := s.Next()
	if err != nil {
		return nil, err
	} else if !b {
		return nil, io.EOF
	}
	return s.ScanValue(0), nil
}

// Count the number of rows modified
// (See http://sqlite.org/c3ref/changes.html)
func (c *Conn) Changes() int {
	return int(C.sqlite3_changes(c.db))
}

// Total number of rows Modified
// (See http://sqlite.org/c3ref/total_changes.html)
func (c *Conn) TotalChanges() int {
	return int(C.sqlite3_total_changes(c.db))
}

// Return the rowid of the most recent successful INSERT into the database.
// (See http://sqlite.org/c3ref/last_insert_rowid.html)
func (c *Conn) LastInsertRowid() int64 {
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Interrupt a long-running query
// (See http://sqlite.org/c3ref/interrupt.html)
func (c *Conn) Interrupt() {
	C.sqlite3_interrupt(c.db)
}

// Test for auto-commit mode
// (See http://sqlite.org/c3ref/get_autocommit.html)
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

// Begin transaction in deferred mode
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

// Commit transaction
func (c *Conn) Commit() error {
	// TODO Check autocommit?
	return c.exec("COMMIT")
}

// Rollback transaction
func (c *Conn) Rollback() error {
	// TODO Check autocommit?
	return c.exec("ROLLBACK")
}

func (c *Conn) exec(cmd string) error {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))
	return c.error(C.sqlite3_exec(c.db, cmdstr, nil, nil, nil))
}

// SQL statement
type Stmt struct {
	c      *Conn
	stmt   *C.sqlite3_stmt
	tail   string
	cols   map[string]int // cached columns index by name
	params map[string]int // cached parameter index by name
	// Enable NULL value check in Scan methods
	CheckNull bool
	// Enable type check in Scan methods
	CheckTypeMismatch bool
}

// Compile an SQL statement and optionally bind values
// Example:
//	stmt, err := db.Prepare("SELECT 1 where 1 = ?", 1)
//	if err != nil {
//		...
//	}
//	defer stmt.Finalize()
//
// (See sqlite3_prepare_v2: http://sqlite.org/c3ref/prepare.html)
func (c *Conn) Prepare(cmd string, args ...interface{}) (*Stmt, error) {
	if c == nil {
		return nil, errors.New("nil sqlite database")
	}
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))
	var stmt *C.sqlite3_stmt
	var tail *C.char
	rv := C.my_prepare_v2(c.db, cmdstr, -1, &stmt, &tail)
	if rv != C.SQLITE_OK {
		return nil, c.error(rv, cmd)
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

// One-step statement execution
// Don't use it with SELECT or anything that returns data.
// (See http://sqlite.org/c3ref/bind_blob.html, http://sqlite.org/c3ref/step.html)
func (s *Stmt) Exec(args ...interface{}) error {
	err := s.Bind(args...)
	if err != nil {
		return err
	}
	rv := C.sqlite3_step(s.stmt)
	C.sqlite3_reset(s.stmt)
	if Errno(rv) != Done {
		return s.error(rv)
	}
	return nil
}

// Like Exec but returns the number of rows that were changed or inserted or deleted.
// Don't use it with SELECT or anything that returns data.
func (s *Stmt) ExecDml(args ...interface{}) (int, error) {
	err := s.Exec(args...)
	if err != nil {
		return -1, err
	}
	return s.c.Changes(), nil
}

// Like ExecDml but returns the autoincremented rowid.
// Don't use it with SELECT or anything that returns data.
func (s *Stmt) Insert(args ...interface{}) (int64, error) {
	n, err := s.ExecDml(args...)
	if err != nil {
		return -1, err
	}
	if n == 0 { // No change => no insert...
		return -1, nil
	}
	return s.c.LastInsertRowid(), nil
}

// The callback function is invoked for each result row coming out of the statement.
//
//  s, err := c.Prepare(...)
//	// TODO error handling
//  defer s.Finalize()
//  err = s.Select(func(s *Stmt) error {
//  	//Scan
//  })
//	// TODO error handling
func (s *Stmt) Select(rowCallbackHandler func(s *Stmt) error) error {
	for {
		if ok, err := s.Next(); err != nil {
			return err
		} else if !ok {
			break
		}
		if err := rowCallbackHandler(s); err != nil {
			return err
		}
	}
	return nil
}

// Number of SQL parameters
// (See http://sqlite.org/c3ref/bind_parameter_count.html)
func (s *Stmt) BindParameterCount() int {
	return int(C.sqlite3_bind_parameter_count(s.stmt))
}

// Index of a parameter with a given name (cached)
// (See http://sqlite.org/c3ref/bind_parameter_index.html)
func (s *Stmt) BindParameterIndex(name string) (int, error) {
	if s.params == nil {
		count := s.BindParameterCount()
		s.params = make(map[string]int, count)
	}
	index, ok := s.params[name]
	if ok {
		return index, nil
	}
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	index = int(C.sqlite3_bind_parameter_index(s.stmt, cname))
	if index == 0 {
		return -1, s.specificError("invalid parameter name: %s", name)
	}
	s.params[name] = index
	return index, nil
}

// Name of a host parameter
// The first host parameter has an index of 1, not 0.
// (See http://sqlite.org/c3ref/bind_parameter_name.html)
func (s *Stmt) BindParameterName(i int) (string, error) {
	name := C.sqlite3_bind_parameter_name(s.stmt, C.int(i))
	if name == nil {
		return "", s.specificError("invalid parameter index: %d", i)
	}
	return C.GoString(name), nil
}

// Bind parameters by their name (name1, value1, ...)
func (s *Stmt) NamedBind(args ...interface{}) error {
	if len(args)%2 != 0 {
		return s.specificError("Expected an even number of arguments")
	}
	for i := 0; i < len(args); i += 2 {
		name, ok := args[i].(string)
		if !ok {
			return s.specificError("non-string param name")
		}
		index, err := s.BindParameterIndex(name) // How to look up only once for one statement ?
		if err != nil {
			return err
		}
		err = s.BindByIndex(index, args[i+1])
		if err != nil {
			return err
		}
	}
	return nil
}

// Bind parameters by their index.
// Calls sqlite3_bind_parameter_count and sqlite3_bind_(blob|double|int|int64|null|text) depending on args type.
// (See http://sqlite.org/c3ref/bind_blob.html)
func (s *Stmt) Bind(args ...interface{}) error {
	n := s.BindParameterCount()
	if n != len(args) {
		return s.specificError("incorrect argument count for Stmt.Bind: have %d want %d", len(args), n)
	}

	for i, v := range args {
		err := s.BindByIndex(i+1, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// Bind value to the specified host parameter of the prepared statement
// The leftmost SQL parameter has an index of 1.
func (s *Stmt) BindByIndex(index int, value interface{}) error {
	i := C.int(index)
	var rv C.int
	switch value := value.(type) {
	case nil:
		rv = C.sqlite3_bind_null(s.stmt, i)
	case string:
		cstr := C.CString(value)
		rv = C.my_bind_text(s.stmt, i, cstr, C.int(len(value)))
		C.free(unsafe.Pointer(cstr))
		//rv = C.my_bind_text(s.stmt, i, *((**C.char)(unsafe.Pointer(&value))), C.int(len(value)))
	case int:
		rv = C.sqlite3_bind_int(s.stmt, i, C.int(value))
	case int64:
		rv = C.sqlite3_bind_int64(s.stmt, i, C.sqlite3_int64(value))
	case byte:
		rv = C.sqlite3_bind_int(s.stmt, i, C.int(value))
	case bool:
		rv = C.sqlite3_bind_int(s.stmt, i, btocint(value))
	case float32:
		rv = C.sqlite3_bind_double(s.stmt, i, C.double(value))
	case float64:
		rv = C.sqlite3_bind_double(s.stmt, i, C.double(value))
	case []byte:
		var p *byte
		if len(value) > 0 {
			p = &value[0]
		}
		rv = C.my_bind_blob(s.stmt, i, unsafe.Pointer(p), C.int(len(value)))
	case ZeroBlobLength:
		rv = C.sqlite3_bind_zeroblob(s.stmt, i, C.int(value))
	default:
		return s.specificError("unsupported type in Bind: %s", reflect.TypeOf(value))
	}
	return s.error(rv)
}

// Evaluate an SQL statement
//
// With custom error handling:
//	for {
//		if ok, err := s.Next(); err != nil {
//			return nil, err
//		} else if !ok {
//			break
//		}
//		err = s.Scan(&fnum, &inum, &sstr)
//	}
// With panic on error:
// 	for Must(s.Next()) {
//		err := s.Scan(&fnum, &inum, &sstr)
//	}
//
// (See http://sqlite.org/c3ref/step.html)
func (s *Stmt) Next() (bool, error) {
	rv := C.sqlite3_step(s.stmt)
	err := Errno(rv)
	if err == Row {
		return true, nil
	}
	C.sqlite3_reset(s.stmt)
	if err != Done {
		return false, s.error(rv)
	}
	return false, nil
}

// Reset a prepared statement
// (See http://sqlite.org/c3ref/reset.html)
func (s *Stmt) Reset() error {
	return s.error(C.sqlite3_reset(s.stmt))
}

// Reset all bindings on a prepared statement
// (See http://sqlite.org/c3ref/clear_bindings.html)
func (s *Stmt) ClearBindings() error {
	return s.error(C.sqlite3_clear_bindings(s.stmt))
}

// Number of columns in a result set
// (See http://sqlite.org/c3ref/column_count.html)
func (s *Stmt) ColumnCount() int {
	return int(C.sqlite3_column_count(s.stmt))
}

// Number of columns in a result set
// (See http://sqlite.org/c3ref/data_count.html)
func (s *Stmt) DataCount() int {
	return int(C.sqlite3_data_count(s.stmt))
}

// Column name in a result set
// The leftmost column is number 0.
// (See http://sqlite.org/c3ref/column_name.html)
func (s *Stmt) ColumnName(index int) string {
	// If there is no AS clause then the name of the column is unspecified and may change from one release of SQLite to the next.
	return C.GoString(C.sqlite3_column_name(s.stmt, C.int(index)))
}

// Column names in a result set
func (s *Stmt) ColumnNames() []string {
	count := s.ColumnCount()
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = s.ColumnName(i)
	}
	return names
}

// SQLite fundamental datatypes
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

// Return the datatype code for the initial data type of the result column.
// The leftmost column is number 0.
// After a type conversion, the value returned by sqlite3_column_type() is undefined.
// (See sqlite3_column_type: http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ColumnType(index int) Type {
	return Type(C.sqlite3_column_type(s.stmt, C.int(index)))
}

// Scan result values from a query by name (name1, value1, ...)
// Example:
//	stmt, err := db.Prepare("SELECT 1 as id, 'test' as name")
//	// TODO error handling
//	defer stmt.Finalize()
//	var id int
//	var name string
//  err = s.Select(func(s *Stmt) (err error) {
//		if err = stmt.NamedScan("name", &name, "id", &id); err != nil {
//			return
//      }
//		fmt.Println(id, name)
//  	return
//  })
//	// TODO error handling
//
// NULL value is converted to 0 if arg type is *int,*int64,*float,*float64, to "" for *string, to []byte{} for *[]byte and to false for *bool.
// Calls sqlite3_column_(blob|double|int|int64|text) depending on args type.
// (See http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) NamedScan(args ...interface{}) error {
	if len(args)%2 != 0 {
		return s.specificError("Expected an even number of arguments")
	}
	for i := 0; i < len(args); i += 2 {
		name, ok := args[i].(string)
		if !ok {
			return s.specificError("non-string field name")
		}
		index, err := s.ColumnIndex(name) // How to look up only once for one statement ?
		if err != nil {
			return err
		}
		ptr := args[i+1]
		_, err = s.ScanByIndex(index, ptr)
		if err != nil {
			return err
		}
	}
	return nil
}

// Scan result values from a query
// Example:
//	stmt, err := db.Prepare("SELECT 1, 'test'")
//	// TODO error handling
//	defer stmt.Finalize()
//	var id int
//	var name string
//  err = s.Select(func(s *Stmt) error {
//		if err = stmt.Scan(&id, &name); err != nil {
//			return
//      }
//		fmt.Println(id, name)
//  	return
//  })
//	// TODO error handling
//
// NULL value is converted to 0 if arg type is *int,*int64,*float,*float64, to "" for *string, to []byte{} for *[]byte and to false for *bool.
// To avoid NULL conversion, arg type must be **T
// Calls sqlite3_column_(blob|double|int|int64|text) depending on args type.
// (See http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) Scan(args ...interface{}) error {
	n := s.ColumnCount()
	if n != len(args) { // What happens when the number of arguments is less than the number of columns?
		return s.specificError("incorrect argument count for Stmt.Scan: have %d want %d", len(args), n)
	}

	for i, v := range args {
		_, err := s.ScanByIndex(i, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// Retrieve statement SQL
// (See http://sqlite.org/c3ref/sql.html)
func (s *Stmt) SQL() string {
	return C.GoString(C.sqlite3_sql(s.stmt))
}

// Column index in a result set for a given column name
// Must scan all columns (but result is cached).
// (See http://sqlite.org/c3ref/column_name.html)
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
	return 0, s.specificError("invalid column name: %s", name)
}

// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_(blob|double|int|int64|text) depending on arg type.
// (See http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ScanByName(name string, value interface{}) (bool, error) {
	index, err := s.ColumnIndex(name)
	if err != nil {
		return false, err
	}
	return s.ScanByIndex(index, value)
}

// The leftmost column/index is number 0.
//
// Destination type is specified by the caller (except when value type is *interface{}).
// The value must be of one of the following types:
//    (*)*string,
//    (*)*int, (*)*int64, (*)*byte,
//    (*)*bool
//    (*)*float64
//    (*)*[]byte
//    *interface{}
//
// Returns true when column is null and Stmt.CheckNull is activated.
// Calls sqlite3_column_(blob|double|int|int64|text) depending on arg type.
// (See http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ScanByIndex(index int, value interface{}) (bool, error) {
	var isNull bool
	var err error
	switch value := value.(type) {
	case nil:
	case *string:
		*value, isNull = s.ScanText(index)
	case **string:
		var st string
		st, isNull = s.ScanText(index)
		if isNull {
			*value = nil
		} else {
			**value = st
		}
	case *int:
		*value, isNull, err = s.ScanInt(index)
	case **int:
		var i int
		i, isNull, err = s.ScanInt(index)
		if err == nil {
			if isNull {
				*value = nil
			} else {
				**value = i
			}
		}
	case *int64:
		*value, isNull, err = s.ScanInt64(index)
	case **int64:
		var i int64
		i, isNull, err = s.ScanInt64(index)
		if err == nil {
			if isNull {
				*value = nil
			} else {
				**value = i
			}
		}
	case *byte:
		*value, isNull, err = s.ScanByte(index)
	case **byte:
		var b byte
		b, isNull, err = s.ScanByte(index)
		if err == nil {
			if isNull {
				*value = nil
			} else {
				**value = b
			}
		}
	case *bool:
		*value, isNull, err = s.ScanBool(index)
	case **bool:
		var b bool
		b, isNull, err = s.ScanBool(index)
		if err == nil {
			if isNull {
				*value = nil
			} else {
				**value = b
			}
		}
	case *float64:
		*value, isNull, err = s.ScanDouble(index)
	case **float64:
		var f float64
		f, isNull, err = s.ScanDouble(index)
		if err == nil {
			if isNull {
				*value = nil
			} else {
				**value = f
			}
		}
	case *[]byte:
		*value, isNull = s.ScanBlob(index)
	case **[]byte:
		var bs []byte
		bs, isNull = s.ScanBlob(index)
		if isNull {
			*value = nil
		} else {
			**value = bs
		}
	case *interface{}:
		*value = s.ScanValue(index)
		isNull = *value == nil
	default:
		return false, s.specificError("unsupported type in Scan: %s", reflect.TypeOf(value))
	}
	return isNull, err
}

// The leftmost column/index is number 0.
//
// Destination type is decided by SQLite.
// The returned value will be of one of the following types:
//    nil
//    string
//    int64
//    float64
//    []byte
//
// Calls sqlite3_column_(blob|double|int|int64|text) depending on columns type.
// (See http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ScanValue(index int) (value interface{}) {
	switch s.ColumnType(index) {
	case Null:
		value = nil
	case Text:
		p := C.sqlite3_column_text(s.stmt, C.int(index))
		value = C.GoString((*C.char)(unsafe.Pointer(p)))
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

// Like ScanValue on several columns
func (s *Stmt) ScanValues(values []interface{}) {
	for i := range values {
		values[i] = s.ScanValue(i)
	}
}

// The leftmost column/index is number 0.
// Returns true when column is null.
// (See sqlite3_column_text: http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ScanText(index int) (value string, isNull bool) {
	p := C.sqlite3_column_text(s.stmt, C.int(index))
	if p == nil {
		isNull = true
	} else {
		value = C.GoString((*C.char)(unsafe.Pointer(p)))
	}
	return
}

// The leftmost column/index is number 0.
// Returns true when column is null and Stmt.CheckNull is activated.
// (See sqlite3_column_int: http://sqlite.org/c3ref/column_blob.html)
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
// (See sqlite3_column_int64: http://sqlite.org/c3ref/column_blob.html)
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
// (See sqlite3_column_int: http://sqlite.org/c3ref/column_blob.html)
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
// (See sqlite3_column_int: http://sqlite.org/c3ref/column_blob.html)
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
// (See sqlite3_column_double: http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ScanDouble(index int) (value float64, isNull bool, err error) {
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
// (See sqlite3_column_blob: http://sqlite.org/c3ref/column_blob.html)
func (s *Stmt) ScanBlob(index int) (value []byte, isNull bool) {
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
			return s.specificError("Type mismatch, source %s vs target %s", source, target)
		}
	case Float:
		switch source {
		case Text:
			fallthrough
		case Blob:
			return s.specificError("Type mismatch, source %s vs target %s", source, target)
		}
	}
	return nil
}

// Determine if a prepared statement has been reset
// (See http://sqlite.org/c3ref/stmt_busy.html)
/*func (s *Stmt) Busy() bool {
	return C.sqlite3_stmt_busy(s.stmt) != 0
}*/

// Destroy a prepared statement
// (See http://sqlite.org/c3ref/finalize.html)
func (s *Stmt) Finalize() error {
	rv := C.sqlite3_finalize(s.stmt)
	if rv != C.SQLITE_OK {
		return s.error(rv)
	}
	s.stmt = nil
	return nil
}

// Find the database handle of a prepared statement
// (Like http://sqlite.org/c3ref/db_handle.html)
func (s *Stmt) Conn() *Conn {
	return s.c
}

// Close a database connection and any dangling statements.
// (See http://sqlite.org/c3ref/close.html)
func (c *Conn) Close() error {
	if c == nil {
		return errors.New("nil sqlite database")
	}
	if c.db == nil {
		return nil
	}
	// Dangling statements
	stmt := C.sqlite3_next_stmt(c.db, nil)
	for stmt != nil {
		Log(C.SQLITE_MISUSE, "Dangling statement")
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

// Determine if an SQL statement writes the database
// (See http://sqlite.org/c3ref/stmt_readonly.html)
func (s *Stmt) ReadOnly() bool {
	return C.sqlite3_stmt_readonly(s.stmt) == 1
}

// Enable or disable extension loading
// (See http://sqlite.org/c3ref/enable_load_extension.html)
func (c *Conn) EnableLoadExtension(b bool) {
	C.sqlite3_enable_load_extension(c.db, btocint(b))
}

// Load an extension
// (See http://sqlite.org/c3ref/load_extension.html)
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
		return c.error(rv, C.GoString(errMsg))
	}
	return nil
}

// Enable or disable shared pager cache
// (See http://sqlite.org/c3ref/enable_shared_cache.html)
func EnableSharedCache(b bool) {
	C.sqlite3_enable_shared_cache(btocint(b))
}

// Check database integrity
// (See http://www.sqlite.org/pragma.html#pragma_integrity_check
// and http://www.sqlite.org/pragma.html#pragma_quick_check)
func (c *Conn) IntegrityCheck(max int, quick bool) error {
	var pragma string
	if quick {
		pragma = "quick"
	} else {
		pragma = "integrity"
	}
	msg, err := c.OneValue(fmt.Sprintf("PRAGMA %s_check(%d)", pragma, max))
	if err != nil {
		return err
	}
	if msg != "ok" {
		return c.specificError("Integrity check failed (%s)", msg)
	}
	return nil
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
