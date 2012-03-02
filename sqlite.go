// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.
//
// Simple example:
//	db, err := sqlite.Open("/path/to/db")
//	if err != nil {
//		...
//	}
//	defer db.Close()
//  err = db.Exec("CREATE TABLE test(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE); -- ... and other ddls separated by semi-colon")
//  ...
//  ins, err := db.Prepare("INSERT INTO test (name) VALUES (?)")
//  if err != nil {
//    ...
//	}
//	defer ins.Finalize()
//  rowId, err := ins.Insert("Bart")
//  ...
//	s, err := db.Prepare("SELECT name from test WHERE name like ?", "%a%")
//  ...
//  defer s.Finalize()
//  var name string
//  err = s.Select(func(s *Stmt) (err error) {
//		err = s.Scan(&name)
//      ...
//		fmt.Printf("%s\n", name)
//	})
package sqlite

/*
#cgo LDFLAGS: -lsqlite3

#include <sqlite3.h>
#include <stdlib.h>

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

func (e *ConnError) Error() string { // FIXME code.Error() & e.msg are often redundant...
	if len(e.details) > 0 {
		return fmt.Sprintf("%s: %s (%s)", e.code.Error(), e.msg, e.details)
	} else if len(e.msg) > 0 {
		return fmt.Sprintf("%s: %s", e.code.Error(), e.msg)
	}
	return e.code.Error()
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
	ErrSpecific         = Errno(-1)                  /* Wrapper specific error */
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
	ErrSpecific:         "Wrapper specific error",
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

func (c *Conn) specificError(msg string, a ...interface{}) error {
	return &ConnError{c: c, code: ErrSpecific, msg: fmt.Sprintf(msg, a...)}
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
func (c *Conn) BusyTimeout(ms int) error { // TODO time.Duration ?
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
//	err := db.Exec("CREATE TABLE test(id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL); -- ...")
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
func (c *Conn) OneValue(query string, value interface{}, args ...interface{}) error {
	s, err := c.Prepare(query, args...)
	if err != nil {
		return err
	}
	defer s.Finalize()
	b, err := s.Next()
	if err != nil {
		return err
	} else if !b {
		return io.EOF
	}
	return s.Scan(value)
}

// Count the number of rows modified.
// If a separate thread makes changes on the same database connection while Changes() is running then the value returned is unpredictable and not meaningful.
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
// If a separate thread performs a new INSERT on the same database connection while the LastInsertRowid() function is running and thus changes the last insert rowid, then the value returned by LastInsertRowid() is unpredictable and might not equal either the old or the new last insert rowid.
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
		if C.sqlite3_stmt_busy(stmt) != 0 {
			Log(C.SQLITE_MISUSE, "Dangling statement (not reset)")
		} else {
			Log(C.SQLITE_MISUSE, "Dangling statement (not finalize)")
		}
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
