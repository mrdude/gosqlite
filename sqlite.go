// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.
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
	"os"
	"strconv"
	"time"
	"unsafe"
)

type ConnError struct {
	c       *Conn
	code    Errno  // thread safe error code
	msg     string // it might be the case that a second error occurs on a separate thread in between the time of the first error and the call to retrieve this message.
	details string // contextual informations, thread safe
}

func (e *ConnError) Code() Errno {
	return e.code
}

// FIXME it might be the case that a second error occurs on a separate thread in between the time of the first error and the call to this method.
func (e *ConnError) ExtendedCode() int {
	return int(C.sqlite3_extended_errcode(e.c.db))
}

// Filename returns database file name from which the error comes from.
func (e *ConnError) Filename() string {
	return e.c.Filename("main")
}

func (e *ConnError) Error() string { // FIXME code.Error() & e.msg are often redundant...
	if len(e.details) > 0 {
		return fmt.Sprintf("%s (%s) (%s)", e.msg, e.details, e.code.Error())
	} else if len(e.msg) > 0 {
		return fmt.Sprintf("%s (%s)", e.msg, e.code.Error())
	}
	return e.code.Error()
}

// Result codes
type Errno int

func (e Errno) Error() string {
	var s string
	if e == ErrSpecific {
		s = "wrapper specific error"
	} else {
		s = C.GoString(C.sqlite3_errstr(C.int(e))) // thread safe
	}
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

const (
	ErrError      = Errno(C.SQLITE_ERROR)      /* SQL error or missing database */
	ErrInternal   = Errno(C.SQLITE_INTERNAL)   /* Internal logic error in SQLite */
	ErrPerm       = Errno(C.SQLITE_PERM)       /* Access permission denied */
	ErrAbort      = Errno(C.SQLITE_ABORT)      /* Callback routine requested an abort */
	ErrBusy       = Errno(C.SQLITE_BUSY)       /* The database file is locked */
	ErrLocked     = Errno(C.SQLITE_LOCKED)     /* A table in the database is locked */
	ErrNoMem      = Errno(C.SQLITE_NOMEM)      /* A malloc() failed */
	ErrReadOnly   = Errno(C.SQLITE_READONLY)   /* Attempt to write a readonly database */
	ErrInterrupt  = Errno(C.SQLITE_INTERRUPT)  /* Operation terminated by sqlite3_interrupt()*/
	ErrIOErr      = Errno(C.SQLITE_IOERR)      /* Some kind of disk I/O error occurred */
	ErrCorrupt    = Errno(C.SQLITE_CORRUPT)    /* The database disk image is malformed */
	ErrNotFound   = Errno(C.SQLITE_NOTFOUND)   /* Unknown opcode in sqlite3_file_control() */
	ErrFull       = Errno(C.SQLITE_FULL)       /* Insertion failed because database is full */
	ErrCantOpen   = Errno(C.SQLITE_CANTOPEN)   /* Unable to open the database file */
	ErrProtocol   = Errno(C.SQLITE_PROTOCOL)   /* Database lock protocol error */
	ErrEmpty      = Errno(C.SQLITE_EMPTY)      /* Database is empty */
	ErrSchema     = Errno(C.SQLITE_SCHEMA)     /* The database schema changed */
	ErrTooBig     = Errno(C.SQLITE_TOOBIG)     /* String or BLOB exceeds size limit */
	ErrConstraint = Errno(C.SQLITE_CONSTRAINT) /* Abort due to constraint violation */
	ErrMismatch   = Errno(C.SQLITE_MISMATCH)   /* Data type mismatch */
	ErrMisuse     = Errno(C.SQLITE_MISUSE)     /* Library used incorrectly */
	ErrNolfs      = Errno(C.SQLITE_NOLFS)      /* Uses OS features not supported on host */
	ErrAuth       = Errno(C.SQLITE_AUTH)       /* Authorization denied */
	ErrFormat     = Errno(C.SQLITE_FORMAT)     /* Auxiliary database format error */
	ErrRange      = Errno(C.SQLITE_RANGE)      /* 2nd parameter to sqlite3_bind out of range */
	ErrNotDB      = Errno(C.SQLITE_NOTADB)     /* File opened that is not a database file */
	Notice        = Errno(C.SQLITE_NOTICE)     /* Notifications from sqlite3_log() */
	Warning       = Errno(C.SQLITE_WARNING)    /* Warnings from sqlite3_log() */
	Row           = Errno(C.SQLITE_ROW)        /* sqlite3_step() has another row ready */
	Done          = Errno(C.SQLITE_DONE)       /* sqlite3_step() has finished executing */
	ErrSpecific   = Errno(-1)                  /* Wrapper specific error */
)

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

// LastError returns the error for the most recent failed sqlite3_* API call associated with a database connection.
// (See http://sqlite.org/c3ref/errcode.html)
func (c *Conn) LastError() error {
	if c == nil {
		return errors.New("nil sqlite database")
	}
	errorCode := C.sqlite3_errcode(c.db)
	if errorCode == C.SQLITE_OK {
		return nil
	}
	return &ConnError{c: c, code: Errno(errorCode), msg: C.GoString(C.sqlite3_errmsg(c.db))}
}

// Database connection handle
// (See http://sqlite.org/c3ref/sqlite3.html)
type Conn struct {
	db              *C.sqlite3
	stmtCache       *cache
	authorizer      *sqliteAuthorizer
	busyHandler     *sqliteBusyHandler
	profile         *sqliteProfile
	progressHandler *sqliteProgressHandler
	trace           *sqliteTrace
	commitHook      *sqliteCommitHook
	rollbackHook    *sqliteRollbackHook
	updateHook      *sqliteUpdateHook
	udfs            map[string]*sqliteFunction
	modules         map[string]*sqliteModule
	timeUsed        time.Time
	nTransaction    uint8
}

// Version returns the run-time library version number
// (See http://sqlite.org/c3ref/libversion.html)
func Version() string {
	p := C.sqlite3_libversion()
	return C.GoString(p)
}

// Flags for file open operations
type OpenFlag int

const (
	OpenReadOnly     OpenFlag = C.SQLITE_OPEN_READONLY
	OpenReadWrite    OpenFlag = C.SQLITE_OPEN_READWRITE
	OpenCreate       OpenFlag = C.SQLITE_OPEN_CREATE
	OpenUri          OpenFlag = C.SQLITE_OPEN_URI
	OpenNoMutex      OpenFlag = C.SQLITE_OPEN_NOMUTEX
	OpenFullMutex    OpenFlag = C.SQLITE_OPEN_FULLMUTEX
	OpenSharedCache  OpenFlag = C.SQLITE_OPEN_SHAREDCACHE
	OpenPrivateCache OpenFlag = C.SQLITE_OPEN_PRIVATECACHE
)

// Open opens a new database connection.
// ":memory:" for memory db,
// "" for temp file db
//
// (See sqlite3_open_v2: http://sqlite.org/c3ref/open.html)
func Open(filename string, flags ...OpenFlag) (*Conn, error) {
	return OpenVfs(filename, "", flags...)
}

// OpenVfs opens a new database with a specified virtual file system.
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
	cname := C.CString(filename)
	defer C.free(unsafe.Pointer(cname))
	var vfs *C.char
	if len(vfsname) > 0 {
		vfs = C.CString(vfsname)
		defer C.free(unsafe.Pointer(vfs))
	}
	rv := C.sqlite3_open_v2(cname, &db, C.int(openFlags), vfs)
	if rv != C.SQLITE_OK {
		if db != nil {
			C.sqlite3_close(db)
		}
		return nil, Errno(rv)
	}
	if db == nil {
		return nil, errors.New("sqlite succeeded without returning a database")
	}
	c := &Conn{db: db, stmtCache: newCache()}
	if os.Getenv("SQLITE_DEBUG") != "" {
		c.SetAuthorizer(authorizer, c.db)
		c.SetCacheSize(0)
	}

	return c, nil
}

func authorizer(d interface{}, action Action, arg1, arg2, dbName, triggerName string) Auth {
	fmt.Fprintf(os.Stderr, "%p: %v, %s, %s, %s, %s\n", d, action, arg1, arg2, dbName, triggerName)
	return AuthOk
}

// BusyTimeout sets a busy timeout.
// (See http://sqlite.org/c3ref/busy_timeout.html)
func (c *Conn) BusyTimeout(d time.Duration) error {
	c.busyHandler = nil
	return c.error(C.sqlite3_busy_timeout(c.db, C.int(d/time.Millisecond)), "Conn.BusyTimeout")
}

// EnableFKey enables or disables the enforcement of foreign key constraints.
// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, b).
// Another way is PRAGMA foreign_keys = boolean;
//
// (See http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html)
func (c *Conn) EnableFKey(b bool) (bool, error) {
	return c.queryOrSetEnableDbConfig(C.SQLITE_DBCONFIG_ENABLE_FKEY, btocint(b))
}

// IsFKeyEnabled reports if the enforcement of foreign key constraints is enabled or not.
// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_FKEY, -1).
// Another way is PRAGMA foreign_keys;
//
// (See http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html)
func (c *Conn) IsFKeyEnabled() (bool, error) {
	return c.queryOrSetEnableDbConfig(C.SQLITE_DBCONFIG_ENABLE_FKEY, -1)
}

// EnableTriggers enables or disables triggers.
// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, b).
//
// (See http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html)
func (c *Conn) EnableTriggers(b bool) (bool, error) {
	return c.queryOrSetEnableDbConfig(C.SQLITE_DBCONFIG_ENABLE_TRIGGER, btocint(b))
}

// AreTriggersEnabled checks if triggers are enabled.
// Calls sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_TRIGGER, -1)
//
// (See http://sqlite.org/c3ref/c_dbconfig_enable_fkey.html)
func (c *Conn) AreTriggersEnabled() (bool, error) {
	return c.queryOrSetEnableDbConfig(C.SQLITE_DBCONFIG_ENABLE_TRIGGER, -1)
}

func (c *Conn) queryOrSetEnableDbConfig(key, i C.int) (bool, error) {
	var ok C.int
	rv := C.my_db_config(c.db, C.SQLITE_DBCONFIG_ENABLE_FKEY, i, &ok)
	if rv == C.SQLITE_OK {
		return (ok == 1), nil
	}
	return false, c.error(rv)
}

// EnableExtendedResultCodes enables or disables the extended result codes feature of SQLite.
// (See http://sqlite.org/c3ref/extended_result_codes.html)
func (c *Conn) EnableExtendedResultCodes(b bool) error {
	return c.error(C.sqlite3_extended_result_codes(c.db, btocint(b)), "Conn.EnableExtendedResultCodes")
}

// Readonly determines if a database is read-only.
// (See http://sqlite.org/c3ref/db_readonly.html)
func (c *Conn) Readonly(dbName string) (bool, error) {
	cname := C.CString(dbName)
	defer C.free(unsafe.Pointer(cname))
	rv := C.sqlite3_db_readonly(c.db, cname)
	if rv == -1 {
		return false, c.specificError("%q is not the name of a database", dbName)
	}
	return rv == 1, nil
}

// Filename returns the filename for a database connection.
// (See http://sqlite.org/c3ref/db_filename.html)
func (c *Conn) Filename(dbName string) string {
	cname := C.CString(dbName)
	defer C.free(unsafe.Pointer(cname))
	return C.GoString(C.sqlite3_db_filename(c.db, cname))
}

// Exec prepares and executes one or many parameterized statement(s) (separated by semi-colon).
// Don't use it with SELECT or anything that returns data.
func (c *Conn) Exec(cmd string, args ...interface{}) error {
	for len(cmd) > 0 {
		s, err := c.prepare(cmd)
		if err != nil {
			return err
		} else if s.stmt == nil {
			// this happens for a comment or white-space
			cmd = s.tail
			continue
		}
		var subargs []interface{}
		count := s.BindParameterCount()
		if len(s.tail) > 0 && len(args) >= count {
			subargs = args[:count]
			args = args[count:]
		} else {
			subargs = args
		}
		err = s.Exec(subargs...)
		if err != nil {
			s.finalize()
			return err
		}
		if err = s.finalize(); err != nil {
			return err
		}
		cmd = s.tail
	}
	return nil
}

// Exists returns true if the specified query returns at least one row.
func (c *Conn) Exists(query string, args ...interface{}) (bool, error) {
	s, err := c.Prepare(query, args...)
	if err != nil {
		return false, err
	}
	defer s.Finalize()
	ok, err := s.Next()
	if err != nil {
		return false, err
	}
	if s.ColumnCount() == 0 {
		return false, s.specificError("don't use Exists with query that returns no data such as %q", query)
	}
	return ok, nil
}

// OneValue is used with SELECT that returns only one row with only one column.
// Returns io.EOF when there is no row.
// No check is performed to ensure that there is no more than one row.
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
		if s.ColumnCount() == 0 {
			return s.specificError("don't use OneValue with query that returns no data such as %q", query)
		}
		return io.EOF
	}
	return s.Scan(value)
}

// Changes returns the number of database rows that were changed or inserted or deleted by the most recently completed SQL statement on the database connection.
// If a separate thread makes changes on the same database connection while Changes() is running then the value returned is unpredictable and not meaningful.
// (See http://sqlite.org/c3ref/changes.html)
func (c *Conn) Changes() int {
	return int(C.sqlite3_changes(c.db))
}

// TotalChanges returns the number of row changes caused by INSERT, UPDATE or DELETE statements since the database connection was opened.
// (See http://sqlite.org/c3ref/total_changes.html)
func (c *Conn) TotalChanges() int {
	return int(C.sqlite3_total_changes(c.db))
}

// LastInsertRowid returns the rowid of the most recent successful INSERT into the database.
// If a separate thread performs a new INSERT on the same database connection while the LastInsertRowid() function is running and thus changes the last insert rowid, then the value returned by LastInsertRowid() is unpredictable and might not equal either the old or the new last insert rowid.
// (See http://sqlite.org/c3ref/last_insert_rowid.html)
func (c *Conn) LastInsertRowid() int64 {
	return int64(C.sqlite3_last_insert_rowid(c.db))
}

// Interrupt interrupts a long-running query.
// (See http://sqlite.org/c3ref/interrupt.html)
func (c *Conn) Interrupt() {
	C.sqlite3_interrupt(c.db)
}

// GetAutocommit tests for auto-commit mode.
// (See http://sqlite.org/c3ref/get_autocommit.html)
func (c *Conn) GetAutocommit() bool {
	return C.sqlite3_get_autocommit(c.db) != 0
}

// See Conn.BeginTransaction
type TransactionType int

const (
	Deferred  TransactionType = 0
	Immediate TransactionType = 1
	Exclusive TransactionType = 2
)

// Begin begins a transaction in deferred mode.
// (See http://www.sqlite.org/lang_transaction.html)
func (c *Conn) Begin() error {
	return c.BeginTransaction(Deferred)
}

// BeginTransaction begins a transaction of the specified type.
// (See http://www.sqlite.org/lang_transaction.html)
func (c *Conn) BeginTransaction(t TransactionType) error {
	if t == Deferred {
		return c.FastExec("BEGIN")
	} else if t == Immediate {
		return c.FastExec("BEGIN IMMEDIATE")
	} else if t == Exclusive {
		return c.FastExec("BEGIN EXCLUSIVE")
	}
	panic(fmt.Sprintf("Unsupported transaction type: '%#v'", t))
	return nil // Go 1.1 unreachable code
}

// Commit commits transaction
func (c *Conn) Commit() error {
	// TODO Check autocommit?
	return c.FastExec("COMMIT")
}

// Rollback rollbacks transaction
func (c *Conn) Rollback() error {
	// TODO Check autocommit?
	return c.FastExec("ROLLBACK")
}

// Transaction is used to execute a function inside an SQLite database transaction.
// The transaction is committed when the function completes (with no error),
// or it rolls back if the function fails.
// If the transaction occurs within another transaction (only one that is started using this method) a Savepoint is created.
// Two errors may be returned: the first is the one returned by the f function,
// the second is the one returned by begin/commit/rollback.
// (See http://sqlite.org/tclsqlite.html#transaction)
func (c *Conn) Transaction(t TransactionType, f func(c *Conn) error) (err error) {
	if c.nTransaction == 0 {
		err = c.BeginTransaction(t)
	} else {
		err = c.Savepoint(strconv.Itoa(int(c.nTransaction)))
	}
	if err != nil {
		return
	}
	c.nTransaction++
	defer func() {
		c.nTransaction--
		if err != nil {
			_, ko := err.(*ConnError)
			if c.nTransaction == 0 || ko {
				c.Rollback()
			} else {
				if rerr := c.RollbackSavepoint(strconv.Itoa(int(c.nTransaction))); rerr != nil {
					Log(-1, rerr.Error())
				} else if rerr := c.ReleaseSavepoint(strconv.Itoa(int(c.nTransaction))); rerr != nil {
					Log(-1, rerr.Error())
				}
			}
		} else {
			if c.nTransaction == 0 {
				err = c.Commit()
			} else {
				err = c.ReleaseSavepoint(strconv.Itoa(int(c.nTransaction)))
			}
			if err != nil {
				c.Rollback()
			}
		}
	}()
	err = f(c)
	return
}

// Savepoint starts a new transaction with a name.
// (See http://sqlite.org/lang_savepoint.html)
func (c *Conn) Savepoint(name string) error {
	return c.FastExec(Mprintf("SAVEPOINT %Q", name))
}

// ReleaseSavepoint causes all savepoints back to and including the most recent savepoint with a matching name to be removed from the transaction stack.
// (See http://sqlite.org/lang_savepoint.html)
func (c *Conn) ReleaseSavepoint(name string) error {
	return c.FastExec(Mprintf("RELEASE %Q", name))
}

// RollbackSavepoint reverts the state of the database back to what it was just before the corresponding SAVEPOINT.
// (See http://sqlite.org/lang_savepoint.html)
func (c *Conn) RollbackSavepoint(name string) error {
	return c.FastExec(Mprintf("ROLLBACK TO SAVEPOINT %Q", name))
}

func (c *Conn) exec(cmd string) error {
	s, err := c.prepare(cmd)
	if err != nil {
		return err
	}
	defer s.finalize()
	rv := C.sqlite3_step(s.stmt)
	if Errno(rv) != Done { // this check cannot be done with sqlite3_exec
		return s.error(rv, "Conn.exec(%q)", cmd)
	}
	return nil
}

// FastExec executes one or many non-parameterized statement(s) (separated by semi-colon) with no control and no stmt cache.
func (c *Conn) FastExec(cmd string) error {
	cmdstr := C.CString(cmd)
	defer C.free(unsafe.Pointer(cmdstr))
	return c.error(C.sqlite3_exec(c.db, cmdstr, nil, nil, nil))
}

// Close closes a database connection and any dangling statements.
// (See http://sqlite.org/c3ref/close.html)
func (c *Conn) Close() error {
	if c == nil {
		return errors.New("nil sqlite database")
	}
	if c.db == nil {
		return nil
	}

	c.stmtCache.flush()

	// Dangling statements
	stmt := C.sqlite3_next_stmt(c.db, nil)
	for stmt != nil {
		if C.sqlite3_stmt_busy(stmt) != 0 {
			Log(C.SQLITE_MISUSE, "Dangling statement (not reset): \""+C.GoString(C.sqlite3_sql(stmt))+"\"")
		} else {
			Log(C.SQLITE_MISUSE, "Dangling statement (not finalize): \""+C.GoString(C.sqlite3_sql(stmt))+"\"")
		}
		C.sqlite3_finalize(stmt)
		stmt = C.sqlite3_next_stmt(c.db, nil)
	}

	rv := C.sqlite3_close(c.db)
	if rv != C.SQLITE_OK {
		Log(int(rv), "error while closing Conn")
		return c.error(rv, "Conn.Close")
	}
	c.db = nil
	return nil
}

func (c *Conn) IsClosed() bool {
	return c == nil || c.db == nil
}

// EnableLoadExtension enables or disables extension loading.
// (See http://sqlite.org/c3ref/enable_load_extension.html)
func (c *Conn) EnableLoadExtension(b bool) error {
	rv := C.sqlite3_enable_load_extension(c.db, btocint(b))
	if rv == C.SQLITE_OK {
		return nil
	}
	return c.error(rv, "Conn.EnableLoadExtension")
}

// LoadExtension loads an extension
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

// EnableSharedCache enables or disables shared pager cache
// (See http://sqlite.org/c3ref/enable_shared_cache.html)
func EnableSharedCache(b bool) error {
	rv := C.sqlite3_enable_shared_cache(btocint(b))
	if rv == C.SQLITE_OK {
		return nil
	}
	return Errno(rv)
}
