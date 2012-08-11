// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

void goSqlite3Trace(sqlite3 *db, void *udp);
void goSqlite3Profile(sqlite3 *db, void *udp);
int goSqlite3SetAuthorizer(sqlite3 *db, void *udp);
int goSqlite3BusyHandler(sqlite3 *db, void *udp);
void goSqlite3ProgressHandler(sqlite3 *db, int numOps, void *udp);

// cgo doesn't support varargs
static void my_log(int iErrCode, char *msg) {
	sqlite3_log(iErrCode, msg);
}

int goSqlite3ConfigLog(void *udp);
int goSqlite3ConfigThreadMode(int mode);
*/
import "C"

import "unsafe"

// See Conn.Trace
type Tracer func(udp interface{}, sql string)

type sqliteTrace struct {
	f   Tracer
	udp interface{}
}

//export goXTrace
func goXTrace(udp unsafe.Pointer, sql *C.char) {
	arg := (*sqliteTrace)(udp)
	arg.f(arg.udp, C.GoString(sql))
}

// Register or clear a trace function.
// (See sqlite3_trace, http://sqlite.org/c3ref/profile.html)
func (c *Conn) Trace(f Tracer, udp interface{}) {
	if f == nil {
		c.trace = nil
		C.sqlite3_trace(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.trace = &sqliteTrace{f, udp}
	C.goSqlite3Trace(c.db, unsafe.Pointer(c.trace))
}

// See Conn.Profile
type Profiler func(udp interface{}, sql string, nanoseconds uint64) // TODO time.Duration

type sqliteProfile struct {
	f   Profiler
	udp interface{}
}

//export goXProfile
func goXProfile(udp unsafe.Pointer, sql *C.char, nanoseconds C.sqlite3_uint64) {
	arg := (*sqliteProfile)(udp)
	arg.f(arg.udp, C.GoString(sql), uint64(nanoseconds))
}

// Register or clear a profile function.
// (See sqlite3_profile, http://sqlite.org/c3ref/profile.html)
func (c *Conn) Profile(f Profiler, udp interface{}) {
	if f == nil {
		c.profile = nil
		C.sqlite3_profile(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.profile = &sqliteProfile{f, udp}
	C.goSqlite3Profile(c.db, unsafe.Pointer(c.profile))
}

// Authorizer return codes
type Auth int

const (
	AUTH_OK     Auth = C.SQLITE_OK
	AUTH_DENY   Auth = C.SQLITE_DENY
	AUTH_IGNORE Auth = C.SQLITE_IGNORE
)

// Authorizer action codes
type Action int

const (
	CREATE_INDEX        Action = C.SQLITE_CREATE_INDEX
	CREATE_TABLE        Action = C.SQLITE_CREATE_TABLE
	CREATE_TEMP_INDEX   Action = C.SQLITE_CREATE_TEMP_INDEX
	CREATE_TEMP_TABLE   Action = C.SQLITE_CREATE_TEMP_TABLE
	CREATE_TEMP_TRIGGER Action = C.SQLITE_CREATE_TEMP_TRIGGER
	CREATE_TEMP_VIEW    Action = C.SQLITE_CREATE_TEMP_VIEW
	CREATE_TRIGGER      Action = C.SQLITE_CREATE_TRIGGER
	CREATE_VIEW         Action = C.SQLITE_CREATE_VIEW
	DELETE              Action = C.SQLITE_DELETE
	DROP_INDEX          Action = C.SQLITE_DROP_INDEX
	DROP_TABLE          Action = C.SQLITE_DROP_TABLE
	DROP_TEMP_INDEX     Action = C.SQLITE_DROP_TEMP_INDEX
	DROP_TEMP_TABLE     Action = C.SQLITE_DROP_TEMP_TABLE
	DROP_TEMP_TRIGGER   Action = C.SQLITE_DROP_TEMP_TRIGGER
	DROP_TEMP_VIEW      Action = C.SQLITE_DROP_TEMP_VIEW
	DROP_TRIGGER        Action = C.SQLITE_DROP_TRIGGER
	DROP_VIEW           Action = C.SQLITE_DROP_VIEW
	INSERT              Action = C.SQLITE_INSERT
	PRAGMA              Action = C.SQLITE_PRAGMA
	READ                Action = C.SQLITE_READ
	SELECT              Action = C.SQLITE_SELECT
	TRANSACTION         Action = C.SQLITE_TRANSACTION
	UPDATE              Action = C.SQLITE_UPDATE
	ATTACH              Action = C.SQLITE_ATTACH
	DETACH              Action = C.SQLITE_DETACH
	ALTER_TABLE         Action = C.SQLITE_ALTER_TABLE
	REINDEX             Action = C.SQLITE_REINDEX
	ANALYZE             Action = C.SQLITE_ANALYZE
	CREATE_VTABLE       Action = C.SQLITE_CREATE_VTABLE
	DROP_VTABLE         Action = C.SQLITE_DROP_VTABLE
	FUNCTION            Action = C.SQLITE_FUNCTION
	SAVEPOINT           Action = C.SQLITE_SAVEPOINT
	COPY                Action = C.SQLITE_COPY
)

// See Conn.SetAuthorizer
type Authorizer func(udp interface{}, action Action, arg1, arg2, dbName, triggerName string) Auth

type sqliteAuthorizer struct {
	f   Authorizer
	udp interface{}
}

//export goXAuth
func goXAuth(udp unsafe.Pointer, action int, arg1, arg2, dbName, triggerName *C.char) C.int {
	arg := (*sqliteAuthorizer)(udp)
	result := arg.f(arg.udp, Action(action), C.GoString(arg1), C.GoString(arg2), C.GoString(dbName), C.GoString(triggerName))
	return C.int(result)
}

// Set or clear the access authorization function.
// (See http://sqlite.org/c3ref/set_authorizer.html)
func (c *Conn) SetAuthorizer(f Authorizer, udp interface{}) error {
	if f == nil {
		c.authorizer = nil
		return c.error(C.sqlite3_set_authorizer(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.authorizer = &sqliteAuthorizer{f, udp}
	return c.error(C.goSqlite3SetAuthorizer(c.db, unsafe.Pointer(c.authorizer)))
}

// Returns true to try again.
// See Conn.BusyHandler
type BusyHandler func(udp interface{}, count int) bool

type sqliteBusyHandler struct {
	f   BusyHandler
	udp interface{}
}

//export goXBusy
func goXBusy(udp unsafe.Pointer, count int) C.int {
	arg := (*sqliteBusyHandler)(udp)
	result := arg.f(arg.udp, count)
	return btocint(result)
}

// Register a callback to handle SQLITE_BUSY errors
// (See http://sqlite.org/c3ref/busy_handler.html)
func (c *Conn) BusyHandler(f BusyHandler, udp interface{}) error {
	if f == nil {
		c.busyHandler = nil
		return c.error(C.sqlite3_busy_handler(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.busyHandler = &sqliteBusyHandler{f, udp}
	return c.error(C.goSqlite3BusyHandler(c.db, unsafe.Pointer(c.busyHandler)))
}

// Returns true to interrupt.
// See Conn.ProgressHandler
type ProgressHandler func(udp interface{}) bool

type sqliteProgressHandler struct {
	f   ProgressHandler
	udp interface{}
}

//export goXProgress
func goXProgress(udp unsafe.Pointer) C.int {
	arg := (*sqliteProgressHandler)(udp)
	result := arg.f(arg.udp)
	return btocint(result)
}

// Query progress callbacks
// The progress callback will be invoked every numOps opcodes.
// (See http://sqlite.org/c3ref/progress_handler.html)
func (c *Conn) ProgressHandler(f ProgressHandler, numOps int, udp interface{}) {
	if f == nil {
		c.progressHandler = nil
		C.sqlite3_progress_handler(c.db, 0, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.progressHandler = &sqliteProgressHandler{f, udp}
	C.goSqlite3ProgressHandler(c.db, C.int(numOps), unsafe.Pointer(c.progressHandler))
}

// Status parameters for prepared statements
type StmtStatus int

const (
	STMTSTATUS_FULLSCAN_STEP StmtStatus = C.SQLITE_STMTSTATUS_FULLSCAN_STEP
	STMTSTATUS_SORT          StmtStatus = C.SQLITE_STMTSTATUS_SORT
	STMTSTATUS_AUTOINDEX     StmtStatus = C.SQLITE_STMTSTATUS_AUTOINDEX
)

// Return the value of a status counter for a prepared statement
// (See http://sqlite.org/c3ref/stmt_status.html)
func (s *Stmt) Status(op StmtStatus, reset bool) int {
	return int(C.sqlite3_stmt_status(s.stmt, C.int(op), btocint(reset)))
}

// Memory allocator statistics
// (See sqlite3_memory_used: http://sqlite.org/c3ref/memory_highwater.html)
func MemoryUsed() int64 {
	return int64(C.sqlite3_memory_used())
}

// Memory allocator statistics
// (See sqlite3_memory_highwater: http://sqlite.org/c3ref/memory_highwater.html)
func MemoryHighwater(reset bool) int64 {
	return int64(C.sqlite3_memory_highwater(btocint(reset)))
}

// Limit on heap size
// (See http://sqlite.org/c3ref/soft_heap_limit64.html)
func SoftHeapLimit() int64 {
	return SetSoftHeapLimit(-1)
}

// Impose a limit on heap size
// (See http://sqlite.org/c3ref/soft_heap_limit64.html)
func SetSoftHeapLimit(n int64) int64 {
	return int64(C.sqlite3_soft_heap_limit64(C.sqlite3_int64(n)))
}

// Determine if an SQL statement is complete
// (See http://sqlite.org/c3ref/complete.html)
func Complete(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.sqlite3_complete(cs) != 0
}

// Error logging interface
// (See http://sqlite.org/c3ref/log.html)
func Log(err /*Errno*/ int, msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.my_log(C.int(err), cs)
}

// See ConfigLog
type Logger func(udp interface{}, err error, msg string)

type sqliteLogger struct {
	f   Logger
	udp interface{}
}

//export goXLog
func goXLog(udp unsafe.Pointer, err int, msg *C.char) {
	arg := (*sqliteLogger)(udp)
	arg.f(arg.udp, Errno(err), C.GoString(msg))
	return
}

var logger *sqliteLogger

// Configure the logger of the SQLite library
// (See sqlite3_config(SQLITE_CONFIG_LOG,...): http://sqlite.org/c3ref/config.html)
func ConfigLog(f Logger, udp interface{}) error {
	var rv C.int
	if f == nil {
		logger = nil
		rv = C.goSqlite3ConfigLog(nil)
	} else {
		// To make sure it is not gced, keep a reference.
		logger = &sqliteLogger{f, udp}
		rv = C.goSqlite3ConfigLog(unsafe.Pointer(logger))
	}
	if rv == C.SQLITE_OK {
		return nil
	}
	return Errno(rv)
}

type ThreadMode int

const (
	SINGLETHREAD ThreadMode = C.SQLITE_CONFIG_SINGLETHREAD
	MULTITHREAD  ThreadMode = C.SQLITE_CONFIG_MULTITHREAD
	SERIALIZED   ThreadMode = C.SQLITE_CONFIG_SERIALIZED
)

// (See sqlite3_config(SQLITE_CONFIG_SINGLETHREAD|SQLITE_CONFIG_MULTITHREAD|SQLITE_CONFIG_SERIALIZED): http://sqlite.org/c3ref/config.html)
func ConfigThreadMode(mode ThreadMode) error {
	rv := C.goSqlite3ConfigThreadMode(C.int(mode))
	if rv == C.SQLITE_OK {
		return nil
	}
	return Errno(rv)
}
