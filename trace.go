// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

extern void goXTrace(void *udp, const char *sql);

static void goSqlite3Trace(sqlite3 *db, void *udp) {
	sqlite3_trace(db, goXTrace, udp);
}
extern void goXProfile(void *udp, const char *sql, sqlite3_uint64 nanoseconds);

static void goSqlite3Profile(sqlite3 *db, void *udp) {
	sqlite3_profile(db, goXProfile, udp);
}

extern int goXAuth(void *udp, int action, const char *arg1, const char *arg2, const char *dbName, const char *triggerName);

static int goSqlite3SetAuthorizer(sqlite3 *db, void *udp) {
	return sqlite3_set_authorizer(db, goXAuth, udp);
}

extern int goXBusy(void *udp, int count);

static int goSqlite3BusyHandler(sqlite3 *db, void *udp) {
	return sqlite3_busy_handler(db, goXBusy, udp);
}

extern int goXProgress(void *udp);

static void goSqlite3ProgressHandler(sqlite3 *db, int numOps, void *udp) {
	sqlite3_progress_handler(db, numOps, goXProgress, udp);
}

// cgo doesn't support varargs
static void my_log(int iErrCode, char *msg) {
	sqlite3_log(iErrCode, msg);
}
*/
import "C"

import "unsafe"

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

// Calls sqlite3_trace, http://sqlite.org/c3ref/profile.html
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

type Profiler func(udp interface{}, sql string, nanoseconds uint64)

type sqliteProfile struct {
	f   Profiler
	udp interface{}
}

//export goXProfile
func goXProfile(udp unsafe.Pointer, sql *C.char, nanoseconds C.sqlite3_uint64) {
	arg := (*sqliteProfile)(udp)
	arg.f(arg.udp, C.GoString(sql), uint64(nanoseconds))
}

// Calls sqlite3_profile, http://sqlite.org/c3ref/profile.html
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

type Auth int

const (
	AUTH_OK     Auth = C.SQLITE_OK
	AUTH_DENY   Auth = C.SQLITE_DENY
	AUTH_IGNORE Auth = C.SQLITE_IGNORE
)

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

type Authorizer func(udp interface{}, action Action, arg1, arg2, dbName, triggerName string) Auth

type sqliteAuthorizer struct {
	f   Authorizer
	udp interface{}
}

//export goXAuth
func goXAuth(udp unsafe.Pointer, action C.int, arg1, arg2, dbName, triggerName *C.char) C.int {
	arg := (*sqliteAuthorizer)(udp)
	result := arg.f(arg.udp, Action(action), C.GoString(arg1), C.GoString(arg2), C.GoString(dbName), C.GoString(triggerName))
	return C.int(result)
}

// Calls http://sqlite.org/c3ref/set_authorizer.html
func (c *Conn) SetAuthorizer(f Authorizer, udp interface{}) error {
	if f == nil {
		c.authorizer = nil
		return c.error(C.sqlite3_set_authorizer(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.authorizer = &sqliteAuthorizer{f, udp}
	return c.error(C.goSqlite3SetAuthorizer(c.db, unsafe.Pointer(c.authorizer)))
}

type BusyHandler func(udp interface{}, count int) int

type sqliteBusyHandler struct {
	f   BusyHandler
	udp interface{}
}

//export goXBusy
func goXBusy(udp unsafe.Pointer, count C.int) C.int {
	arg := (*sqliteBusyHandler)(udp)
	result := arg.f(arg.udp, int(count))
	return C.int(result)
}

// TODO NOT TESTED
// Calls http://sqlite.org/c3ref/busy_handler.html
func (c *Conn) BusyHandler(f BusyHandler, udp interface{}) error {
	if f == nil {
		c.busyHandler = nil
		return c.error(C.sqlite3_busy_handler(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.busyHandler = &sqliteBusyHandler{f, udp}
	return c.error(C.goSqlite3BusyHandler(c.db, unsafe.Pointer(c.busyHandler)))
}

// Returns non-zero to interrupt.
type ProgressHandler func(udp interface{}) int

type sqliteProgressHandler struct {
	f   ProgressHandler
	udp interface{}
}

//export goXProgress
func goXProgress(udp unsafe.Pointer) C.int {
	arg := (*sqliteProgressHandler)(udp)
	result := arg.f(arg.udp)
	return C.int(result)
}

// Calls http://sqlite.org/c3ref/progress_handler.html
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

type StmtStatus int

const (
	STMTSTATUS_FULLSCAN_STEP StmtStatus = C.SQLITE_STMTSTATUS_FULLSCAN_STEP
	STMTSTATUS_SORT          StmtStatus = C.SQLITE_STMTSTATUS_SORT
	STMTSTATUS_AUTOINDEX     StmtStatus = C.SQLITE_STMTSTATUS_AUTOINDEX
)

// Calls http://sqlite.org/c3ref/stmt_status.html
func (s *Stmt) Status(op StmtStatus, reset bool) int {
	return int(C.sqlite3_stmt_status(s.stmt, C.int(op), btocint(reset)))
}

// Calls sqlite3_memory_used: http://sqlite.org/c3ref/memory_highwater.html
func MemoryUsed() int64 {
	return int64(C.sqlite3_memory_used())
}
// Calls sqlite3_memory_highwater: http://sqlite.org/c3ref/memory_highwater.html
func MemoryHighwater(reset bool) int64 {
	return int64(C.sqlite3_memory_highwater(btocint(reset)))
}

// Calls http://sqlite.org/c3ref/soft_heap_limit64.html
func SoftHeapLimit() int64 {
	return SetSoftHeapLimit(-1)
}
// Calls http://sqlite.org/c3ref/soft_heap_limit64.html
func SetSoftHeapLimit(n int64) int64 {
	return int64(C.sqlite3_soft_heap_limit64(C.sqlite3_int64(n)))
}

// Calls http://sqlite.org/c3ref/complete.html
func Complete(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.sqlite3_complete(cs) != 0
}

// Calls http://sqlite.org/c3ref/log.html
func Log(err /*Errno*/ int, msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.my_log(C.int(err), cs)
}
