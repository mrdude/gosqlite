// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

extern void goXTrace(void *pArg, const char *t);

static void goSqlite3Trace(sqlite3 *db, void *pArg) {
	sqlite3_trace(db, goXTrace, pArg);
}
extern void goXProfile(void *pArg, const char *sql, sqlite3_uint64 nanoseconds);

static void goSqlite3Profile(sqlite3 *db, void *pArg) {
	sqlite3_profile(db, goXProfile, pArg);
}

extern int goXAuth(void *pUserData, int action, const char *arg1, const char *arg2, const char *arg3, const char *arg4);

static int goSqlite3SetAuthorizer(sqlite3 *db, void *pUserData) {
	return sqlite3_set_authorizer(db, goXAuth, pUserData);
}

extern int goXBusy(void *pArg, int n);

static int goSqlite3BusyHandler(sqlite3 *db, void *pArg) {
	return sqlite3_busy_handler(db, goXBusy, pArg);
}
*/
import "C"

import (
	"os"
	"unsafe"
)

type Tracer func(d interface{}, t string)

type sqliteTrace struct {
	f Tracer
	d interface{}
}

//export goXTrace
func goXTrace(pArg unsafe.Pointer, t *C.char) {
	arg := (*sqliteTrace)(pArg)
	arg.f(arg.d, C.GoString(t))
}

// Calls sqlite3_trace, http://sqlite.org/c3ref/profile.html
func (c *Conn) Trace(f Tracer, arg interface{}) {
	if f == nil {
		c.trace = nil
		C.sqlite3_trace(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.trace = &sqliteTrace{f, arg}
	C.goSqlite3Trace(c.db, unsafe.Pointer(c.trace))
}

type Profiler func(d interface{}, sql string, nanoseconds uint64)

type sqliteProfile struct {
	f Profiler
	d interface{}
}

//export goXProfile
func goXProfile(pArg unsafe.Pointer, sql *C.char, nanoseconds C.sqlite3_uint64) {
	arg := (*sqliteProfile)(pArg)
	arg.f(arg.d, C.GoString(sql), uint64(nanoseconds))
}

// Calls sqlite3_profile, http://sqlite.org/c3ref/profile.html
func (c *Conn) Profile(f Profiler, arg interface{}) {
	if f == nil {
		c.profile = nil
		C.sqlite3_profile(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.profile = &sqliteProfile{f, arg}
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

type Authorizer func(d interface{}, action Action, arg1, arg2, arg3, arg4 string) Auth

type sqliteAuthorizer struct {
	f Authorizer
	d interface{}
}

//export goXAuth
func goXAuth(pUserData unsafe.Pointer, action C.int, arg1, arg2, arg3, arg4 *C.char) C.int {
	arg := (*sqliteAuthorizer)(pUserData)
	result := arg.f(arg.d, Action(action), C.GoString(arg1), C.GoString(arg2), C.GoString(arg3), C.GoString(arg4))
	return C.int(result)
}

// Calls http://sqlite.org/c3ref/set_authorizer.html
func (c *Conn) SetAuthorizer(f Authorizer, arg interface{}) os.Error {
	if f == nil {
		c.authorizer = nil
		return c.error(C.sqlite3_set_authorizer(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.authorizer = &sqliteAuthorizer{f, arg}
	return c.error(C.goSqlite3SetAuthorizer(c.db, unsafe.Pointer(c.authorizer)))
}

type BusyHandler func(d interface{}, n int) int

type sqliteBusyHandler struct {
	f BusyHandler
	d interface{}
}

//export goXBusy
func goXBusy(pArg unsafe.Pointer, n C.int) C.int {
	arg := (*sqliteBusyHandler)(pArg)
	result := arg.f(arg.d, int(n))
	return C.int(result)
}

// TODO NOT TESTED
// Calls http://sqlite.org/c3ref/busy_handler.html
func (c *Conn) BusyHandler(f BusyHandler, arg interface{}) os.Error {
	if f == nil {
		c.busyHandler = nil
		return c.error(C.sqlite3_busy_handler(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.busyHandler = &sqliteBusyHandler{f, arg}
	return c.error(C.goSqlite3BusyHandler(c.db, unsafe.Pointer(c.busyHandler)))
}
