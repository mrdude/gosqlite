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

extern int goXAuth(void *pUserData, int action, const char *arg1, const char *arg2, const char *arg3, const char *arg4);

static int goSqlite3SetAuthorizer(sqlite3 *db, void *pUserData) {
	return sqlite3_set_authorizer(db, goXAuth, pUserData);
}
*/
import "C"

import (
	"os"
	"unsafe"
)

type SqliteTrace func(d interface{}, t string)

type sqliteTrace struct {
	f SqliteTrace
	d interface{}
}

//export goXTrace
func goXTrace(pArg unsafe.Pointer, t *C.char) {
	arg := (*sqliteTrace)(pArg)
	arg.f(arg.d, C.GoString(t))
}

// Calls sqlite3_trace, http://sqlite.org/c3ref/profile.html
func (c *Conn) Trace(f SqliteTrace, arg interface{}) {
	if f == nil {
		C.sqlite3_trace(c.db, nil, nil)
		return
	}
	pArg := unsafe.Pointer(&sqliteTrace{f, arg})
	C.goSqlite3Trace(c.db, pArg)
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

type SqliteAuthorizer func(d interface{}, action Action, arg1, arg2, arg3, arg4 string) Auth

type sqliteAuthorizer struct {
	f SqliteAuthorizer
	d interface{}
}

//export goXAuth
func goXAuth(pUserData unsafe.Pointer, action C.int, arg1, arg2, arg3, arg4 *C.char) C.int {
	arg := (*sqliteAuthorizer)(pUserData)
	result := arg.f(arg.d, Action(action), C.GoString(arg1), C.GoString(arg2), C.GoString(arg3), C.GoString(arg4))
	return C.int(result)
}

// Calls http://sqlite.org/c3ref/set_authorizer.html
func (c *Conn) SetAuthorizer(f SqliteAuthorizer, arg interface{}) os.Error {
	if f == nil {
		c.authorizer = nil
		return c.error(C.sqlite3_set_authorizer(c.db, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.authorizer = &sqliteAuthorizer{f, arg}
	return c.error(C.goSqlite3SetAuthorizer(c.db, unsafe.Pointer(c.authorizer)))
}
