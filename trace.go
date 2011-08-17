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
	"fmt"
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

// Calls sqlite3_trace
func (c *Conn) Trace(f SqliteTrace, arg interface{}) {
	if f == nil {
		C.sqlite3_trace(c.db, nil, nil)
		return
	}
	pArg := unsafe.Pointer(&sqliteTrace{f, arg})
	C.goSqlite3Trace(c.db, pArg)
}

type SqliteAuthorizer func(d interface{}, action int, arg1, arg2, arg3, arg4 string) int

type sqliteAuthorizer struct {
	f SqliteAuthorizer
	d interface{}
}

//export goXAuth
func goXAuth(pUserData unsafe.Pointer, action C.int, arg1, arg2, arg3, arg4 *C.char) C.int {
	var result int
	if pUserData != nil {
		arg := (*sqliteAuthorizer)(pUserData)
		result = arg.f(arg.d, int(action), C.GoString(arg1), C.GoString(arg2), C.GoString(arg3), C.GoString(arg4))
	} else {
		fmt.Printf("ERROR - %v\n", pUserData)
		result = 0
	}
	return C.int(result)
}

// Calls sqlite3_set_authorizer
func (c *Conn) SetAuthorizer(f SqliteAuthorizer, arg interface{}) int {
	// c.authorizer = f
	if f == nil {
		return int(C.sqlite3_set_authorizer(c.db, nil, nil))
	}
	pArg := unsafe.Pointer(&sqliteAuthorizer{f, arg})
	return int(C.goSqlite3SetAuthorizer(c.db, pArg))
}
