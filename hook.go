// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

extern int goXCommitHook(void *udp);

static void* goSqlite3CommitHook(sqlite3 *db, void *udp) {
	return sqlite3_commit_hook(db, goXCommitHook, udp);
}

extern void goXRollbackHook(void *udp);

static void* goSqlite3RollbackHook(sqlite3 *db, void *udp) {
	return sqlite3_rollback_hook(db, goXRollbackHook, udp);
}

extern void goXUpdateHook(void *udp, int action, char const *dbName, char const *tableName, sqlite3_int64 rowId);

static void* goSqlite3UpdateHook(sqlite3 *db, void *udp) {
	return sqlite3_update_hook(db, goXUpdateHook, udp);
}
*/
import "C"

import (
	"unsafe"
)

type CommitHook func(udp interface{}) int

type sqliteCommitHook struct {
	f   CommitHook
	udp interface{}
}

//export goXCommitHook
func goXCommitHook(udp unsafe.Pointer) C.int {
	arg := (*sqliteCommitHook)(udp)
	return C.int(arg.f(arg.udp))
}

// Commit notification callback
// Calls http://sqlite.org/c3ref/commit_hook.html
func (c *Conn) CommitHook(f CommitHook, udp interface{}) {
	if f == nil {
		c.commitHook = nil
		C.sqlite3_commit_hook(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.commitHook = &sqliteCommitHook{f, udp}
	C.goSqlite3CommitHook(c.db, unsafe.Pointer(c.commitHook))
}

type RollbackHook func(udp interface{})

type sqliteRollbackHook struct {
	f   RollbackHook
	udp interface{}
}

//export goXRollbackHook
func goXRollbackHook(udp unsafe.Pointer) {
	arg := (*sqliteRollbackHook)(udp)
	arg.f(arg.udp)
}

// Rollback notification callback
// Calls http://sqlite.org/c3ref/commit_hook.html
func (c *Conn) RollbackHook(f RollbackHook, udp interface{}) {
	if f == nil {
		c.rollbackHook = nil
		C.sqlite3_rollback_hook(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.rollbackHook = &sqliteRollbackHook{f, udp}
	C.goSqlite3RollbackHook(c.db, unsafe.Pointer(c.rollbackHook))
}

type UpdateHook func(udp interface{}, a Action, dbName, tableName string, rowId int64)

type sqliteUpdateHook struct {
	f   UpdateHook
	udp interface{}
}

//export goXUpdateHook
func goXUpdateHook(udp unsafe.Pointer, action C.int, dbName, tableName *C.char, rowId C.sqlite3_int64) {
	arg := (*sqliteUpdateHook)(udp)
	arg.f(arg.udp, Action(action), C.GoString(dbName), C.GoString(tableName), int64(rowId))
}

// Data change notification callbacks
// Calls http://sqlite.org/c3ref/update_hook.html
func (c *Conn) UpdateHook(f UpdateHook, udp interface{}) {
	if f == nil {
		c.updateHook = nil
		C.sqlite3_update_hook(c.db, nil, nil)
		return
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.updateHook = &sqliteUpdateHook{f, udp}
	C.goSqlite3UpdateHook(c.db, unsafe.Pointer(c.updateHook))
}
