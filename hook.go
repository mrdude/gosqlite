// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#include <sqlite3.h>

void* goSqlite3CommitHook(sqlite3 *db, void *udp);
void* goSqlite3RollbackHook(sqlite3 *db, void *udp);
void* goSqlite3UpdateHook(sqlite3 *db, void *udp);
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
// (See http://sqlite.org/c3ref/commit_hook.html)
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
// (See http://sqlite.org/c3ref/commit_hook.html)
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
func goXUpdateHook(udp unsafe.Pointer, action int, dbName, tableName *C.char, rowId C.sqlite3_int64) {
	arg := (*sqliteUpdateHook)(udp)
	arg.f(arg.udp, Action(action), C.GoString(dbName), C.GoString(tableName), int64(rowId))
}

// Data change notification callbacks
// (See http://sqlite.org/c3ref/update_hook.html)
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
