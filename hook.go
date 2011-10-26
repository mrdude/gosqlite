// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqlite provides access to the SQLite library, version 3.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

extern void goXUpdateHook(void *pArg, int action, char const *db, char const *table, sqlite3_int64 rowId);

static void goSqlite3UpdateHook(sqlite3 *db, void *pArg) {
	sqlite3_update_hook(db, goXUpdateHook, pArg);
}
*/
import "C"

import (
	"unsafe"
)

type UpdateHook func(d interface{}, a Action, db, table string, rowId int64)

type sqliteUpdateHook struct {
	f UpdateHook
	d interface{}
}

//export goXUpdateHook
func goXUpdateHook(pArg unsafe.Pointer, action C.int, db, table *C.char, rowId C.sqlite3_int64) {
	arg := (*sqliteUpdateHook)(pArg)
	arg.f(arg.d, Action(action), C.GoString(db), C.GoString(table), int64(rowId))
}

// Calls http://sqlite.org/c3ref/update_hook.html
func (c *Conn) UpdateHook(f UpdateHook, arg interface{}) {
	if f == nil {
		c.updateHook = nil
		C.sqlite3_update_hook(c.db, nil, nil)
	}
	// To make sure it is not gced, keep a reference in the connection.
	c.updateHook = &sqliteUpdateHook{f, arg}
	C.goSqlite3UpdateHook(c.db, unsafe.Pointer(c.updateHook))
}
