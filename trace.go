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
*/
import "C"

import (
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

func (c *Conn) Trace(f SqliteTrace, arg interface{}) {
	pArg := unsafe.Pointer(&sqliteTrace{f, arg})
	C.goSqlite3Trace(c.db, pArg)
}
