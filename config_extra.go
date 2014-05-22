// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all
// See SQLITE_OMIT_LOAD_EXTENSION (http://www.sqlite.org/compile.html)

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>
*/
import "C"

import (
	"unsafe"
)

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
