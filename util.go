// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>

// cgo doesn't support varargs
static char *my_mprintf(char *zFormat, char *arg) {
	return sqlite3_mprintf(zFormat, arg);
}
static char *my_mprintf2(char *zFormat, char *arg1, char *arg2) {
	return sqlite3_mprintf(zFormat, arg1, arg2);
}
*/
import "C"

import (
	"reflect"
	"unsafe"
)

// Mprintf is like fmt.Printf but implements some additional formatting options
// that are useful for constructing SQL statements.
// (See http://sqlite.org/c3ref/mprintf.html)
func Mprintf(format string, arg string) string {
	zSQL := mPrintf(format, arg)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}
func mPrintf(format, arg string) *C.char {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca := C.CString(arg)
	defer C.free(unsafe.Pointer(ca))
	return C.my_mprintf(cf, ca)
}

// Mprintf2 is like fmt.Printf but implements some additional formatting options
// that are useful for constructing SQL statements.
// (See http://sqlite.org/c3ref/mprintf.html)
func Mprintf2(format string, arg1, arg2 string) string {
	cf := C.CString(format)
	defer C.free(unsafe.Pointer(cf))
	ca1 := C.CString(arg1)
	defer C.free(unsafe.Pointer(ca1))
	ca2 := C.CString(arg2)
	defer C.free(unsafe.Pointer(ca2))
	zSQL := C.my_mprintf2(cf, ca1, ca2)
	defer C.sqlite3_free(unsafe.Pointer(zSQL))
	return C.GoString(zSQL)
}

// Must is a helper that wraps a call to a function returning (bool, os.Error)
// and panics if the error is non-nil.
func Must(b bool, err error) bool {
	if err != nil {
		panic(err)
	}
	return b
}

func btocint(b bool) C.int {
	if b {
		return 1
	}
	return 0
}
func cstring(s string) (*C.char, C.int) {
	cs := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	return (*C.char)(unsafe.Pointer(cs.Data)), C.int(cs.Len)
}

/*
func gostring(cs *C.char) string {
	var x reflect.StringHeader
	x.Data = uintptr(unsafe.Pointer(cs))
	x.Len = int(C.strlen(cs))
	return *(*string)(unsafe.Pointer(&x))
}
*/
