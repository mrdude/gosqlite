// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#include <sqlite3.h>
#include <stdlib.h>
// These wrappers are necessary because SQLITE_TRANSIENT
// is a pointer constant, and cgo doesn't translate them correctly.

static void my_result_text(sqlite3_context *ctx, char *p, int np) {
	sqlite3_result_text(ctx, p, np, SQLITE_TRANSIENT);
}
static void my_result_blob(sqlite3_context *ctx, void *p, int np) {
	sqlite3_result_blob(ctx, p, np, SQLITE_TRANSIENT);
}

static void my_result_value(sqlite3_context* ctx, sqlite3_value** argv, int i) {
	sqlite3_result_value(ctx, argv[i]);
}

static const void *my_value_blob(sqlite3_value** argv, int i) {
	return sqlite3_value_blob(argv[i]);
}
static int my_value_bytes(sqlite3_value** argv, int i) {
	return sqlite3_value_bytes(argv[i]);
}
static double my_value_double(sqlite3_value** argv, int i) {
	return sqlite3_value_double(argv[i]);
}
static int my_value_int(sqlite3_value** argv, int i) {
	return sqlite3_value_int(argv[i]);
}
static sqlite3_int64 my_value_int64(sqlite3_value** argv, int i) {
	return sqlite3_value_int64(argv[i]);
}
static const unsigned char *my_value_text(sqlite3_value** argv, int i) {
	return sqlite3_value_text(argv[i]);
}
static int my_value_type(sqlite3_value** argv, int i) {
	return sqlite3_value_type(argv[i]);
}
static int my_value_numeric_type(sqlite3_value** argv, int i) {
	return sqlite3_value_numeric_type(argv[i]);
}

extern void goXFunc(sqlite3_context* ctx, int argc, sqlite3_value** argv);
extern void goXDestroy(void *pApp);

static int goSqlite3CreateFunctionV2(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp) {
	return sqlite3_create_function_v2(db, zFunctionName, nArg, eTextRep, pApp, goXFunc, NULL, NULL, goXDestroy);
}
*/
import "C"

import (
	"unsafe"
)

/*
Database Connection For Functions
http://sqlite.org/c3ref/context_db_handle.html

sqlite3 *sqlite3_context_db_handle(sqlite3_context*);
*/

type Context struct {
	context *C.sqlite3_context
	argv    **C.sqlite3_value
}

// Set the result of an SQL function
// Calls sqlite3_result_blob, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultBlob(b []byte) {
	var p *byte
	if len(b) > 0 {
		p = &b[0]
	}
	C.my_result_blob(c.context, unsafe.Pointer(p), C.int(len(b)))
}

// Set the result of an SQL function
// Calls sqlite3_result_double, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultDouble(d float64) {
	C.sqlite3_result_double(c.context, C.double(d))
}

// Set the result of an SQL function
// Calls sqlite3_result_error, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultError(msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.sqlite3_result_error(c.context, cs, -1)
}

// Set the result of an SQL function
// Calls sqlite3_result_error_toobig, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultErrorTooBig() {
	C.sqlite3_result_error_toobig(c.context)
}

// Set the result of an SQL function
// Calls sqlite3_result_error_nomem, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultErrorNoMem() {
	C.sqlite3_result_error_nomem(c.context)
}

// Set the result of an SQL function
// Calls sqlite3_result_error_code, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultErrorCode(e Errno) {
	C.sqlite3_result_error_code(c.context, C.int(e))
}

// Set the result of an SQL function
// Calls sqlite3_result_int, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultInt(i int) {
	C.sqlite3_result_int(c.context, C.int(i))
}

// Set the result of an SQL function
// Calls sqlite3_result_int64, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultInt64(i int64) {
	C.sqlite3_result_int64(c.context, C.sqlite3_int64(i))
}

// Set the result of an SQL function
// Calls sqlite3_result_null, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultNull() {
	C.sqlite3_result_null(c.context)
}

// Set the result of an SQL function
// Calls sqlite3_result_text, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultText(s string) {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	C.my_result_text(c.context, cs, -1)
}

// Set the result of an SQL function
// The leftmost value is number 0.
// Calls sqlite3_result_value, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultValue(i int) {
	C.my_result_value(c.context, c.argv, C.int(i))
}

// Set the result of an SQL function
// Calls sqlite3_result_zeroblob, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultZeroblob(n ZeroBlobLength) {
	C.sqlite3_result_zeroblob(c.context, C.int(n))
}

// User data for functions
// Calls http://sqlite.org/c3ref/user_data.html
func (c *Context) UserData() interface{} {
	udp := (*sqliteScalarFunction)(C.sqlite3_user_data(c.context))
	return udp.pApp
}

// Function auxiliary data
// Calls sqlite3_get_auxdata, http://sqlite.org/c3ref/get_auxdata.html
func (c *Context) GetAuxData(n int) interface{} {
	return C.sqlite3_get_auxdata(c.context, C.int(n))
}

type AuxDataDestructor func(ad interface{})

// Function auxiliary data
// Calls sqlite3_set_auxdata, http://sqlite.org/c3ref/get_auxdata.html
func (c *Context) SetAuxData(n int, ad interface{}, f AuxDataDestructor) {
	// FIXME C.sqlite3_set_auxdata(c.context, C.int(n), unsafe.Pointer(ad), nil /*void (*)(void*)*/ )
}

// The leftmost value is number 0.
// Calls sqlite3_value_blob and sqlite3_value_bytes, http://sqlite.org/c3ref/value_blob.html
func (c *Context) Blob(i int) (value []byte) {
	p := C.my_value_blob(c.argv, C.int(i))
	if p != nil {
		n := C.my_value_bytes(c.argv, C.int(i))
		value = (*[1 << 30]byte)(unsafe.Pointer(p))[0:n]
	}
	return
}

// The leftmost value is number 0.
// Calls sqlite3_value_double, http://sqlite.org/c3ref/value_blob.html
func (c *Context) Double(i int) float64 {
	return float64(C.my_value_double(c.argv, C.int(i)))
}

// The leftmost value is number 0.
// Calls sqlite3_value_int, http://sqlite.org/c3ref/value_blob.html
func (c *Context) Int(i int) int {
	return int(C.my_value_int(c.argv, C.int(i)))
}

// The leftmost value is number 0.
// Calls sqlite3_value_int64, http://sqlite.org/c3ref/value_blob.html
func (c *Context) Int64(i int) int64 {
	return int64(C.my_value_int64(c.argv, C.int(i)))
}

// The leftmost value is number 0.
// Calls sqlite3_value_text, http://sqlite.org/c3ref/value_blob.html
func (c *Context) Text(i int) string {
	p := C.my_value_text(c.argv, C.int(i))
	if p == nil {
		return ""
	}
	n := C.my_value_bytes(c.argv, C.int(i))
	return C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
}

// The leftmost value is number 0.
// SQL function parameter value type
// Calls sqlite3_value_type, http://sqlite.org/c3ref/value_blob.html
func (c *Context) Type(i int) Type {
	return Type(C.my_value_type(c.argv, C.int(i)))
}

// The leftmost value is number 0.
// SQL function parameter value numeric type (with possible conversion)
// Calls sqlite3_value_numeric_type, http://sqlite.org/c3ref/value_blob.html
func (c *Context) NumericType(i int) Type {
	return Type(C.my_value_numeric_type(c.argv, C.int(i)))
}

type ScalarFunction func(ctx *Context, nArg int)
type DestroyFunctionData func(pApp interface{})

type sqliteScalarFunction struct {
	f    ScalarFunction
	d    DestroyFunctionData
	pApp interface{}
}

//export goXFunc
func goXFunc(ctxp unsafe.Pointer, argc int, argv unsafe.Pointer) {
	ctx := (*C.sqlite3_context)(ctxp)
	udp := (*sqliteScalarFunction)(C.sqlite3_user_data(ctx))
	// TODO How to avoid to create a Context at each call?
	context := &Context{ctx, (**C.sqlite3_value)(argv)}
	udp.f(context, argc)
}

//export goXDestroy
func goXDestroy(pApp unsafe.Pointer) {
	arg := (*sqliteScalarFunction)(pApp)
	if arg.d != nil {
		arg.d(arg.pApp)
	}
}

// Create or redefine SQL functions
// TODO Make possible to specify the preferred encoding
// Calls http://sqlite.org/c3ref/create_function.html
func (c *Conn) CreateScalarFunction(functionName string, nArg int, pApp interface{}, f ScalarFunction, d DestroyFunctionData) error {
	fname := C.CString(functionName)
	defer C.free(unsafe.Pointer(fname))
	if f == nil {
		if len(c.udfs) > 0 {
			delete(c.udfs, functionName)
		}
		return c.error(C.sqlite3_create_function_v2(c.db, fname, C.int(nArg), C.SQLITE_UTF8, nil, nil, nil, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	xFunc := &sqliteScalarFunction{f, d, pApp}
	if len(c.udfs) == 0 {
		c.udfs = make(map[string]*sqliteScalarFunction)
	}
	c.udfs[functionName] = xFunc // FIXME same function name with different args is not supported
	return c.error(C.goSqlite3CreateFunctionV2(c.db, fname, C.int(nArg), C.SQLITE_UTF8, unsafe.Pointer(xFunc)))
}
