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

static void my_result_value(sqlite3_context *ctx, sqlite3_value **argv, int i) {
	sqlite3_result_value(ctx, argv[i]);
}

static const void *my_value_blob(sqlite3_value **argv, int i) {
	return sqlite3_value_blob(argv[i]);
}
static int my_value_bytes(sqlite3_value **argv, int i) {
	return sqlite3_value_bytes(argv[i]);
}
static double my_value_double(sqlite3_value **argv, int i) {
	return sqlite3_value_double(argv[i]);
}
static int my_value_int(sqlite3_value **argv, int i) {
	return sqlite3_value_int(argv[i]);
}
static sqlite3_int64 my_value_int64(sqlite3_value **argv, int i) {
	return sqlite3_value_int64(argv[i]);
}
static const unsigned char *my_value_text(sqlite3_value **argv, int i) {
	return sqlite3_value_text(argv[i]);
}
static int my_value_type(sqlite3_value **argv, int i) {
	return sqlite3_value_type(argv[i]);
}
static int my_value_numeric_type(sqlite3_value **argv, int i) {
	return sqlite3_value_numeric_type(argv[i]);
}

extern void goXAuxDataDestroy(void *ad);

static void goSqlite3SetAuxdata(sqlite3_context *ctx, int N, void *ad) {
	sqlite3_set_auxdata(ctx, N, ad, goXAuxDataDestroy);
}

extern void goXFunc(sqlite3_context *ctx, void *udf, void *goctx, int argc, sqlite3_value **argv);
extern void goXStep(sqlite3_context *ctx, void *udf, int argc, sqlite3_value **argv);
extern void goXFinal(sqlite3_context *ctx, void *udf);
extern void goXDestroy(void *pApp);

static void cXFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	void *udf = sqlite3_user_data(ctx);
	void *goctx = sqlite3_get_auxdata(ctx, 0);
	goXFunc(ctx, udf, goctx, argc, argv);
}

static void cXStep(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	void *udf = sqlite3_user_data(ctx);
	goXStep(ctx, udf, argc, argv);
}

static void cXFinal(sqlite3_context *ctx) {
	void *udf = sqlite3_user_data(ctx);
	goXFinal(ctx, udf);
}

static int goSqlite3CreateScalarFunction(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp) {
	return sqlite3_create_function_v2(db, zFunctionName, nArg, eTextRep, pApp, cXFunc, NULL, NULL, goXDestroy);
}
static int goSqlite3CreateAggregateFunction(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp) {
	return sqlite3_create_function_v2(db, zFunctionName, nArg, eTextRep, pApp, NULL, cXStep, cXFinal, goXDestroy);
}
*/
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"
)

/*
Database Connection For Functions
http://sqlite.org/c3ref/context_db_handle.html

sqlite3 *sqlite3_context_db_handle(sqlite3_context*);
*/

type Context struct {
	sc               *C.sqlite3_context
	argv             **C.sqlite3_value
	ad               map[int]interface{} // Function Auxiliary Data
	AggregateContext interface{}         // Aggregate Function Context
}

func (c *Context) Result(r interface{}) {
	switch r := r.(type) {
	case nil:
		c.ResultNull()
	case string:
		c.ResultText(r)
	case int:
		c.ResultInt(r)
	case int64:
		c.ResultInt64(r)
	case byte:
		c.ResultInt(int(r))
	case bool:
		c.ResultBool(r)
	case float32:
		c.ResultDouble(float64(r))
	case float64:
		c.ResultDouble(r)
	case []byte:
		c.ResultBlob(r)
	case ZeroBlobLength:
		c.ResultZeroblob(r)
	case error:
		c.ResultError(r.Error())
	case Errno:
		c.ResultErrorCode(r)
	default:
		panic(fmt.Sprintf("unsupported type in Result: %s", reflect.TypeOf(r)))
	}
}

// Set the result of an SQL function
func (c *Context) ResultBool(b bool) {
	if b {
		c.ResultInt(1)
	} else {
		c.ResultInt(0)
	}
}

// Set the result of an SQL function
// Calls sqlite3_result_blob, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultBlob(b []byte) {
	var p *byte
	if len(b) > 0 {
		p = &b[0]
	}
	C.my_result_blob(c.sc, unsafe.Pointer(p), C.int(len(b)))
}

// Set the result of an SQL function
// Calls sqlite3_result_double, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultDouble(d float64) {
	C.sqlite3_result_double(c.sc, C.double(d))
}

// Set the result of an SQL function
// Calls sqlite3_result_error, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultError(msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.sqlite3_result_error(c.sc, cs, -1)
}

// Set the result of an SQL function
// Calls sqlite3_result_error_toobig, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultErrorTooBig() {
	C.sqlite3_result_error_toobig(c.sc)
}

// Set the result of an SQL function
// Calls sqlite3_result_error_nomem, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultErrorNoMem() {
	C.sqlite3_result_error_nomem(c.sc)
}

// Set the result of an SQL function
// Calls sqlite3_result_error_code, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultErrorCode(e Errno) {
	C.sqlite3_result_error_code(c.sc, C.int(e))
}

// Set the result of an SQL function
// Calls sqlite3_result_int, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultInt(i int) {
	C.sqlite3_result_int(c.sc, C.int(i))
}

// Set the result of an SQL function
// Calls sqlite3_result_int64, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultInt64(i int64) {
	C.sqlite3_result_int64(c.sc, C.sqlite3_int64(i))
}

// Set the result of an SQL function
// Calls sqlite3_result_null, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultNull() {
	C.sqlite3_result_null(c.sc)
}

// Set the result of an SQL function
// Calls sqlite3_result_text, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultText(s string) {
	cs := C.CString(s)
	defer C.free(unsafe.Pointer(cs))
	C.my_result_text(c.sc, cs, -1)
}

// Set the result of an SQL function
// The leftmost value is number 0.
// Calls sqlite3_result_value, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultValue(i int) {
	C.my_result_value(c.sc, c.argv, C.int(i))
}

// Set the result of an SQL function
// Calls sqlite3_result_zeroblob, http://sqlite.org/c3ref/result_blob.html
func (c *Context) ResultZeroblob(n ZeroBlobLength) {
	C.sqlite3_result_zeroblob(c.sc, C.int(n))
}

// User data for functions
// Calls http://sqlite.org/c3ref/user_data.html
func (c *Context) UserData() interface{} {
	udf := (*sqliteFunction)(C.sqlite3_user_data(c.sc))
	return udf.pApp
}

// Function auxiliary data
// Calls sqlite3_get_auxdata, http://sqlite.org/c3ref/get_auxdata.html
func (c *Context) GetAuxData(n int) interface{} {
	if len(c.ad) == 0 {
		return nil
	}
	return c.ad[n]
}

// Function auxiliary data
// No destructor is needed a priori
// Calls sqlite3_set_auxdata, http://sqlite.org/c3ref/get_auxdata.html
func (c *Context) SetAuxData(n int, ad interface{}) {
	if len(c.ad) == 0 {
		c.ad = make(map[int]interface{})
	}
	c.ad[n] = ad
}

// The leftmost value is number 0.
func (c *Context) Bool(i int) bool {
	return c.Int(i) == 1
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

func (c *Context) Value(i int) (value interface{}) {
	switch c.Type(i) {
	case Null:
		value = nil
	case Text:
		value = c.Text(i)
	case Integer:
		value = c.Int64(i)
	case Float:
		value = c.Double(i)
	case Blob:
		value = c.Blob(i)
	default:
		panic("The value type is not one of SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB, or SQLITE_NULL")
	}
	return
}

type ScalarFunction func(ctx *Context, nArg int)
type FinalFunction func(ctx *Context)
type DestroyFunctionData func(pApp interface{})

/*
  void (*xStep)(sqlite3_context*,int,sqlite3_value**),
*/

type sqliteFunction struct {
	funcOrStep ScalarFunction
	final      FinalFunction
	d          DestroyFunctionData
	pApp       interface{}
}

// To prevent Context from being gced
// TODO Retry to put this in the sqliteFunction
var contexts map[*C.sqlite3_context]*Context = make(map[*C.sqlite3_context]*Context)

//export goXAuxDataDestroy
func goXAuxDataDestroy(ad unsafe.Pointer) {
	c := (*Context)(ad)
	if c != nil {
		delete(contexts, c.sc)
	}
	//fmt.Printf("%v\n", contexts)
}

//export goXFunc
func goXFunc(scp, udfp, ctxp unsafe.Pointer, argc int, argv unsafe.Pointer) {
	udf := (*sqliteFunction)(udfp)
	// To avoid the creation of a Context at each call, just put it in auxdata
	c := (*Context)(ctxp)
	if c == nil {
		c = new(Context)
		c.sc = (*C.sqlite3_context)(scp)
		C.goSqlite3SetAuxdata(c.sc, 0, unsafe.Pointer(c))
		// To make sure it is not cged
		contexts[c.sc] = c
	}
	c.argv = (**C.sqlite3_value)(argv)
	udf.funcOrStep(c, argc)
	c.argv = nil
}

//export goXStep
func goXStep(scp, udfp unsafe.Pointer, argc int, argv unsafe.Pointer) {
	//udf := (*sqliteFunction)(udfp)
	//c := nil // FIXME
}

//export goXFinal
func goXFinal(scp, udfp unsafe.Pointer) {
	//udf := (*sqliteFunction)(udfp)
	//c := nil // FIXME (*C.sqlite3_context)(scp)
	//udf.final(c)
}

//export goXDestroy
func goXDestroy(pApp unsafe.Pointer) {
	udf := (*sqliteFunction)(pApp)
	if udf.d != nil {
		udf.d(udf.pApp)
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
	udf := &sqliteFunction{f, nil, d, pApp}
	if len(c.udfs) == 0 {
		c.udfs = make(map[string]*sqliteFunction)
	}
	c.udfs[functionName] = udf // FIXME same function name with different args is not supported
	return c.error(C.goSqlite3CreateScalarFunction(c.db, fname, C.int(nArg), C.SQLITE_UTF8, unsafe.Pointer(udf)))
}

// Calls http://sqlite.org/c3ref/aggregate_context.html
func (c *Context) AggregateContext(nBytes int) interface{} {
	return C.sqlite3_aggregate_context(c.sc, C.int(nBytes))
}

// Create or redefine SQL functions
// TODO Make possible to specify the preferred encoding
// Calls http://sqlite.org/c3ref/create_function.html
func (c *Conn) CreateAggregateFunction(functionName string, nArg int, pApp interface{},
	step ScalarFunction, final FinalFunction, d DestroyFunctionData) error {
	fname := C.CString(functionName)
	defer C.free(unsafe.Pointer(fname))
	if step == nil {
		if len(c.udfs) > 0 {
			delete(c.udfs, functionName)
		}
		return c.error(C.sqlite3_create_function_v2(c.db, fname, C.int(nArg), C.SQLITE_UTF8, nil, nil, nil, nil, nil))
	}
	// To make sure it is not gced, keep a reference in the connection.
	udf := &sqliteFunction{step, final, d, pApp}
	if len(c.udfs) == 0 {
		c.udfs = make(map[string]*sqliteFunction)
	}
	c.udfs[functionName] = udf // FIXME same function name with different args is not supported
	return c.error(C.goSqlite3CreateAggregateFunction(c.db, fname, C.int(nArg), C.SQLITE_UTF8, unsafe.Pointer(udf)))
}
