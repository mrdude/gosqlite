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

void goSqlite3SetAuxdata(sqlite3_context *ctx, int N, void *ad);
int goSqlite3CreateScalarFunction(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp);
int goSqlite3CreateAggregateFunction(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp);
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

// Context common to scalar and aggregate function
// (See http://sqlite.org/c3ref/context.html)
type Context struct {
	sc *C.sqlite3_context
}

type FunctionContext struct {
	Context
	argv **C.sqlite3_value
}

// Context associated to scalar function
type ScalarContext struct {
	FunctionContext
	ad  map[int]interface{} // Function Auxiliary Data
	udf *sqliteFunction
}

// Context associated to aggregate function
type AggregateContext struct {
	FunctionContext
	Aggregate interface{}
}

// Result sets the result of an SQL function.
func (c *FunctionContext) Result(r interface{}) {
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

// ResultBool sets the result of an SQL function.
func (c *Context) ResultBool(b bool) {
	if b {
		c.ResultInt(1)
	} else {
		c.ResultInt(0)
	}
}

// ResultBlob sets the result of an SQL function.
// (See sqlite3_result_blob, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultBlob(b []byte) {
	var p *byte
	if len(b) > 0 {
		p = &b[0]
	}
	C.my_result_blob(c.sc, unsafe.Pointer(p), C.int(len(b)))
}

// ResultDouble sets the result of an SQL function.
// (See sqlite3_result_double, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultDouble(d float64) {
	C.sqlite3_result_double(c.sc, C.double(d))
}

// ResultError sets the result of an SQL function.
// (See sqlite3_result_error, http://sqlite.org/c3ref/result_blob.html)
func (c *FunctionContext) ResultError(msg string) {
	cs := C.CString(msg)
	defer C.free(unsafe.Pointer(cs))
	C.sqlite3_result_error(c.sc, cs, -1)
}

// ResultErrorTooBig sets the result of an SQL function.
// (See sqlite3_result_error_toobig, http://sqlite.org/c3ref/result_blob.html)
func (c *FunctionContext) ResultErrorTooBig() {
	C.sqlite3_result_error_toobig(c.sc)
}

// ResultErrorNoMem sets the result of an SQL function.
// (See sqlite3_result_error_nomem, http://sqlite.org/c3ref/result_blob.html)
func (c *FunctionContext) ResultErrorNoMem() {
	C.sqlite3_result_error_nomem(c.sc)
}

// ResultErrorCode sets the result of an SQL function.
// (See sqlite3_result_error_code, http://sqlite.org/c3ref/result_blob.html)
func (c *FunctionContext) ResultErrorCode(e Errno) {
	C.sqlite3_result_error_code(c.sc, C.int(e))
}

// ResultInt sets the result of an SQL function.
// (See sqlite3_result_int, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultInt(i int) {
	C.sqlite3_result_int(c.sc, C.int(i))
}

// ResultInt64 sets the result of an SQL function.
// (See sqlite3_result_int64, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultInt64(i int64) {
	C.sqlite3_result_int64(c.sc, C.sqlite3_int64(i))
}

// ResultNull sets the result of an SQL function.
// (See sqlite3_result_null, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultNull() {
	C.sqlite3_result_null(c.sc)
}

// ResultText sets the result of an SQL function.
// (See sqlite3_result_text, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultText(s string) {
	cs, l := cstring(s)
	C.my_result_text(c.sc, cs, l)
}

// ResultValue sets the result of an SQL function.
// The leftmost value is number 0.
// (See sqlite3_result_value, http://sqlite.org/c3ref/result_blob.html)
func (c *FunctionContext) ResultValue(i int) {
	C.my_result_value(c.sc, c.argv, C.int(i))
}

// ResultZeroblob sets the result of an SQL function.
// (See sqlite3_result_zeroblob, http://sqlite.org/c3ref/result_blob.html)
func (c *Context) ResultZeroblob(n ZeroBlobLength) {
	C.sqlite3_result_zeroblob(c.sc, C.int(n))
}

// UserData returns the user data for functions.
// (See http://sqlite.org/c3ref/user_data.html)
func (c *FunctionContext) UserData() interface{} {
	udf := (*sqliteFunction)(C.sqlite3_user_data(c.sc))
	return udf.pApp
}

// GetAuxData returns function auxiliary data.
// (See sqlite3_get_auxdata, http://sqlite.org/c3ref/get_auxdata.html)
func (c *ScalarContext) GetAuxData(n int) interface{} {
	if len(c.ad) == 0 {
		return nil
	}
	return c.ad[n]
}

// SetAuxData sets function auxiliary data.
// No destructor is needed a priori
// (See sqlite3_set_auxdata, http://sqlite.org/c3ref/get_auxdata.html)
func (c *ScalarContext) SetAuxData(n int, ad interface{}) {
	if len(c.ad) == 0 {
		c.ad = make(map[int]interface{})
	}
	c.ad[n] = ad
}

// Bool obtains a SQL function parameter value.
// The leftmost value is number 0.
func (c *FunctionContext) Bool(i int) bool {
	return c.Int(i) == 1
}

// Blob obtains a SQL function parameter value.
// The leftmost value is number 0.
// (See sqlite3_value_blob and sqlite3_value_bytes, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) Blob(i int) (value []byte) {
	p := C.my_value_blob(c.argv, C.int(i))
	if p != nil {
		n := C.my_value_bytes(c.argv, C.int(i))
		// value = (*[1 << 30]byte)(unsafe.Pointer(p))[:n]
		value = C.GoBytes(p, n) // The memory space used to hold strings and BLOBs is freed automatically.
	}
	return
}

// Double obtains a SQL function parameter value.
// The leftmost value is number 0.
// (See sqlite3_value_double, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) Double(i int) float64 {
	return float64(C.my_value_double(c.argv, C.int(i)))
}

// Int obtains a SQL function parameter value.
// The leftmost value is number 0.
// (See sqlite3_value_int, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) Int(i int) int {
	return int(C.my_value_int(c.argv, C.int(i)))
}

// Int64 obtains a SQL function parameter value.
// The leftmost value is number 0.
// (See sqlite3_value_int64, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) Int64(i int) int64 {
	return int64(C.my_value_int64(c.argv, C.int(i)))
}

// Text obtains a SQL function parameter value.
// The leftmost value is number 0.
// (See sqlite3_value_text, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) Text(i int) string {
	p := C.my_value_text(c.argv, C.int(i))
	if p == nil {
		return ""
	}
	n := C.my_value_bytes(c.argv, C.int(i))
	return C.GoStringN((*C.char)(unsafe.Pointer(p)), n)
}

// Type obtains a SQL function parameter value type.
// The leftmost value is number 0.
// (See sqlite3_value_type, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) Type(i int) Type {
	return Type(C.my_value_type(c.argv, C.int(i)))
}

// NumericType obtains a SQL function parameter value numeric type (with possible conversion).
// The leftmost value is number 0.
// (See sqlite3_value_numeric_type, http://sqlite.org/c3ref/value_blob.html)
func (c *FunctionContext) NumericType(i int) Type {
	return Type(C.my_value_numeric_type(c.argv, C.int(i)))
}

// Value obtains a SQL function parameter value depending on its type.
func (c *FunctionContext) Value(i int) (value interface{}) {
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

type ScalarFunction func(ctx *ScalarContext, nArg int)
type StepFunction func(ctx *AggregateContext, nArg int)
type FinalFunction func(ctx *AggregateContext)
type DestroyFunctionData func(pApp interface{})

type sqliteFunction struct {
	scalar     ScalarFunction
	step       StepFunction
	final      FinalFunction
	d          DestroyFunctionData
	pApp       interface{}
	scalarCtxs map[*ScalarContext]bool
	aggrCtxs   map[*AggregateContext]bool
}

//export goXAuxDataDestroy
func goXAuxDataDestroy(ad unsafe.Pointer) {
	c := (*ScalarContext)(ad)
	if c != nil {
		delete(c.udf.scalarCtxs, c)
	}
	//	fmt.Printf("Contexts: %v\n", c.udf.scalarCtxs)
}

//export goXFunc
func goXFunc(scp, udfp, ctxp unsafe.Pointer, argc int, argv unsafe.Pointer) {
	udf := (*sqliteFunction)(udfp)
	// To avoid the creation of a Context at each call, just put it in auxdata
	c := (*ScalarContext)(ctxp)
	if c == nil {
		c = new(ScalarContext)
		c.sc = (*C.sqlite3_context)(scp)
		c.udf = udf
		C.goSqlite3SetAuxdata(c.sc, 0, unsafe.Pointer(c))
		// To make sure it is not cged
		udf.scalarCtxs[c] = true
	}
	c.argv = (**C.sqlite3_value)(argv)
	udf.scalar(c, argc)
	c.argv = nil
}

//export goXStep
func goXStep(scp, udfp unsafe.Pointer, argc int, argv unsafe.Pointer) {
	udf := (*sqliteFunction)(udfp)
	var cp unsafe.Pointer
	cp = C.sqlite3_aggregate_context((*C.sqlite3_context)(scp), C.int(unsafe.Sizeof(cp)))
	if cp != nil {
		var c *AggregateContext
		p := *(*unsafe.Pointer)(cp)
		if p == nil {
			c = new(AggregateContext)
			c.sc = (*C.sqlite3_context)(scp)
			*(*unsafe.Pointer)(cp) = unsafe.Pointer(c)
			// To make sure it is not cged
			udf.aggrCtxs[c] = true
		} else {
			c = (*AggregateContext)(p)
		}

		c.argv = (**C.sqlite3_value)(argv)
		udf.step(c, argc)
		c.argv = nil
	}
}

//export goXFinal
func goXFinal(scp, udfp unsafe.Pointer) {
	udf := (*sqliteFunction)(udfp)
	cp := C.sqlite3_aggregate_context((*C.sqlite3_context)(scp), 0)
	if cp != nil {
		p := *(*unsafe.Pointer)(cp)
		if p != nil {
			c := (*AggregateContext)(p)
			delete(udf.aggrCtxs, c)
			c.sc = (*C.sqlite3_context)(scp)
			udf.final(c)
		}
	}
	//	fmt.Printf("Contexts: %v\n", udf.aggrCtxts)
}

//export goXDestroy
func goXDestroy(pApp unsafe.Pointer) {
	udf := (*sqliteFunction)(pApp)
	if udf.d != nil {
		udf.d(udf.pApp)
	}
}

// CreateScalarFunction creates or redefines SQL scalar functions.
// TODO Make possible to specify the preferred encoding
// (See http://sqlite.org/c3ref/create_function.html)
func (c *Conn) CreateScalarFunction(functionName string, nArg int, pApp interface{}, f ScalarFunction, d DestroyFunctionData) error {
	fname := C.CString(functionName)
	defer C.free(unsafe.Pointer(fname))
	if f == nil {
		if len(c.udfs) > 0 {
			delete(c.udfs, functionName)
		}
		return c.error(C.sqlite3_create_function_v2(c.db, fname, C.int(nArg), C.SQLITE_UTF8, nil, nil, nil, nil, nil),
			fmt.Sprintf("<Conn.CreateScalarFunction(%q)", functionName))
	}
	// To make sure it is not gced, keep a reference in the connection.
	udf := &sqliteFunction{f, nil, nil, d, pApp, make(map[*ScalarContext]bool), nil}
	if len(c.udfs) == 0 {
		c.udfs = make(map[string]*sqliteFunction)
	}
	c.udfs[functionName] = udf // FIXME same function name with different args is not supported
	return c.error(C.goSqlite3CreateScalarFunction(c.db, fname, C.int(nArg), C.SQLITE_UTF8, unsafe.Pointer(udf)),
		fmt.Sprintf("Conn.CreateScalarFunction(%q)", functionName))
}

// CreateAggregateFunction creates or redefines SQL aggregate functions.
// TODO Make possible to specify the preferred encoding
// (See http://sqlite.org/c3ref/create_function.html)
func (c *Conn) CreateAggregateFunction(functionName string, nArg int, pApp interface{},
	step StepFunction, final FinalFunction, d DestroyFunctionData) error {
	fname := C.CString(functionName)
	defer C.free(unsafe.Pointer(fname))
	if step == nil {
		if len(c.udfs) > 0 {
			delete(c.udfs, functionName)
		}
		return c.error(C.sqlite3_create_function_v2(c.db, fname, C.int(nArg), C.SQLITE_UTF8, nil, nil, nil, nil, nil),
			fmt.Sprintf("<Conn.CreateAggregateFunction(%q)", functionName))
	}
	// To make sure it is not gced, keep a reference in the connection.
	udf := &sqliteFunction{nil, step, final, d, pApp, nil, make(map[*AggregateContext]bool)}
	if len(c.udfs) == 0 {
		c.udfs = make(map[string]*sqliteFunction)
	}
	c.udfs[functionName] = udf // FIXME same function name with different args is not supported
	return c.error(C.goSqlite3CreateAggregateFunction(c.db, fname, C.int(nArg), C.SQLITE_UTF8, unsafe.Pointer(udf)),
		fmt.Sprintf("Conn.CreateAggregateFunction(%q)", functionName))
}
