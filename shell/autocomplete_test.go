// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shell_test

import (
	"testing"

	"github.com/bmizerany/assert"
	"github.com/gwenn/gosqlite"
	. "github.com/gwenn/gosqlite/shell"
)

func createCache(t *testing.T) *CompletionCache {
	cc, err := CreateCache()
	assert.Tf(t, err == nil, "%v", err)
	return cc
}

func TestPragmaNames(t *testing.T) {
	cc := createCache(t)
	defer cc.Close()
	pragmas, err := cc.CompletePragma("fo")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equalf(t, 3, len(pragmas), "got %d pragmas; expected %d", len(pragmas), 3)
	assert.Equal(t, []string{"foreign_key_check", "foreign_key_list(", "foreign_keys"}, pragmas, "unexpected pragmas")
}

func TestFuncNames(t *testing.T) {
	cc := createCache(t)
	defer cc.Close()
	funcs, err := cc.CompleteFunc("su")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equal(t, 2, len(funcs), "got %d functions; expected %d", len(funcs), 2)
	assert.Equal(t, []string{"substr(", "sum("}, funcs, "unexpected functions")
}

func TestCmdNames(t *testing.T) {
	cc := createCache(t)
	defer cc.Close()
	cmds, err := cc.CompleteCmd(".h")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equal(t, 2, len(cmds), "got %d commands; expected %d", len(cmds), 2)
	assert.Equal(t, []string{".headers", ".help"}, cmds, "unexpected commands")
}

func TestCache(t *testing.T) {
	db, err := sqlite.Open(":memory:")
	assert.Tf(t, err == nil, "%v", err)
	defer db.Close()
	cc := createCache(t)
	defer cc.Close()
	err = cc.Cache(db)
	assert.Tf(t, err == nil, "%v", err)
	err = cc.Flush(db)
	assert.Tf(t, err == nil, "%v", err)
}

func TestCacheUpdate(t *testing.T) {
	db, err := sqlite.Open(":memory:")
	assert.Tf(t, err == nil, "%v", err)
	defer db.Close()
	cc := createCache(t)
	defer cc.Close()
	err = cc.Cache(db)
	assert.Tf(t, err == nil, "%v", err)
	err = db.FastExec("CREATE TABLE test (id INTEGER PRIMARY KEY NOT NULL, name TEXT UNIQUE NOT NULL)")
	assert.Tf(t, err == nil, "%v", err)
	cc.Update(db)

	db_names, err := cc.CompleteDbName("m")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equal(t, 1, len(db_names), "got %d database names; expected %d", len(db_names), 1)
	assert.Equal(t, []string{"main"}, db_names, "unexpected database names")

	tbl_names, err := cc.CompleteTableName("", "te", "")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equal(t, 1, len(tbl_names), "got %d table names; expected %d", len(tbl_names), 1)
	assert.Equal(t, []string{"test"}, tbl_names, "unexpected table names")

	tbl_names, err = cc.CompleteTableName("main", "te", "table")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equal(t, 1, len(tbl_names), "got %d table names; expected %d", len(tbl_names), 1)
	assert.Equal(t, []string{"test"}, tbl_names, "unexpected table names")

	col_names, err := cc.CompleteColName("", []string{"test"}, "n")
	assert.Tf(t, err == nil, "%v", err)
	assert.Equal(t, 1, len(col_names), "got %d column names; expected %d", len(col_names), 1)
	assert.Equal(t, []string{"name"}, col_names, "unexpected column names")
}
