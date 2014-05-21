// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all

package sqlite_test

import (
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
)

func TestPragmaNames(t *testing.T) {
	pragmas := CompletePragma("fo")
	assert.Equalf(t, 3, len(pragmas), "got %d pragmas; expected %d", len(pragmas), 3)
	assert.Equal(t, []string{"foreign_key_check", "foreign_key_list(", "foreign_keys"}, pragmas, "unexpected pragmas")
}
func TestFuncNames(t *testing.T) {
	funcs := CompleteFunc("su")
	assert.Equal(t, 2, len(funcs), "got %d functions; expected %d", len(funcs), 2)
	assert.Equal(t, []string{"substr(", "sum("}, funcs, "unexpected functions")
}
