// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all

package sqlite_test

import (
	"fmt"
	"testing"
	. "github.com/gwenn/gosqlite"
)

func init() {
	err := EnableSharedCache(false)
	if err != nil {
		panic(fmt.Sprintf("couldn't disable shared cache: '%s'", err))
	}
}

func TestEnableLoadExtension(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.EnableLoadExtension(false)
	checkNoError(t, err, "EnableLoadExtension error: %s")
}
