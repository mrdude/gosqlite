// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all

package sqlite_test

import (
	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"

	"testing"
)

func TestIntArrayModule(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	err := db.FastExec(`CREATE TABLE t1 (x INT);
		INSERT INTO t1 VALUES (1), (3);
		CREATE TABLE t2 (y INT);
		INSERT INTO t2 VALUES (11);
		CREATE TABLE t3 (z INT);
		INSERT INTO t3 VALUES (-5);`)
	assert.T(t, err == nil)

	var p1, p2, p3 IntArray
	p1, err = db.CreateIntArray("ex1")
	assert.T(t, err == nil)
	p2, err = db.CreateIntArray("ex2")
	assert.T(t, err == nil)
	p3, err = db.CreateIntArray("ex3")
	assert.T(t, err == nil)

	s, err := db.Prepare(`SELECT * FROM t1, t2, t3
	 WHERE t1.x IN ex1
	  AND t2.y IN ex2
	  AND t3.z IN ex3`)
	assert.T(t, err == nil)
	defer checkFinalize(s, t)

	p1.Bind([]int64{1, 2, 3, 4})
	p2.Bind([]int64{5, 6, 7, 8, 9, 10, 11})
	// Fill in content of a3
	p3.Bind([]int64{-1, -5, -10})

	var i1, i2, i3 int64
	for checkStep(t, s) {
		err = s.Scan(&i1, &i2, &i3)
		assert.T(t, err == nil)
		assert.T(t, i1 == 1 || i1 == 3)
		assert.T(t, i2 == 11)
		assert.T(t, i3 == -5)
	}
}
