// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	pool := NewPool(func() (*Conn, error) {
		return open(t), nil
	}, 3, time.Minute*10)
	for i := 0; i <= 10; i++ {
		c, err := pool.Get()
		checkNoError(t, err, "error getting connection from the pool: %s")
		assert(t, "no connection returned by the pool", c != nil)
		assert(t, "connection returned by the pool is alive", !c.IsClosed())
		_, err = c.SchemaVersion("main")
		checkNoError(t, err, "error using connection from the pool: %s")
		pool.Release(c)
	}
	pool.Close()
	assert(t, "pool not closed", pool.IsClosed())
}

func TestTryGet(t *testing.T) {
	pool := NewPool(func() (*Conn, error) {
		return open(t), nil
	}, 1, time.Minute*10)
	defer pool.Close()
	c, err := pool.TryGet()
	checkNoError(t, err, "error getting connection from the pool: %s")
	assert(t, "no connection returned by the pool", c != nil)
	defer pool.Release(c)

	c1, err := pool.TryGet()
	assert(t, "no connection returned by the pool", c1 == nil && err == nil)
}
