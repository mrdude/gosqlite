// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"container/list"
	"fmt"
	"sync"
)

const (
	defaultCacheSize = 0
)

// Like http://www.sqlite.org/tclsqlite.html#cache
type Cache struct {
	m       sync.Mutex
	l       *list.List
	maxSize int // Cache turned off when maxSize <= 0
}

func newCache() *Cache {
	return newCacheSize(defaultCacheSize)
}
func newCacheSize(maxSize int) *Cache {
	if maxSize <= 0 {
		return &Cache{maxSize: maxSize}
	}
	return &Cache{l: list.New(), maxSize: maxSize}
}

// TODO To be called in Conn#Prepare
func (c *Cache) find(sql string) *Stmt {
	if c.maxSize <= 0 {
		return nil
	}
	c.m.Lock()
	defer c.m.Unlock()
	for e := c.l.Front(); e != nil; e = e.Next() {
		if s, ok := e.Value.(*Stmt); ok {
			if s.SQL() == sql { // TODO s.SQL() may have been trimmed by SQLite
				c.l.Remove(e)
				return s
			}
		}
	}
	return nil
}

// TODO To be called instead of Stmt#Finalize
func (c *Cache) release(s *Stmt) {
	if c.maxSize <= 0 || len(s.tail) > 0 {
		s.Finalize()
		return
	}
	c.m.Lock()
	defer c.m.Unlock()
	c.l.InsertBefore(s, c.l.Front())
	for c.l.Len() > c.maxSize {
		v := c.l.Remove(c.l.Back())
		if s, ok := v.(*Stmt); ok {
			s.Finalize()
		}
	}
}

// Finalize and free the cached prepared statements
// (To be called in Conn#Close)
func (c *Cache) flush() {
	if c.maxSize <= 0 {
		return
	}
	c.m.Lock()
	defer c.m.Unlock()
	var e, next *list.Element
	for e = c.l.Front(); e != nil; e = next {
		next = e.Next()
		v := c.l.Remove(e)
		if s, ok := v.(*Stmt); ok {
			s.Finalize()
		} else {
			panic(fmt.Sprintf("unexpected element in Stmt cache: %#v", v))
		}
	}
}

// Return (current, max) sizes.
// Cache is turned off when max size is 0
func (c *Conn) CacheSize() (int, int) {
	if c.stmtCache.maxSize <= 0 {
		return 0, 0
	}
	return c.stmtCache.l.Len(), c.stmtCache.maxSize
}

// Cache is turned off (and flushed) when size <= 0
func (c *Conn) SetCacheSize(size int) {
	stmtCache := c.stmtCache
	if stmtCache.l == nil && size > 0 {
		stmtCache.l = list.New()
	}
	if size <= 0 {
		stmtCache.flush()
	}
	stmtCache.maxSize = size
}
