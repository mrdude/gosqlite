// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"container/list"
	"sync"
)

type Cache struct {
	m sync.Mutex
	l *list.List
	MaxSize int // Cache turned off when MaxSize <= 0
}

func (c *Cache) find(sql string) *Stmt {
	if c.MaxSize <= 0 {
		return nil
	}
	c.m.Lock()
	defer c.m.Unlock()
	for e := c.l.Front(); e != nil; e = e.Next() {
		if s, ok := e.Value.(*Stmt); ok {
			if s.SQL() == sql {
				c.l.Remove(e)
				return s
			}
		}
	}
	return nil
}

func (c *Cache) release(s *Stmt) {
	if c.MaxSize <= 0 {
		s.Finalize()
		return
	}
	c.m.Lock()
	defer c.m.Unlock()
	c.l.InsertBefore(s, c.l.Front())
	for c.l.Len() > c.MaxSize {
		v := c.l.Remove(c.l.Back())
		if s, ok := v.(*Stmt); ok {
			s.Finalize()
		}
	}
}

func (c *Cache) flush() {
	c.m.Lock()
	defer c.m.Unlock()
	var e, next *list.Element
	for e = c.l.Front(); e != nil; e = next {
		next = e.Next()
		v := c.l.Remove(e)
		if s, ok := v.(*Stmt); ok {
			s.Finalize()
		}
	}
}
