// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"fmt"
	"io"
)

// IntegrityCheck checks database integrity.
// Database name is optional (default is 'main').
// (See http://www.sqlite.org/pragma.html#pragma_integrity_check
// and http://www.sqlite.org/pragma.html#pragma_quick_check)
func (c *Conn) IntegrityCheck(dbName string, max int, quick bool) error {
	var prefix string
	if quick {
		prefix = "quick"
	} else {
		prefix = "integrity"
	}
	pragmaName := fmt.Sprintf("%s_check(%d)", prefix, max)
	var msg string
	err := c.oneValue(pragma(dbName, pragmaName), &msg)
	if err != nil {
		return err
	}
	if msg != "ok" {
		return c.specificError("Integrity check failed (%s)", msg)
	}
	return nil
}

// Encoding returns the text encoding used by the specified database.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_encoding)
func (c *Conn) Encoding(dbName string) (string, error) {
	var encoding string
	err := c.oneValue(pragma(dbName, "encoding"), &encoding)
	if err != nil {
		return "", err
	}
	return encoding, nil
}

// SchemaVersion gets the value of the schema-version.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_schema_version)
func (c *Conn) SchemaVersion(dbName string) (int, error) {
	var version int
	err := c.oneValue(pragma(dbName, "schema_version"), &version)
	if err != nil {
		return -1, err
	}
	return version, nil
}

// SetRecursiveTriggers sets or clears the recursive trigger capability.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_recursive_triggers)
func (c *Conn) SetRecursiveTriggers(dbName string, on bool) error {
	return c.exec(pragma(dbName, fmt.Sprintf("recursive_triggers=%t", on)))
}

// JournalMode queries the current journaling mode for database.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_journal_mode)
func (c *Conn) JournalMode(dbName string) (string, error) {
	var mode string
	err := c.oneValue(pragma(dbName, "journal_mode"), &mode)
	if err != nil {
		return "", err
	}
	return mode, nil
}

// SetJournalMode changes the journaling mode for database.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_journal_mode)
func (c *Conn) SetJournalMode(dbName, mode string) (string, error) {
	var newMode string
	err := c.oneValue(pragma(dbName, Mprintf("journal_mode=%Q", mode)), &newMode)
	if err != nil {
		return "", err
	}
	return newMode, nil
}

// LockingMode queries the database connection locking-mode.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_locking_mode)
func (c *Conn) LockingMode(dbName string) (string, error) {
	var mode string
	err := c.oneValue(pragma(dbName, "locking_mode"), &mode)
	if err != nil {
		return "", err
	}
	return mode, nil
}

// SetLockingMode changes the database connection locking-mode.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_locking_mode)
func (c *Conn) SetLockingMode(dbName, mode string) (string, error) {
	var newMode string
	err := c.oneValue(pragma(dbName, Mprintf("locking_mode=%Q", mode)), &newMode)
	if err != nil {
		return "", err
	}
	return newMode, nil
}

// Synchronous queries the synchronous flag.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_synchronous)
func (c *Conn) Synchronous(dbName string) (int, error) {
	var mode int
	err := c.oneValue(pragma(dbName, "synchronous"), &mode)
	if err != nil {
		return -1, err
	}
	return mode, nil
}

// SetSynchronous changes the synchronous flag.
// Database name is optional (default is 'main').
// (See http://sqlite.org/pragma.html#pragma_synchronous)
func (c *Conn) SetSynchronous(dbName string, mode int) error {
	return c.exec(pragma(dbName, fmt.Sprintf("synchronous=%d", mode)))
}

func pragma(dbName, pragmaName string) string {
	if len(dbName) == 0 {
		return "PRAGMA " + pragmaName
	}
	return Mprintf("PRAGMA %Q."+pragmaName, dbName)
}

func (c *Conn) oneValue(query string, value interface{}, args ...interface{}) error { // no cache
	s, err := c.prepare(query, args...)
	if err != nil {
		return err
	}
	defer func() {
		s.Reset()
		s.finalize()
	}()
	b, err := s.Next()
	if err != nil {
		return err
	} else if !b {
		return io.EOF
	}
	return s.Scan(value)
}
