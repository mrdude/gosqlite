// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"fmt"
)

// Check database integrity
// (See http://www.sqlite.org/pragma.html#pragma_integrity_check
// and http://www.sqlite.org/pragma.html#pragma_quick_check)
// TODO Make possible to specify the database-name (PRAGMA %Q.integrity_check(.))
func (c *Conn) IntegrityCheck(max int, quick bool) error {
	var pragma string
	if quick {
		pragma = "quick"
	} else {
		pragma = "integrity"
	}
	msg, err := c.OneValue(fmt.Sprintf("PRAGMA %s_check(%d)", pragma, max))
	if err != nil {
		return err
	}
	if msg != "ok" {
		return c.specificError("Integrity check failed (%s)", msg)
	}
	return nil
}

// Returns the text encoding used by the main database
// (See http://sqlite.org/pragma.html#pragma_encoding)
// TODO Make possible to specify the database-name (PRAGMA %Q.encoding)
func (c *Conn) Encoding() (string, error) {
	value, err := c.OneValue("PRAGMA encoding")
	if err != nil {
		return "", err
	}
	if encoding, ok := value.(string); ok {
		return encoding, nil
	}
	return "", c.specificError("Unexpected encoding (%v)", value)
}

// (See http://sqlite.org/pragma.html#pragma_schema_version)
// TODO Make possible to specify the database-name (PRAGMA %Q.schema_version)
func (c *Conn) SchemaVersion() (int64, error) {
	value, err := c.OneValue("PRAGMA schema_version")
	if err != nil {
		return -1, err
	}
	if version, ok := value.(int64); ok {
		return version, nil
	}
	return -1, c.specificError("Unexpected version (%v)", value)
}