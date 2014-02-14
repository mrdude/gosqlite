// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	"testing"

	"github.com/bmizerany/assert"
	. "github.com/gwenn/gosqlite"
)

func TestLimit(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	limitVariableNumber := db.Limit(LimitVariableNumber)
	assert.T(t, limitVariableNumber < 1000, "unexpected value for LimitVariableNumber")
	oldLimitVariableNumber := db.SetLimit(LimitVariableNumber, 99)
	assert.Equalf(t, limitVariableNumber, oldLimitVariableNumber, "unexpected value for LimitVariableNumber: %d <> %d", limitVariableNumber, oldLimitVariableNumber)
	limitVariableNumber = db.Limit(LimitVariableNumber)
	assert.Equalf(t, int32(99), limitVariableNumber, "unexpected value for LimitVariableNumber: %d <> %d", 99, limitVariableNumber)

}
