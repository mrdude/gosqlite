// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
	"time"
)

func TestJulianDay(t *testing.T) {
	utc := JulianDayToUTC(2440587.5)
	if utc.Unix() != 0 {
		t.Errorf("Error, expecting %d got %d", 0, utc.Unix())
	}
	now := time.Now()
	r := JulianDayToLocalTime(JulianDay(now))
	if r.Unix() != now.Unix() { // FIXME Rounding problem?
		t.Errorf("%#v <> %#v", now, r)
	}
}

func TestBindTime(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	var delta int
	err := db.OneValue("SELECT CAST(strftime('%s', 'now') AS NUMERIC) - ?", &delta, time.Now())
	checkNoError(t, err, "Error reading date: %#v")
	if delta != 0 {
		t.Errorf("Delta between Go and SQLite timestamps: %d", delta)
	}
}

func TestScanTime(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	var dt time.Time
	err := db.OneValue("SELECT date('now')", &dt)
	checkNoError(t, err, "Error reading date: %#v")
	if dt.IsZero() {
		t.Error("Unexpected zero date")
	}

	var tm time.Time
	err = db.OneValue("SELECT time('now')", &tm)
	checkNoError(t, err, "Error reading date: %#v")
	if tm.IsZero() {
		t.Error("Unexpected zero time")
	}

	var dtm time.Time
	err = db.OneValue("SELECT strftime('%Y-%m-%dT%H:%M:%f', 'now')", &dtm)
	checkNoError(t, err, "Error reading date: %#v")
	if dtm.IsZero() {
		t.Error("Unexpected zero datetime")
	}

	var jd time.Time
	err = db.OneValue("SELECT CAST(strftime('%J', 'now') AS NUMERIC)", &jd)
	checkNoError(t, err, "Error reading date: %#v")
	if jd.IsZero() {
		t.Error("Unexpected zero julian day")
	}

	var unix time.Time
	err = db.OneValue("SELECT CAST(strftime('%s', 'now') AS NUMERIC)", &unix)
	checkNoError(t, err, "Error reading date: %#v")
	if unix.IsZero() {
		t.Error("Unexpected zero julian day")
	}
}

func TestScanNullTime(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)

	var unix UnixTime
	err := db.OneValue("SELECT NULL", &unix)
	checkNoError(t, err, "Error scanning null time: %#v")
	if !(time.Time)(unix).IsZero() {
		t.Error("Expected zero time")
	}
}

func TestBindTimeAsString(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (time TEXT)")
	checkNoError(t, err, "exec error: %s")

	is, err := db.Prepare("INSERT INTO test (time) VALUES (?)")
	checkNoError(t, err, "prepare error: %s")

	now := time.Now()
	//id1, err := is.Insert(YearMonthDay(now))
	//checkNoError(t, err, "error inserting YearMonthDay: %s")
	id2, err := is.Insert(TimeStamp(now))
	checkNoError(t, err, "error inserting TimeStamp: %s")

	// The format used to persist has a max precision of 1ms.
	now = now.Truncate(time.Millisecond)

	var tim time.Time
	//err = db.OneValue("SELECT /*date(*/time/*)*/ FROM test where ROWID = ?", &tim, id1)
	//checkNoError(t, err, "error selecting YearMonthDay: %s")
	//assertEquals(t, "Year MonthDay: %d vs %d", now.Year(), tim.Year())
	//assertEquals(t, "YearMonth Day: %d vs %d", now.YearDay(), tim.YearDay())

	err = db.OneValue("SELECT /*datetime(*/time/*)*/ FROM test where ROWID = ?", &tim, id2)
	checkNoError(t, err, "error selecting TimeStamp: %s")
	assertEquals(t, "TimeStamp: %s vs %s", now, tim)
}

func TestBindTimeAsNumeric(t *testing.T) {
	db := open(t)
	defer checkClose(db, t)
	err := db.Exec("CREATE TABLE test (time NUMERIC)")
	checkNoError(t, err, "exec error: %s")

	is, err := db.Prepare("INSERT INTO test (time) VALUES (?)")
	checkNoError(t, err, "prepare error: %s")

	now := time.Now()
	id1, err := is.Insert(UnixTime(now))
	checkNoError(t, err, "error inserting UnixTime: %s")
	id2, err := is.Insert(JulianTime(now))
	checkNoError(t, err, "error inserting JulianTime: %s")
	checkFinalize(is, t)

	// And the format used to persist has a max precision of 1s.
	now = now.Truncate(time.Second)

	var tim time.Time
	err = db.OneValue("SELECT /*datetime(*/ time/*, 'unixepoch')*/ FROM test where ROWID = ?", &tim, id1)
	checkNoError(t, err, "error selecting UnixTime: %s")
	assertEquals(t, "Year: %s vs %s", now, tim)

	err = db.OneValue("SELECT /*julianday(*/time/*)*/ FROM test where ROWID = ?", &tim, id2)
	checkNoError(t, err, "error selecting JulianTime: %s")
	assertEquals(t, "Year: %s vs %s", now, tim)
}
