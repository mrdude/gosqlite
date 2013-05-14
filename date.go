// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

import (
	"database/sql/driver"
	"fmt"
	"time"
)

const (
	julianDay    = 2440587.5 // 1970-01-01 00:00:00 is JD 2440587.5
	dayInSeconds = 60 * 60 * 24
)

// JulianDayToUTC transforms a julian day number into an UTC Time.
func JulianDayToUTC(jd float64) time.Time {
	jd -= julianDay
	jd *= dayInSeconds
	return time.Unix(int64(jd), 0).UTC()
}

// JulianDayToLocalTime transforms a julian day number into a local Time.
func JulianDayToLocalTime(jd float64) time.Time {
	jd -= julianDay
	jd *= dayInSeconds
	return time.Unix(int64(jd), 0)
}

// JulianDay converts a Time into a julian day number.
func JulianDay(t time.Time) float64 {
	ns := float64(t.Unix())
	if ns >= 0 {
		ns += 0.5
	}
	return ns/dayInSeconds + julianDay
}

// UnixTime is an alias used to persist time as int64 (max precision is 1s and timezone is lost) (default)
type UnixTime time.Time

func (t *UnixTime) Scan(src interface{}) error {
	if src == nil {
		t = nil
		return nil
	} else if unixepoch, ok := src.(int64); ok {
		*t = UnixTime(time.Unix(unixepoch, 0)) // local time
		return nil
	}
	return fmt.Errorf("Unsupported UnixTime src: %T", src)
}
func (t UnixTime) Value() (driver.Value, error) {
	if (time.Time)(t).IsZero() {
		return nil, nil
	}
	return (time.Time)(t).Unix(), nil
}

// JulianTime is an alias used to persist time as float64 (max precision is 1s and timezone is lost)
type JulianTime time.Time

func (t *JulianTime) Scan(src interface{}) error {
	if src == nil {
		t = nil
		return nil
	} else if jd, ok := src.(float64); ok {
		*t = JulianTime(JulianDayToLocalTime(jd)) // local time
		return nil
	}
	return fmt.Errorf("Unsupported JulianTime src: %T", src)
}
func (t JulianTime) Value() (driver.Value, error) {
	if (time.Time)(t).IsZero() {
		return nil, nil
	}
	return JulianDay((time.Time)(t)), nil
}

// TimeStamp is an alias used to persist time as '2006-01-02T15:04:05.999Z07:00' string
type TimeStamp time.Time

func (t *TimeStamp) Scan(src interface{}) error {
	if src == nil {
		t = nil
		return nil
	} else if txt, ok := src.(string); ok {
		v, err := time.Parse("2006-01-02T15:04:05.999Z07:00", txt)
		if err != nil {
			return err
		}
		*t = TimeStamp(v)
		return nil
	}
	return fmt.Errorf("Unsupported TimeStamp src: %T", src)
}
func (t TimeStamp) Value() (driver.Value, error) {
	if (time.Time)(t).IsZero() {
		return nil, nil
	}
	return (time.Time)(t).Format("2006-01-02T15:04:05.999Z07:00"), nil
}
