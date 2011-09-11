package sqlite

import (
	"testing"
	"time"
)

func TestJulianDay(t *testing.T) {
	utc := JulianDayToUTC(JULIAN_DAY)
	if utc.Nanoseconds() != 0 {
		t.Errorf("Error, expecting %d got %d", 0, utc.Nanoseconds())
	}
	now := time.LocalTime()
	r := JulianDayToLocalTime(JulianDay(now))
	if r.Nanoseconds()/10000 != now.Nanoseconds()/10000 { // FIXME Rounding problem?
		t.Errorf("%#v <> %#v", now, r)
	}
}
