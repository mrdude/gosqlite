package sqlite

import (
	"testing"
	"time"
)

func TestJulianDay(t *testing.T) {
	utc := JulianDayToUTC(JULIAN_DAY)
	if utc.Unix() != 0 {
		t.Errorf("Error, expecting %d got %d", 0, utc.Unix())
	}
	now := time.Now()
	r := JulianDayToLocalTime(JulianDay(now))
	if r.Unix() != now.Unix() { // FIXME Rounding problem?
		t.Errorf("%#v <> %#v", now, r)
	}
}
