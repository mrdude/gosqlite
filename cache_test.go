package sqlite_test

import (
	//	. "github.com/gwenn/gosqlite"
	"testing"
)

func TestDisabledCache(t *testing.T) {
	db := open(t)
	defer db.Close()

	db.SetCacheSize(0)
	if size, maxSize := db.CacheSize(); size != 0 || maxSize != 0 {
		t.Errorf("%d <> %d || %d <> %d", 0, size, 0, maxSize)
	}

	s, err := db.CacheOrPrepare("SELECT 1")
	checkNoError(t, err, "couldn't prepare stmt: %#v")
	if !s.Cacheable {
		t.Error("expected cacheable stmt")
	}

	err = s.Finalize()
	checkNoError(t, err, "couldn't finalize stmt: %#v")

	if size, maxSize := db.CacheSize(); size != 0 || maxSize != 0 {
		t.Errorf("%d <> %d || %d <> %d", 0, size, 0, maxSize)
	}
}
