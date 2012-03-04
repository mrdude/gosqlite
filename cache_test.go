package sqlite_test

import (
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

func TestEnabledCache(t *testing.T) {
	db := open(t)
	defer db.Close()

	db.SetCacheSize(10)
	if size, maxSize := db.CacheSize(); size != 0 || maxSize != 10 {
		t.Errorf("%d <> %d || %d <> %d", 0, size, 10, maxSize)
	}

	s, err := db.CacheOrPrepare("SELECT 1")
	checkNoError(t, err, "couldn't prepare stmt: %#v")
	if !s.Cacheable {
		t.Error("expected cacheable stmt")
	}

	err = s.Finalize()
	checkNoError(t, err, "couldn't finalize stmt: %#v")
	if size, maxSize := db.CacheSize(); size != 1 || maxSize != 10 {
		t.Errorf("%d <> %d || %d <> %d", 1, size, 10, maxSize)
	}

	ns, err := db.CacheOrPrepare("SELECT 1")
	checkNoError(t, err, "couldn't prepare stmt: %#v")
	if size, maxSize := db.CacheSize(); size != 0 || maxSize != 10 {
		t.Errorf("%d <> %d || %d <> %d", 0, size, 10, maxSize)
	}

	err = ns.Finalize()
	checkNoError(t, err, "couldn't finalize stmt: %#v")
	if size, maxSize := db.CacheSize(); size != 1 || maxSize != 10 {
		t.Errorf("%d <> %d || %d <> %d", 1, size, 10, maxSize)
	}

	db.SetCacheSize(0)
	if size, maxSize := db.CacheSize(); size != 0 || maxSize != 0 {
		t.Errorf("%d <> %d || %d <> %d", 0, size, 0, maxSize)
	}
}
