package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

func TestBackup(t *testing.T) {
	dst := open(t)
	defer dst.Close()
	src := open(t)
	defer src.Close()
	fill(src, 1000)

	bck, err := NewBackup(dst, "main", src, "main")
	checkNoError(t, err, "couldn't init backup: %#v")

	cbs := make(chan BackupStatus)
	go func() {
		for {
			s := <-cbs
			t.Logf("Backup progress %#v\n", s)
		}
	}()
	err = bck.Run(10, 0, cbs)
	checkNoError(t, err, "couldn't do backup: %#v")

	err = bck.Close()
	checkNoError(t, err, "couldn't close backup twice: %#v")
}

func TestBackupMisuse(t *testing.T) {
	db := open(t)
	defer db.Close()

	bck, err := NewBackup(db, "", db, "")
	assert(t, "source and destination must be distinct", bck == nil && err != nil)
	err = bck.Run(10, 0, nil)
	assert(t, "misuse expected", err != nil)
}
