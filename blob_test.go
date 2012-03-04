package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"testing"
)

func TestBlob(t *testing.T) {
	db := open(t)
	defer db.Close()

	err := db.Exec("CREATE TABLE test (content BLOB);")
	checkNoError(t, err, "error creating table: %s")
	s, err := db.Prepare("INSERT INTO test VALUES (?)")
	checkNoError(t, err, "prepare error: %s")
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()
	err = s.Exec(ZeroBlobLength(10))
	checkNoError(t, err, "insert error: %s")
	rowid := db.LastInsertRowid()

	bw, err := db.NewBlobReadWriter("main", "test", "content", rowid)
	checkNoError(t, err, "blob open error: %s")
	defer bw.Close()
	content := []byte("Clob")
	n, err := bw.Write(content)
	checkNoError(t, err, "blob write error: %s")

	br, err := db.NewBlobReader("main", "test", "content", rowid)
	checkNoError(t, err, "blob open error: %s")
	defer br.Close()
	size, err := br.Size()
	checkNoError(t, err, "blob size error: %s")
	content = make([]byte, size)
	n, err = br.Read(content)
	checkNoError(t, err, "blob read error: %s")
	if n != 10 {
		t.Fatalf("Expected 10 bytes but got %d", n)
	}
	//fmt.Printf("%#v\n", content)
	br.Close()
}

func TestBlobMisuse(t *testing.T) {
	db := open(t)
	defer db.Close()

	bw, err := db.NewBlobReadWriter("main", "test", "content", 0)
	//t.Logf("%#v", err)
	if bw != nil || err == nil {
		t.Errorf("error expected")
	}
	err = bw.Close()
	if err == nil {
		t.Errorf("error expected")
	}
}
