package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"strings"
	"testing"
)

func open(t *testing.T) *Conn {
	db, err := Open("", OPEN_READWRITE, OPEN_CREATE, OPEN_FULLMUTEX, OPEN_URI)
	if err != nil {
		t.Fatalf("couldn't open database file: %s", err)
	}
	if db == nil {
		t.Fatal("opened database is nil")
	}
	//db.Profile(profile, "PROFILE")
	return db
}

func createTable(db *Conn, t *testing.T) {
	err := db.Exec("DROP TABLE IF EXISTS test;" +
		"CREATE TABLE test (id INTEGER PRIMARY KEY AUTOINCREMENT," +
		" float_num REAL, int_num INTEGER, a_string TEXT); -- bim")
	if err != nil {
		t.Fatalf("error creating table: %s", err)
	}
}

func TestVersion(t *testing.T) {
	v := Version()
	if !strings.HasPrefix(v, "3") {
		t.Fatalf("unexpected library version: %s", v)
	}
}

func TestOpen(t *testing.T) {
	db := open(t)
	if err := db.Close(); err != nil {
		t.Fatalf("Error closing database: %s", err)
	}
}

func TestEnableFKey(t *testing.T) {
	db := open(t)
	defer db.Close()
	b := Must(db.IsFKeyEnabled())
	if !b {
		b = Must(db.EnableFKey(true))
		if !b {
			t.Error("cannot enabled FK")
		}
	}
}

func TestIntegrityCheck(t *testing.T) {
	db := open(t)
	defer db.Close()
	if err := db.IntegrityCheck(1, true); err != nil {
		t.Fatalf("Error checking integrity of database: %s", err)
	}
}

func TestCreateTable(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
}

func TestTransaction(t *testing.T) {
	db := open(t)
	defer db.Close()
	if err := db.Begin(); err != nil {
		t.Fatalf("Error while beginning transaction: %s", err)
	}
	if err := db.Begin(); err == nil {
		t.Fatalf("Error expected (transaction cannot be nested)")
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("Error while commiting transaction: %s", err)
	}
}

func TestExists(t *testing.T) {
	db := open(t)
	defer db.Close()
	b := Must(db.Exists("SELECT 1 where 1 = 0"))
	if b {
		t.Error("No row expected")
	}
	b = Must(db.Exists("SELECT 1 where 1 = 1"))
	if !b {
		t.Error("One row expected")
	}
}

func TestInsert(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	db.Begin()
	for i := 0; i < 1000; i++ {
		ierr := db.Exec("INSERT INTO test (float_num, int_num, a_string) VALUES (?, ?, ?)", float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Fatalf("insert error: %s", ierr)
		}
		c := db.Changes()
		if c != 1 {
			t.Errorf("insert error: %d <> 1", c)
		}
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("Error: %s", err)
	}

	lastId := db.LastInsertRowid()
	if lastId != 1000 {
		t.Errorf("last insert row id error: %d <> 1000", lastId)
	}

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()

	paramCount := cs.BindParameterCount()
	if paramCount != 0 {
		t.Errorf("bind parameter count error: %d <> 0", paramCount)
	}
	columnCount := cs.ColumnCount()
	if columnCount != 1 {
		t.Errorf("column count error: %d <> 1", columnCount)
	}

	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Fatalf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}
}

func TestInsertWithStatement(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (:f, :i, :s)")
	if serr != nil {
		t.Fatalf("prepare error: %s", serr)
	}
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()

	if s.ReadOnly() {
		t.Errorf("update statement is not readonly")
	}

	paramCount := s.BindParameterCount()
	if paramCount != 3 {
		t.Errorf("bind parameter count error: %d <> 3", paramCount)
	}
	firstParamName, berr := s.BindParameterName(1)
	if firstParamName != ":f" {
		t.Errorf("bind parameter name error: %s <> ':f' (%s)", firstParamName, berr)
	}
	lastParamIndex, berr := s.BindParameterIndex(":s")
	if lastParamIndex != 3 {
		t.Errorf("bind parameter name error: %d <> 3 (%s)", lastParamIndex, berr)
	}

	db.Begin()
	for i := 0; i < 1000; i++ {
		c, ierr := s.ExecUpdate(float64(i)*float64(3.14), i, "hello")
		if ierr != nil {
			t.Fatalf("insert error: %s", ierr)
		}
		if c != 1 {
			t.Errorf("insert error: %d <> 1", c)
		}
	}

	if err := db.Commit(); err != nil {
		t.Fatalf("Error: %s", err)
	}

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()
	if !cs.ReadOnly() {
		t.Errorf("update statement is not readonly")
	}
	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	err := cs.Scan(&i)
	if err != nil {
		t.Fatalf("error scanning count: %s", err)
	}
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}

	rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test where a_string like ? ORDER BY int_num LIMIT 2", "hel%")
	defer rs.Finalize()
	columnCount := rs.ColumnCount()
	if columnCount != 3 {
		t.Errorf("column count error: %d <> 3", columnCount)
	}
	secondColumnName := rs.ColumnName(1)
	if secondColumnName != "int_num" {
		t.Errorf("column name error: %s <> 'int_num'", secondColumnName)
	}

	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.Scan(&fnum, &inum, &sstr)
		if fnum != 0 {
			t.Errorf("Expected 0 <> %f\n", fnum)
		}
		if inum != 0 {
			t.Errorf("Expected 0 <> %d\n", inum)
		}
		if sstr != "hello" {
			t.Errorf("Expected 'hello' <> %s\n", sstr)
		}
	}
	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.NamedScan("a_string", &sstr, "float_num", &fnum, "int_num", &inum)
		if fnum != 3.14 {
			t.Errorf("Expected 3.14 <> %f\n", fnum)
		}
		if inum != 1 {
			t.Errorf("Expected 1 <> %d\n", inum)
		}
		if sstr != "hello" {
			t.Errorf("Expected 'hello' <> %s\n", sstr)
		}
	}
	if 999 != rs.Status(STMTSTATUS_FULLSCAN_STEP, false) {
		t.Errorf("Expected full scan")
	}
	if 1 != rs.Status(STMTSTATUS_SORT, false) {
		t.Errorf("Expected one sort")
	}
	if 0 != rs.Status(STMTSTATUS_AUTOINDEX, false) {
		t.Errorf("Expected no auto index")
	}
}

func TestBlob(t *testing.T) {
	db := open(t)
	defer db.Close()

	err := db.Exec("CREATE TABLE test (content BLOB);")
	if err != nil {
		t.Fatalf("error creating tables: %s", err)
	}
	s, err := db.Prepare("INSERT INTO test VALUES (?)")
	if err != nil {
		t.Fatalf("prepare error: %s", err)
	}
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()
	err = s.Exec(ZeroBlobLength(10))
	if err != nil {
		t.Fatalf("insert error: %s", err)
	}
	rowid := db.LastInsertRowid()

	bw, err := db.NewBlobReadWriter("main", "test", "content", rowid)
	if err != nil {
		t.Fatalf("blob open error: %s", err)
	}
	defer bw.Close()
	content := []byte("Clob")
	n, err := bw.Write(content)
	if err != nil {
		t.Fatalf("blob write error: %s", err)
	}

	br, err := db.NewBlobReader("main", "test", "content", rowid)
	if err != nil {
		t.Fatalf("blob open error: %s", err)
	}
	defer br.Close()
	size, err := br.Size()
	if err != nil {
		t.Fatalf("blob size error: %s", err)
	}
	content = make([]byte, size)
	n, err = br.Read(content)
	if err != nil {
		t.Fatalf("blob read error: %s", err)
	}
	if n != 10 {
		t.Fatalf("Expected 10 bytes <> %d", n)
	}
	//fmt.Printf("%#v\n", content)
	br.Close()
}

func TestScanColumn(t *testing.T) {
	db := open(t)
	defer db.Close()

	s, err := db.Prepare("select 1, null, 0")
	if err != nil {
		t.Fatalf("prepare error: %s", err)
	}
	defer s.Finalize()
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByIndex(0, &i1 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i1 != 1 {
		t.Errorf("Expected 1 <> %d\n", i1)
	}
	null = Must(s.ScanByIndex(1, &i2 /*, true*/ ))
	if !null {
		t.Errorf("Expected null value")
	} else if i2 != 0 {
		t.Errorf("Expected 0 <> %d\n", i2)
	}
	null = Must(s.ScanByIndex(2, &i3 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i3 != 0 {
		t.Errorf("Expected 0 <> %d\n", i3)
	}
}

func TestNamedScanColumn(t *testing.T) {
	db := open(t)
	defer db.Close()

	s, err := db.Prepare("select 1 as i1, null as i2, 0 as i3")
	if err != nil {
		t.Fatalf("prepare error: %s", err)
	}
	defer s.Finalize()
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByName("i1", &i1 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i1 != 1 {
		t.Errorf("Expected 1 <> %d\n", i1)
	}
	null = Must(s.ScanByName("i2", &i2 /*, true*/ ))
	if !null {
		t.Errorf("Expected null value")
	} else if i2 != 0 {
		t.Errorf("Expected 0 <> %d\n", i2)
	}
	null = Must(s.ScanByName("i3", &i3 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i3 != 0 {
		t.Errorf("Expected 0 <> %d\n", i3)
	}
}

/*
func TestLoadExtension(t *testing.T) {
	db := open(t)

	db.EnableLoadExtension(true)

	err := db.LoadExtension("/tmp/myext.so")
	if err != nil {
		t.Errorf("load extension error: %s", err)
	}
}
*/
