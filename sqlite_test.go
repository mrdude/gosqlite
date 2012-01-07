package sqlite_test

import (
	. "github.com/gwenn/gosqlite"
	"reflect"
	"strings"
	"testing"
)

func checkNoError(t *testing.T, err error, format string) {
	if err != nil {
		t.Fatalf(format, err)
	}
}

func open(t *testing.T) *Conn {
	db, err := Open("", OPEN_READWRITE, OPEN_CREATE, OPEN_FULLMUTEX, OPEN_URI)
	checkNoError(t, err, "couldn't open database file: %s")
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
	checkNoError(t, err, "error creating table: %s")
}

func TestVersion(t *testing.T) {
	v := Version()
	if !strings.HasPrefix(v, "3") {
		t.Fatalf("unexpected library version: %s", v)
	}
}

func TestOpen(t *testing.T) {
	db := open(t)
	checkNoError(t, db.Close(), "Error closing database: %s")
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

func TestEnableExtendedResultCodes(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.EnableExtendedResultCodes(true), "cannot enabled extended result codes: %s")
}

func TestIntegrityCheck(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.IntegrityCheck(1, true), "Error checking integrity of database: %s")
}

func TestCreateTable(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
}

func TestTransaction(t *testing.T) {
	db := open(t)
	defer db.Close()
	checkNoError(t, db.Begin(), "Error while beginning transaction: %s")
	if err := db.Begin(); err == nil {
		t.Fatalf("Error expected (transaction cannot be nested)")
	}
	checkNoError(t, db.Commit(), "Error while commiting transaction: %s")
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
		checkNoError(t, ierr, "insert error: %s")
		c := db.Changes()
		if c != 1 {
			t.Errorf("insert error: %d but got 1", c)
		}
	}
	checkNoError(t, db.Commit(), "Error: %s")

	lastId := db.LastInsertRowid()
	if lastId != 1000 {
		t.Errorf("last insert row id error: %d but got 1000", lastId)
	}

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()

	paramCount := cs.BindParameterCount()
	if paramCount != 0 {
		t.Errorf("bind parameter count error: %d but got 0", paramCount)
	}
	columnCount := cs.ColumnCount()
	if columnCount != 1 {
		t.Errorf("column count error: %d but got 1", columnCount)
	}

	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}
}

func TestInsertWithStatement(t *testing.T) {
	db := open(t)
	defer db.Close()
	createTable(db, t)
	s, serr := db.Prepare("INSERT INTO test (float_num, int_num, a_string) VALUES (:f, :i, :s)")
	checkNoError(t, serr, "prepare error: %s")
	if s == nil {
		t.Fatal("statement is nil")
	}
	defer s.Finalize()

	if s.ReadOnly() {
		t.Errorf("update statement should not be readonly")
	}

	paramCount := s.BindParameterCount()
	if paramCount != 3 {
		t.Errorf("bind parameter count error: %d but got 3", paramCount)
	}
	firstParamName, berr := s.BindParameterName(1)
	if firstParamName != ":f" {
		t.Errorf("bind parameter name error: %s but got ':f' (%s)", firstParamName, berr)
	}
	lastParamIndex, berr := s.BindParameterIndex(":s")
	if lastParamIndex != 3 {
		t.Errorf("bind parameter name error: %d but got 3 (%s)", lastParamIndex, berr)
	}

	db.Begin()
	for i := 0; i < 1000; i++ {
		c, ierr := s.ExecUpdate(float64(i)*float64(3.14), i, "hello")
		checkNoError(t, ierr, "insert error: %s")
		if c != 1 {
			t.Errorf("insert error: %d but got 1", c)
		}
	}

	checkNoError(t, db.Commit(), "Error: %s")

	cs, _ := db.Prepare("SELECT COUNT(*) FROM test")
	defer cs.Finalize()
	if !cs.ReadOnly() {
		t.Errorf("select statement should be readonly")
	}
	if !Must(cs.Next()) {
		t.Fatal("no result for count")
	}
	var i int
	checkNoError(t, cs.Scan(&i), "error scanning count: %s")
	if i != 1000 {
		t.Errorf("count should be 1000, but it is %d", i)
	}

	rs, _ := db.Prepare("SELECT float_num, int_num, a_string FROM test where a_string like ? ORDER BY int_num LIMIT 2", "hel%")
	defer rs.Finalize()
	columnCount := rs.ColumnCount()
	if columnCount != 3 {
		t.Errorf("column count error: %d but got 3", columnCount)
	}
	secondColumnName := rs.ColumnName(1)
	if secondColumnName != "int_num" {
		t.Errorf("column name error: %s but got 'int_num'", secondColumnName)
	}

	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.Scan(&fnum, &inum, &sstr)
		if fnum != 0 {
			t.Errorf("Expected 0 but got %f\n", fnum)
		}
		if inum != 0 {
			t.Errorf("Expected 0 but got %d\n", inum)
		}
		if sstr != "hello" {
			t.Errorf("Expected 'hello' but got %s\n", sstr)
		}
	}
	if Must(rs.Next()) {
		var fnum float64
		var inum int64
		var sstr string
		rs.NamedScan("a_string", &sstr, "float_num", &fnum, "int_num", &inum)
		if fnum != 3.14 {
			t.Errorf("Expected 3.14 but got %f\n", fnum)
		}
		if inum != 1 {
			t.Errorf("Expected 1 but got %d\n", inum)
		}
		if sstr != "hello" {
			t.Errorf("Expected 'hello' but got %s\n", sstr)
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

func TestScanColumn(t *testing.T) {
	db := open(t)
	defer db.Close()

	s, err := db.Prepare("select 1, null, 0")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByIndex(0, &i1 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i1 != 1 {
		t.Errorf("Expected 1 but got %d\n", i1)
	}
	null = Must(s.ScanByIndex(1, &i2 /*, true*/ ))
	if !null {
		t.Errorf("Expected null value")
	} else if i2 != 0 {
		t.Errorf("Expected 0 but got %d\n", i2)
	}
	null = Must(s.ScanByIndex(2, &i3 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i3 != 0 {
		t.Errorf("Expected 0 but got %d\n", i3)
	}
}

func TestNamedScanColumn(t *testing.T) {
	db := open(t)
	defer db.Close()

	s, err := db.Prepare("select 1 as i1, null as i2, 0 as i3")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i1, i2, i3 int
	null := Must(s.ScanByName("i1", &i1 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i1 != 1 {
		t.Errorf("Expected 1 but got %d\n", i1)
	}
	null = Must(s.ScanByName("i2", &i2 /*, true*/ ))
	if !null {
		t.Errorf("Expected null value")
	} else if i2 != 0 {
		t.Errorf("Expected 0 but got %d\n", i2)
	}
	null = Must(s.ScanByName("i3", &i3 /*, true*/ ))
	if null {
		t.Errorf("Expected not null value")
	} else if i3 != 0 {
		t.Errorf("Expected 0 but got %d\n", i3)
	}
}

func TestScanCheck(t *testing.T) {
	db := open(t)
	defer db.Close()

	s, err := db.Prepare("select 'hello'")
	checkNoError(t, err, "prepare error: %s")
	defer s.Finalize()
	if !Must(s.Next()) {
		t.Fatal("no result")
	}
	var i int
	_, err = s.ScanByIndex(0, &i)
	if serr, ok := err.(*StmtError); ok {
		if serr.Filename() != "" {
			t.Errorf("Expected '' but got '%s'", serr.Filename())
		}
		if serr.Code() != ErrSpecific {
			t.Errorf("Expected %s but got %s", ErrSpecific, serr.Code())
		}
		if serr.SQL() != s.SQL() {
			t.Errorf("Expected %s but got %s", s.SQL(), serr.SQL())
		}
	} else {
		t.Errorf("Expected StmtError but got %s", reflect.TypeOf(err))
	}
}

/*
func TestLoadExtension(t *testing.T) {
	db := open(t)

	db.EnableLoadExtension(true)

	err := db.LoadExtension("/tmp/myext.so")
	checkNoError(t, err, "load extension error: %s")
}
*/
