// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build all

package sqlite

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gwenn/yacr"
)

type csvModule struct {
}

// args[0] => module name
// args[1] => db name
// args[2] => table name

func (m csvModule) Create(c *Conn, args []string) (VTab, error) {
	if len(args) < 4 {
		return nil, errors.New("no CSV file specified")
	}
	/* pull out name of csv file (remove quotes) */
	filename := args[3]
	if filename[0] == '\'' {
		filename = filename[1 : len(filename)-1]
	}
	/* if a custom delimiter specified, pull it out */
	var separator byte = ','
	/* should the header zRow be used */
	useHeaderRow := false
	quoted := true
	guess := true
	for i := 4; i < len(args); i++ {
		arg := args[i]
		switch {
		case strings.Contains(strings.ToUpper(arg), "HEADER"):
			useHeaderRow = true
		case strings.Contains(strings.ToUpper(arg), "NO_QUOTE"):
			quoted = false
		case len(arg) == 1:
			separator = arg[0]
			guess = false
		case len(arg) == 3 && arg[0] == '\'':
			separator = arg[1]
			guess = false
		}
	}
	/* open the source csv file */
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening CSV file: '%s'", filename)
	}
	defer file.Close()
	/* Read first zRow to obtain column names/number */
	vTab := &csvTab{f: filename, sep: separator, quoted: quoted, cols: make([]string, 0, 10)}
	vTab.maxLength = int(c.Limit(LimitLength))
	vTab.maxColumn = int(c.Limit(LimitColumn))

	reader := yacr.NewReader(file, separator, quoted, guess)
	if useHeaderRow {
		reader.Split(vTab.split(reader.ScanField))
	}
	if err = vTab.readRow(reader); err != nil || len(vTab.cols) == 0 {
		if err == nil {
			err = errors.New("no columns found")
		}
		return nil, err
	}
	if guess {
		vTab.sep = reader.Sep()
	}
	/* Create the underlying relational database schema. If
	 * that is successful, call sqlite3_declare_vtab() to configure
	 * the csv table schema.
	 */
	sql := "CREATE TABLE x("
	tail := ", "
	for i, col := range vTab.cols {
		if i == len(vTab.cols)-1 {
			tail = ");"
		}
		if useHeaderRow {
			if len(col) == 0 {
				return nil, errors.New("no column name found")
			}
			sql = fmt.Sprintf("%s\"%s\"%s", sql, col, tail)
		} else {
			sql = fmt.Sprintf("%scol%d%s", sql, i+1, tail)
		}
	}
	if err = c.DeclareVTab(sql); err != nil {
		return nil, err
	}
	return vTab, nil
}
func (m csvModule) Connect(c *Conn, args []string) (VTab, error) {
	return m.Create(c, args)
}

func (m csvModule) Destroy() { // nothing to do
}

type csvTab struct {
	f              string
	sep            byte
	quoted         bool
	eof            bool
	offsetFirstRow int64
	cols           []string

	maxLength int
	maxColumn int
}

func (v *csvTab) split(original bufio.SplitFunc) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		advance, token, err = original(data, atEOF)
		v.offsetFirstRow += int64(advance)
		return
	}
}

func (v *csvTab) readRow(r *yacr.Reader) error {
	v.cols = v.cols[:0]
	for {
		if !r.Scan() {
			err := r.Err()
			v.eof = err == nil
			return err
		}
		if r.EmptyLine() { // skip empty line (or line comment)
			continue
		}
		col := r.Text()
		if len(col) >= v.maxLength {
			return fmt.Errorf("CSV row is too long (>= %d)", v.maxLength)
		}
		v.cols = append(v.cols, col)
		if len(v.cols) >= v.maxColumn {
			return fmt.Errorf("too many columns (>= %d)", v.maxColumn)
		}
		if r.EndOfRecord() {
			break
		}
	}
	return nil
}

func (v *csvTab) BestIndex() error {
	return nil
}
func (v *csvTab) Disconnect() error {
	return nil
}
func (v *csvTab) Destroy() error {
	return nil
}
func (v *csvTab) Open() (VTabCursor, error) {
	f, err := os.Open(v.f)
	if err != nil {
		return nil, err
	}
	return &csvTabCursor{vTab: v, f: f, rowNumber: 0}, nil
}

type csvTabCursor struct {
	vTab      *csvTab
	f         *os.File
	r         *yacr.Reader
	rowNumber int64
}

func (vc *csvTabCursor) Close() error {
	return vc.f.Close()
}
func (vc *csvTabCursor) Filter() error {
	v := vc.vTab
	/* seek back to start of first zRow */
	v.eof = false
	if _, err := vc.f.Seek(v.offsetFirstRow, os.SEEK_SET); err != nil {
		return err
	}
	vc.rowNumber = 0
	/* a new reader/scanner must be created because there is no way to reset its internal buffer/state (which has been invalidated by the SEEK_SET)*/
	vc.r = yacr.NewReader(vc.f, v.sep, v.quoted, false)
	/* read and parse next line */
	return vc.Next()
}
func (vc *csvTabCursor) Next() error {
	v := vc.vTab
	if v.eof {
		return io.EOF
	}
	if vc.r == nil {
		vc.r = yacr.NewReader(vc.f, v.sep, v.quoted, false)
	}
	/* read the next row of data */
	err := v.readRow(vc.r)
	if err == nil {
		vc.rowNumber++
	}
	return err
}
func (vc *csvTabCursor) Eof() bool {
	return vc.vTab.eof
}
func (vc *csvTabCursor) Column(c *Context, col int) error {
	cols := vc.vTab.cols
	if col < 0 || col >= len(cols) {
		return fmt.Errorf("column index out of bounds: %d", col)
	}
	if cols == nil {
		c.ResultNull()
		return nil
	}
	// TODO dynamic typing c.ResultInt64()
	c.ResultText(cols[col])
	return nil
}
func (vc *csvTabCursor) Rowid() (int64, error) {
	return vc.rowNumber, nil
}

// LoadCsvModule loads CSV virtual table module.
//   CREATE VIRTUAL TABLE vtab USING csv('test.csv', USE_HEADER_ROW, NO_QUOTE)
func LoadCsvModule(db *Conn) error {
	return db.CreateModule("csv", csvModule{})
}

// ExportTableToCSV export table or view content to CSV.
// 'headers' flag turns output of headers on or off.
// NULL values are output as specified by 'nullvalue' parameter.
func (db *Conn) ExportTableToCSV(dbName, table string, nullvalue string, headers bool, w *yacr.Writer) error {
	var sql string
	if len(dbName) == 0 {
		sql = fmt.Sprintf(`SELECT * FROM "%s"`, escapeQuote(table))
	} else {
		sql = fmt.Sprintf(`SELECT * FROM %s."%s"`, doubleQuote(dbName), escapeQuote(table))
	}
	s, err := db.prepare(sql)
	if err != nil {
		return err
	}
	defer s.finalize()
	return s.ExportToCSV(nullvalue, headers, w)
}

// ExportTableToCSV export statement result to CSV.
// 'headers' flag turns output of headers on or off.
// NULL values are output as specified by 'nullvalue' parameter.
func (s *Stmt) ExportToCSV(nullvalue string, headers bool, w *yacr.Writer) error {
	if headers {
		for _, header := range s.ColumnNames() {
			w.Write([]byte(header))
		}
		w.EndOfRecord()
		if err := w.Err(); err != nil {
			return err
		}
	}
	s.Select(func(s *Stmt) error {
		for i := 0; i < s.ColumnCount(); i++ {
			rb, null := s.ScanRawBytes(i)
			if null {
				w.Write([]byte(nullvalue))
			} else {
				w.Write(rb)
			}
		}
		w.EndOfRecord()
		return w.Err()
	})
	w.Flush()
	return w.Err()
}

type ImportConfig struct {
	Name      string     // the name of the input; used only for error reports
	Separator byte       // CSV separator
	Quoted    bool       // CSV field are quoted or not
	Guess     bool       // guess separator
	Trim      bool       // trim spaces
	Comment   byte       // comment marker
	Headers   bool       // skip headers (first line)
	Types     []Affinity // optional, when target table does not exist, specify columns type
	Log       io.Writer  // optional, used to tace lines in error
}

func (ic ImportConfig) getType(i int) string {
	if i >= len(ic.Types) || ic.Types[i] == Textual {
		return "TEXT"
	}
	if ic.Types[i] == Integral {
		return "INT"
	}
	if ic.Types[i] == Real {
		return "REAL"
	}
	if ic.Types[i] == Numerical {
		return "NUMERIC"
	}
	return ""
}

// ImportCSV import CSV data into the specified table (which may not exist yet).
// Code is adapted from .import command implementation in SQLite3 shell sources.
func (db *Conn) ImportCSV(in io.Reader, ic ImportConfig, dbName, table string) error {
	columns, err := db.Columns(dbName, table)
	if err != nil {
		return err
	}
	r := yacr.NewReader(in, ic.Separator, ic.Quoted, ic.Guess)
	r.Trim = ic.Trim
	r.Comment = ic.Comment
	nCol := len(columns)
	if nCol == 0 { // table does not exist, let's create it
		var sql string
		if len(dbName) == 0 {
			sql = fmt.Sprintf(`CREATE TABLE "%s" `, escapeQuote(table))
		} else {
			sql = fmt.Sprintf(`CREATE TABLE %s."%s" `, doubleQuote(dbName), escapeQuote(table))
		}
		sep := '('
		for i := 0; r.Scan(); i++ {
			if r.EmptyLine() {
				continue
			}
			sql += fmt.Sprintf("%c\n  \"%s\" %s", sep, r.Text(), ic.getType(i))
			sep = ','
			nCol++
			if r.EndOfRecord() {
				break
			}
		}
		if err = r.Err(); err != nil {
			return err
		}
		if sep == '(' {
			return errors.New("empty file/input")
		}
		sql += "\n)"
		if err = db.FastExec(sql); err != nil {
			return err
		}
	} else if ic.Headers { // skip headers line
		for r.Scan() {
			if r.EndOfRecord() {
				break
			}
		}
		if err = r.Err(); err != nil {
			return err
		}
	}
	var sql string
	if len(dbName) == 0 {
		sql = fmt.Sprintf(`INSERT INTO "%s" VALUES (?%s)`, escapeQuote(table), strings.Repeat(", ?", nCol-1))
	} else {
		sql = fmt.Sprintf(`INSERT INTO %s."%s" VALUES (?%s)`, doubleQuote(dbName), escapeQuote(table), strings.Repeat(", ?", nCol-1))
	}
	s, err := db.prepare(sql)
	if err != nil {
		return err
	}
	defer s.Finalize()
	ac := db.GetAutocommit()
	if ac {
		if err = db.Begin(); err != nil {
			return err
		}
	}
	defer func() {
		if err != nil && ac {
			_ = db.Rollback()
		}
	}()
	startLine := r.LineNumber()
	for i := 1; r.Scan(); i++ {
		if r.EmptyLine() {
			i = 0
			startLine = r.LineNumber()
			continue
		}
		if i <= nCol {
			if err = s.BindByIndex(i, r.Text()); err != nil {
				return err
			}
		}
		if r.EndOfRecord() {
			if i < nCol {
				if ic.Log != nil {
					fmt.Fprintf(ic.Log, "%s:%d: expected %d columns but found %d - filling the rest with NULL\n", ic.Name, startLine, nCol, i)
				}
				for ; i <= nCol; i++ {
					if err = s.BindByIndex(i, nil); err != nil {
						return err
					}
				}
			} else if i > nCol && ic.Log != nil {
				fmt.Fprintf(ic.Log, "%s:%d: expected %d columns but found %d - extras ignored\n", ic.Name, startLine, nCol, i)
			}
			if _, err = s.Next(); err != nil {
				return err
			}
			i = 0
			startLine = r.LineNumber()
		}
	}
	if err = r.Err(); err != nil {
		return err
	}
	if ac {
		if err = db.Commit(); err != nil {
			return err
		}
	}
	return nil
}
