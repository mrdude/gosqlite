// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build ignore

package sqlite

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/gwenn/yacr"
)

type csvModule struct { // ok
}

// args[0] => module name
// args[1] => db name
// args[2] => table name

func (m csvModule) Create(c *Conn, args []string) (VTab, error) {
	/*
		err := c.DeclareVTab("CREATE TABLE x(test TEXT)")
		if err != nil {
			return nil, err
		}*/
	if len(args) < 4 {
		return nil, errors.New("No CSV file specified")
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
		return nil, fmt.Errorf("Error opening CSV file: '%s'", filename)
	}
	/* Read first zRow to obtain column names/number */
	reader := yacr.NewReader(file, separator, quoted, guess)
	vTab := &csvTab{f: file, r: reader, cols: make([]string, 0, 10)}
	vTab.maxLength = int(c.Limit(LimitLength))
	vTab.maxColumn = int(c.Limit(LimitColumn))

	if err = vTab.readRow(); err != nil || len(vTab.cols) == 0 {
		file.Close()
		if err == nil {
			err = errors.New("No columns found")
		}
		return nil, err
	}
	if useHeaderRow {
		if vTab.offsetFirstRow, err = file.Seek(0, os.SEEK_CUR); err != nil {
			file.Close()
			return nil, err
		}
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
				file.Close()
				return nil, errors.New("No column name found")
			}
			sql = fmt.Sprintf("%s\"%s\"%s", sql, col, tail)
		} else {
			sql = fmt.Sprintf("%scol%d%s", sql, i+1, tail)
		}
	}
	if err = c.DeclareVTab(sql); err != nil {
		file.Close()
		return nil, err
	}
	return vTab, nil
}
func (m csvModule) Connect(c *Conn, args []string) (VTab, error) { // ok
	return m.Create(c, args)
}

func (m csvModule) Destroy() { // nothing to do
}

type csvTab struct {
	f              *os.File
	r              *yacr.Reader
	eof            bool
	offsetFirstRow int64
	cols           []string

	maxLength int
	maxColumn int
}

func (v *csvTab) readRow() error {
	v.cols = v.cols[:0]
	for {
		if !v.r.Scan() {
			err := v.r.Err()
			v.eof = err == nil
			return err
		}
		if v.r.EmptyLine() { // skip empty line (or line comment)
			continue
		}
		col := v.r.Text()
		if len(col) >= v.maxLength {
			return fmt.Errorf("CSV row is too long (>= %d)", v.maxLength)
		}
		v.cols = append(v.cols, col)
		if len(v.cols) >= v.maxColumn {
			return fmt.Errorf("Too many columns (>= %d)", v.maxColumn)
		}
		if v.r.EndOfRecord() {
			break
		}
	}
	return nil
}

func (v *csvTab) release() error {
	// TODO csvRelease has a counter reference?
	if v != nil && v.f != nil {
		return v.f.Close()
	}
	return nil
}

func (v *csvTab) BestIndex() error { // ok
	return nil
}
func (v *csvTab) Disconnect() error { // ok
	return v.release()
}
func (v *csvTab) Destroy() error { // ok
	return v.release()
}
func (v *csvTab) Open() (VTabCursor, error) { // ok
	return &csvTabCursor{v, 0}, nil
}

type csvTabCursor struct {
	vTab   *csvTab
	csvpos int64 // ftell position of current zRow
}

func (vc *csvTabCursor) Close() error { // ok
	return nil
}
func (vc *csvTabCursor) Filter() error { // ok
	// csvFilter
	/* seek back to start of first zRow */
	vc.vTab.eof = false
	if _, err := vc.vTab.f.Seek(vc.vTab.offsetFirstRow, os.SEEK_SET); err != nil {
		return err
	}
	/* read and parse next line */
	return vc.Next()
}
func (vc *csvTabCursor) Next() (err error) { // ok
	if vc.vTab.eof {
		return io.EOF
	}
	/* update the cursor */
	if vc.csvpos, err = vc.vTab.f.Seek(0, os.SEEK_CUR); err != nil {
		return err
	}
	/* read the next row of data */
	return vc.vTab.readRow()
}
func (vc *csvTabCursor) Eof() bool { // ok
	return vc.vTab.eof
}
func (vc *csvTabCursor) Column(c *Context, col int) error { // ok
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
func (vc *csvTabCursor) Rowid() (int64, error) { // ok
	return vc.csvpos, nil
}

func LoadCsvModule(db *Conn) error { // ok
	return db.CreateModule("csv", csvModule{})
}
