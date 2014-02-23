// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build ignore

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/signal"
	"os/user"
	"path"
	"syscall"
	"text/tabwriter"
	"unicode"

	"github.com/gwenn/gosqlite"
	"github.com/gwenn/liner"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}
func trace(err error) bool {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
	return err != nil
}

const (
	mainPrompt      = "sqlite> "
	continuePrompt  = "   ...> "
	historyFileName = ".gosqlite_history"
)

func loadHistory(state *liner.State, historyFileName string) error {
	//liner.HistoryLimit = 100 // to limit memory usage
	user, err := user.Current()
	if err != nil {
		return err
	}
	path := path.Join(user.HomeDir, historyFileName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	_, err = state.ReadHistory(r)
	return err
}

func isBlank(line string) bool {
	for _, r := range line {
		if unicode.IsSpace(r) {
			continue
		}
		return false
	}
	return true
}
func isCommand(line string) bool {
	return len(line) > 0 && line[0] == '.'
}

func appendHistory(state *liner.State, item string) {
	state.AppendHistory(item) // ignore consecutive dups, blank line, space
}

func saveHistory(state *liner.State, historyFileName string) error {
	user, err := user.Current()
	if err != nil {
		return err
	}
	path := path.Join(user.HomeDir, historyFileName)
	// append for multi-sessions handling
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600) // os.O_TRUNC
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	_, err = state.WriteHistory(w)
	if err != nil {
		return err
	}
	err = w.Flush()
	return err
}

// Ctl-C
func catchInterrupt() {
	ch := make(chan os.Signal)
	go func() {
		for _ = range ch {
			/*db.Interrupt()
			if !interactive {
				os.Exit(0)
			}*/
			fmt.Fprintln(os.Stderr, "^C")
		}
	}()
	signal.Notify(ch, syscall.SIGINT)
}

func main() {
	if !liner.IsTerminal() {
		return // TODO non-interactive mode
	}
	state, err := liner.NewLiner()
	check(err)
	defer func() {
		err := saveHistory(state, historyFileName)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		state.Close()
	}()
	// TODO state.SetCompleter(completion)
	err = loadHistory(state, historyFileName)
	check(err)

	dbFilename := ":memory:"
	if len(os.Args) > 1 {
		dbFilename = os.Args[1]
	}
	db, err := sqlite.Open(dbFilename) // TODO command-line flag
	check(err)
	defer db.Close()

	catchInterrupt()

	// TODO .mode MODE ?TABLE?     Set output mode where MODE is one of:
	// TODO .separator STRING      Change separator used by output mode and .import
	tw := tabwriter.NewWriter(os.Stdout, 0, 8, 0, '\t', 0)
	prompt := mainPrompt
	var b bytes.Buffer
	for {
		line, err := state.Prompt(prompt)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "%#v\n", err)
			}
			println() /* End of input */
			break
		}

		if isBlank(line) {
			continue
		} else if isCommand(line) {
			fmt.Println("TBD")
			continue
		}

		b.WriteString(line)
		cmd := b.String()
		if !sqlite.Complete(cmd) {
			b.WriteByte(' ') // TODO Validate ' ' versus '\n'
			prompt = continuePrompt
			continue
		}
		prompt = mainPrompt
		// TODO .echo ON|OFF           Turn command echo on or off
		//fmt.Println(cmd)
		appendHistory(state, cmd)
		for len(cmd) > 0 {
			s, err := db.Prepare(cmd)
			if trace(err) {
				break // TODO bail_on_error
			} else if s.Empty() {
				cmd = s.Tail()
				continue
			}
			columnCount := s.ColumnCount()
			if columnCount > 0 {
				// FIXME headers are displayed only if DataCount() > 0
				headers := s.ColumnNames() // TODO .header(s) ON|OFF      Turn display of headers on or off
				for _, header := range headers {
					io.WriteString(tw, header)
					io.WriteString(tw, "\t")
				}
				io.WriteString(tw, "\n")
				err = s.Select(func(s *sqlite.Stmt) error {
					for i := 0; i < columnCount; i++ {
						blob, _ := s.ScanRawBytes(i)
						// TODO .nullvalue STRING      Use STRING in place of NULL values
						tw.Write(blob)
						io.WriteString(tw, "\t") // https://github.com/kr/text
					}
					io.WriteString(tw, "\n")
					return nil
				})
				tw.Flush()
			} else {
				err = s.Exec()
			}
			if trace(err) {
				s.Finalize()
				break // TODO bail_on_error
			}
			if trace(s.Finalize()) {
				break // TODO bail_on_error
			}
			cmd = s.Tail()
		} // exec
		b.Reset()
	}
}
