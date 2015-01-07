// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package shell

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// SQL lexer based on template/parse/lex.go.
// itemType identifies the type of lex items.
type itemType int

// item represents a token or text string returned from the scanner.
type item struct {
	typ itemType
	pos int
	val string
}

func (i item) String() string {
	switch {
	case i.typ == itemEOF:
		return "EOF"
	case i.typ == itemError:
		return i.val
	case i.typ == itemID:
		return fmt.Sprintf("[%s]", i.val)
	case i.typ > itemKeyword:
		return fmt.Sprintf("<%s>", i.val)
	case len(i.val) > 10:
		return fmt.Sprintf("%.10q...", i.val)
	}
	return fmt.Sprintf("%q %d", i.val, i.typ)
}

const (
	itemError itemType = iota // error occurred; value is text of error
	itemEOF
	itemSpace // run of spaces separating arguments
	itemMinus
	itemLP
	itemRP
	itemSemi
	itemPlus
	itemStar
	itemSlash
	itemRem
	itemEq
	itemLT
	itemLE
	itemNE
	itemLShift
	itemGT
	itemGE
	itemRShift
	itemConcat
	itemBitOr
	itemComma
	itemBitAnd
	itemBitNot
	itemString
	itemID
	itemVariable
	itemBlob
	itemDot
	itemInteger
	itemFloat
	// Keywords appear after all the rest.
	itemKeyword // used only to delimit the keywords
	itemAbort
	itemAction
	itemAdd
	itemAfter
	itemAll
	itemAlter
	itemAnalyze
	itemAnd
	itemAs
	itemAsc
	itemAttach
	itemAutoincr
	itemBefore
	itemBegin
	itemBetween
	itemBy
	itemCascade
	itemCase
	itemCast
	itemCheck
	itemCollate
	itemColumnKw
	itemCommit
	itemConflict
	itemConstraint
	itemCreate
	itemCtimeKw
	itemDatabase
	itemDefault
	itemDeferrable
	itemDeferred
	itemDelete
	itemDesc
	itemDetach
	itemDistinct
	itemDrop
	itemEach
	itemElse
	itemEnd
	itemEscape
	itemExcept
	itemExclusive
	itemExists
	itemExplain
	itemFail
	itemFor
	itemForeign
	itemFrom
	itemGroup
	itemHaving
	itemIf
	itemIgnore
	itemImmediate
	itemIn
	itemIndex
	itemIndexed
	itemInitially
	itemInsert
	itemInstead
	itemIntersect
	itemInto
	itemIs
	itemIsnull
	itemJoin
	itemJoinKw
	itemKey
	itemLikeKw
	itemLimit
	itemMatch
	itemNo
	itemNot
	itemNotNull
	itemNull
	itemOf
	itemOffset
	itemOn
	itemOr
	itemOrder
	itemPlan
	itemPragma
	itemPrimary
	itemQuery
	itemRaise
	itemRecursive
	itemReferences
	itemReindex
	itemRelease
	itemRename
	itemReplace
	itemRestrict
	itemRollback
	itemRow
	itemSavepoint
	itemSelect
	itemSet
	itemTable
	itemTemp
	itemThen
	itemTo
	itemTransaction
	itemTrigger
	itemUnion
	itemUnique
	itemUpdate
	itemUsing
	itemVacuum
	itemValues
	itemView
	itemVirtual
	itemWhen
	itemWhere
	itemWith
	itemWithout
)

const eof = -1

var key = map[string]itemType{
	"ABORT":             itemAbort,
	"ACTION":            itemAction,
	"ADD":               itemAdd,
	"AFTER":             itemAfter,
	"ALL":               itemAll,
	"ALTER":             itemAlter,
	"ANALYZE":           itemAnalyze,
	"AND":               itemAnd,
	"AS":                itemAs,
	"ASC":               itemAsc,
	"ATTACH":            itemAttach,
	"AUTOINCREMENT":     itemAutoincr,
	"BEFORE":            itemBefore,
	"BEGIN":             itemBegin,
	"BETWEEN":           itemBetween,
	"BY":                itemBy,
	"CASCADE":           itemCascade,
	"CASE":              itemCase,
	"CAST":              itemCast,
	"CHECK":             itemCheck,
	"COLLATE":           itemCollate,
	"COLUMN":            itemColumnKw,
	"COMMIT":            itemCommit,
	"CONFLICT":          itemConflict,
	"CONSTRAINT":        itemConstraint,
	"CREATE":            itemCreate,
	"CROSS":             itemJoinKw,
	"CURRENT_DATE":      itemCtimeKw,
	"CURRENT_TIME":      itemCtimeKw,
	"CURRENT_TIMESTAMP": itemCtimeKw,
	"DATABASE":          itemDatabase,
	"DEFAULT":           itemDefault,
	"DEFERRED":          itemDeferred,
	"DEFERRABLE":        itemDeferrable,
	"DELETE":            itemDelete,
	"DESC":              itemDesc,
	"DETACH":            itemDetach,
	"DISTINCT":          itemDistinct,
	"DROP":              itemDrop,
	"END":               itemEnd,
	"EACH":              itemEach,
	"ELSE":              itemElse,
	"ESCAPE":            itemEscape,
	"EXCEPT":            itemExcept,
	"EXCLUSIVE":         itemExclusive,
	"EXISTS":            itemExists,
	"EXPLAIN":           itemExplain,
	"FAIL":              itemFail,
	"FOR":               itemFor,
	"FOREIGN":           itemForeign,
	"FROM":              itemFrom,
	"FULL":              itemJoinKw,
	"GLOB":              itemLikeKw,
	"GROUP":             itemGroup,
	"HAVING":            itemHaving,
	"IF":                itemIf,
	"IGNORE":            itemIgnore,
	"IMMEDIATE":         itemImmediate,
	"IN":                itemIn,
	"INDEX":             itemIndex,
	"INDEXED":           itemIndexed,
	"INITIALLY":         itemInitially,
	"INNER":             itemJoinKw,
	"INSERT":            itemInsert,
	"INSTEAD":           itemInstead,
	"INTERSECT":         itemIntersect,
	"INTO":              itemInto,
	"IS":                itemIs,
	"ISNULL":            itemIsnull,
	"JOIN":              itemJoin,
	"KEY":               itemKey,
	"LEFT":              itemJoinKw,
	"LIKE":              itemLikeKw,
	"LIMIT":             itemLimit,
	"MATCH":             itemMatch,
	"NATURAL":           itemJoinKw,
	"NO":                itemNo,
	"NOT":               itemNot,
	"NOTNULL":           itemNotNull,
	"NULL":              itemNull,
	"OF":                itemOf,
	"OFFSET":            itemOffset,
	"ON":                itemOn,
	"OR":                itemOr,
	"ORDER":             itemOrder,
	"OUTER":             itemJoinKw,
	"PLAN":              itemPlan,
	"PRAGMA":            itemPragma,
	"PRIMARY":           itemPrimary,
	"QUERY":             itemQuery,
	"RAISE":             itemRaise,
	"RECURSIVE":         itemRecursive,
	"REFERENCES":        itemReferences,
	"REGEXP":            itemLikeKw,
	"REINDEX":           itemReindex,
	"RELEASE":           itemRelease,
	"RENAME":            itemRename,
	"REPLACE":           itemReplace,
	"RESTRICT":          itemRestrict,
	"RIGHT":             itemJoinKw,
	"ROLLBACK":          itemRollback,
	"ROW":               itemRow,
	"SAVEPOINT":         itemSavepoint,
	"SELECT":            itemSelect,
	"SET":               itemSet,
	"TABLE":             itemTable,
	"TEMP":              itemTemp,
	"TEMPORARY":         itemTemp,
	"THEN":              itemThen,
	"TO":                itemTo,
	"TRANSACTION":       itemTransaction,
	"TRIGGER":           itemTrigger,
	"UNION":             itemUnion,
	"UNIQUE":            itemUnique,
	"UPDATE":            itemUpdate,
	"USING":             itemUsing,
	"VACUUM":            itemVacuum,
	"VALUES":            itemValues,
	"VIEW":              itemView,
	"VIRTUAL":           itemVirtual,
	"WITH":              itemWith,
	"WITHOUT":           itemWithout,
	"WHEN":              itemWhen,
	"WHERE":             itemWhere,
}

// stateFn represents the state of the scanner as a function that returns the next state.
type stateFn func(*lexer) stateFn

type lexer struct {
	input string    // the input being scanned.
	items chan item // channel of scanned items.
	pos   int       // current position in the line
	start int       // start position of this item
	width int       // width of last rune read from input
}

// next returns the next rune in the input.
func (l *lexer) next() rune {
	if l.pos >= len(l.input) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.input[l.pos:])
	l.width = w
	l.pos += l.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (l *lexer) backup() {
	l.pos -= l.width
}

// emit passes an item back to the client.
func (l *lexer) emit(t itemType) {
	l.items <- item{t, l.start, l.input[l.start:l.pos]}
	l.start = l.pos
}

// ignore skips over the pending input before this point.
func (l *lexer) ignore() {
	l.start = l.pos
}

// errorf returns an error token and terminates the scan by passing
// back a nil pointer that will be the next state, terminating l.Nextitem.
func (l *lexer) errorf(format string, args ...interface{}) stateFn {
	l.items <- item{itemError, l.start, fmt.Sprintf(format, args...)}
	return nil
}

// nextItem returns the next item from the input.
func (l *lexer) nextItem() item {
	return <-l.items
}

// lex creates a new scanner for the input string.
func lex(input string) *lexer {
	l := &lexer{
		input: input,
		items: make(chan item),
	}
	go l.run()
	return l
}

// run runs the state machine for the lexer.
func (l *lexer) run() {
	for state := lexStart; state != nil; {
		state = state(l)
	}
}

func lexSpace(l *lexer) stateFn {
	for {
		r := l.next()
		if !isSpace(r) {
			break
		}
	}
	l.backup()
	l.emit(itemSpace)
	return lexStart
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\f' || r == '\v'
}

func lexLineComment(l *lexer) stateFn {
	for {
		r := l.next()
		if r == eof || r == '\n' {
			break
		}
	}
	l.emit(itemSpace)
	return lexStart
}

func lexBlockComment(l *lexer) stateFn {
	var pr rune
	for {
		r := l.next()
		if r == eof || (pr == '*' && r == '/') {
			break
		}
		pr = r
	}
	l.emit(itemSpace)
	return lexStart
}

func lexQuote(delim rune) stateFn {
	return func(l *lexer) stateFn {
		var pr rune
		for {
			r := l.next()
			if r == eof {
				return l.errorf("unterminated quoted string")
			}
			if r == delim && pr != delim {
				break
			}
			pr = r
		}
		if delim == '\'' {
			l.emit(itemString)
			return lexStart
		}
		l.emit(itemID)
		return lexStart
	}
}

func lexBracket(l *lexer) stateFn {
	for {
		r := l.next()
		if r == eof {
			return l.errorf("unterminated bracketed identifier")
		}
		if r == ']' {
			break
		}
	}
	l.emit(itemID)
	return lexStart
}

func lexBlobLiteral(l *lexer) stateFn {
	n := 0
	for {
		r := l.next()
		if ('0' <= r && r <= '9') || ('a' <= r && r <= 'f') || ('A' <= r && r <= 'F') { // isxdigit
			n++
			continue
		}
		if r != '\'' || n%2 != 0 {
			// TODO consume until '\''
			return l.errorf("malformed blob literal")
		}
		break
	}
	l.emit(itemBlob)
	return lexStart
}

func lexHexInteger(l *lexer) stateFn {
	n := 0
	for {
		r := l.next()
		if ('0' <= r && r <= '9') || ('a' <= r && r <= 'f') || ('A' <= r && r <= 'F') { // isxdigit
			continue
		}
		if n == 0 {
			return l.errorf("malformed hex integer")
		}
		break
	}
	l.backup()
	l.emit(itemInteger)
	return lexStart
}

func idChar(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || r == '_' || (r >= 'a' && r <= 'z') || r > 127
}

func lexStart(l *lexer) stateFn {
	r := l.next()
	switch r {
	case eof:
		l.emit(itemEOF)
		return nil
	case ' ', '\t', '\n', '\f', '\r':
		return lexSpace
	case '-':
		if l.peek() == '-' {
			return lexLineComment
		}
		l.emit(itemMinus)
		return lexStart
	case '(':
		l.emit(itemLP)
		return lexStart
	case ')':
		l.emit(itemRP)
		return lexStart
	case ';':
		l.emit(itemSemi)
		return lexStart
	case '+':
		l.emit(itemPlus)
		return lexStart
	case '*':
		l.emit(itemStar)
		return lexStart
	case '/':
		if l.peek() == '*' {
			l.next()
			return lexBlockComment
		}
		l.emit(itemSlash)
		return lexStart
	case '%':
		l.emit(itemRem)
		return lexStart
	case '=':
		if l.peek() == '=' {
			l.next()
		}
		l.emit(itemEq)
		return lexStart
	case '<':
		it := itemLT
		switch l.peek() {
		case '=':
			it = itemLE
			l.next()
		case '>':
			it = itemNE
			l.next()
		case '<':
			it = itemLShift
			l.next()
		}
		l.emit(it)
		return lexStart
	case '>':
		it := itemGT
		switch l.peek() {
		case '=':
			it = itemGE
			l.next()
		case '>':
			it = itemRShift
			l.next()
		}
		l.emit(it)
		return lexStart
	case '!':
		if l.next() != '=' {
			return l.errorf("illegal rune: %q", r)
		}
		l.emit(itemNE)
		return lexStart
	case '|':
		if l.peek() == '|' {
			l.next()
			l.emit(itemConcat)
			return lexStart
		}
		l.emit(itemBitOr)
		return lexStart
	case ',':
		l.emit(itemComma)
		return lexStart
	case '&':
		l.emit(itemBitAnd)
		return lexStart
	case '~':
		l.emit(itemBitNot)
		return lexStart
	case '`', '\'', '"':
		return lexQuote(r)
	case '.':
		if !unicode.IsDigit(l.peek()) {
			l.emit(itemDot)
			return lexStart
		}
		fallthrough
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if r == '0' {
			p := l.peek()
			if p == 'x' || p == 'X' {
				l.next()
				return lexHexInteger
			}
		}
		for unicode.IsDigit(r) {
			r = l.next()
		}
		it := itemInteger
		if r == '.' {
			r = l.next()
			for unicode.IsDigit(r) {
				r = l.next()
			}
			it = itemFloat
		}
		if r == 'e' || r == 'E' {
			r = l.next()
			if unicode.IsDigit(r) {
				for unicode.IsDigit(r) {
					r = l.next()
				}
			} else if r == '+' || r == '-' {
				r = l.next()
				if unicode.IsDigit(r) {
					for unicode.IsDigit(r) {
						r = l.next()
					}
				} else {
					return l.errorf("bad number")
				}
			} else {
				return l.errorf("bad number")
			}
		}
		if idChar(r) {
			return l.errorf("bad number")
		}
		l.backup()
		l.emit(it)
		return lexStart
	case '[':
		return lexBracket
	case '?':
		for nr := l.next(); '0' <= nr && nr <= '9'; nr = l.next() {
		}
		l.backup()
		l.emit(itemVariable)
		return lexStart
	case '$', '@', '#', ':':
		r = l.next()
		if !idChar(r) {
			return l.errorf("bad variable name")
		}
		for idChar(r) {
			r = l.next()
		}
		l.backup()
		l.emit(itemVariable)
		return lexStart
	case 'x', 'X':
		if l.peek() == '\'' {
			l.next()
			return lexBlobLiteral
		}
		fallthrough
	default:
		if !idChar(r) {
			break
		}
		for idChar(r) {
			r = l.next()
		}
		l.backup()
		it, ok := key[strings.ToUpper(l.input[l.start:l.pos])]
		if !ok {
			it = itemID
		}
		l.emit(it)
		return lexStart
	}
	return l.errorf("illegal rune: %q", r)
}

func Parse(line string) {
	l := lex(line)

	for {
		item := l.nextItem()
		if item.typ == itemEOF {
			break
		}
		//fmt.Printf("%s\n", item)
	}
}
