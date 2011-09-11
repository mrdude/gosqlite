# Copyright 2010 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=github.com/gwenn/sqlite

CGOFILES=\
	sqlite.go\
	backup.go\
	meta.go\
	trace.go\
	blob.go

GOFILES=\
	date.go

include $(GOROOT)/src/Make.pkg
