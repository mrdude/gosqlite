// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include <sqlite3.h>
#include <string.h>
#include "_cgo_export.h"

typedef struct goVTab goVTab;

struct goVTab {
  sqlite3_vtab base;
  void *vTab;
};

static int cXInit(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr, int isCreate) {
  void *vTab = goMInit(db, pAux, argc, (char**)argv, pzErr, isCreate);
  if (!vTab || *pzErr) {
    return SQLITE_ERROR;
  }
  goVTab *pvTab = (goVTab *)sqlite3_malloc(sizeof(goVTab));
  if (!pvTab) {
    *pzErr = sqlite3_mprintf("%s", "Out of memory");
    return SQLITE_NOMEM;
  }
  memset(pvTab, 0, sizeof(goVTab));
  pvTab->vTab = vTab;

  *ppVTab = (sqlite3_vtab *)pvTab;
  *pzErr = NULL;
  return SQLITE_OK;
}

static int cXCreate(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr) {
  return cXInit(db, pAux, argc, argv, ppVTab, pzErr, 1);
}
static int cXConnect(sqlite3 *db, void *pAux, int argc, const char *const*argv, sqlite3_vtab **ppVTab, char **pzErr) {
	return cXInit(db, pAux, argc, argv, ppVTab, pzErr, 0);
}

static int cXBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *info) {
  // TODO
	return SQLITE_OK;
}

static int cXRelease(sqlite3_vtab *pVTab, int isDestroy) {
  char *pzErr = goVRelease(((goVTab*)pVTab)->vTab, isDestroy);
  if (pzErr) {
    if (pVTab->zErrMsg)
      sqlite3_free(pVTab->zErrMsg);
    pVTab->zErrMsg = pzErr;
    return SQLITE_ERROR;
  }
  if (pVTab->zErrMsg)
    sqlite3_free(pVTab->zErrMsg);
  sqlite3_free(pVTab);
  return SQLITE_OK;
}

static int cXDisconnect(sqlite3_vtab *pVTab) {
	return cXRelease(pVTab, 0);
}
static int cXDestroy(sqlite3_vtab *pVTab) {
  return cXRelease(pVTab, 1);
}

typedef struct goVTabCursor goVTabCursor;

struct goVTabCursor {
  sqlite3_vtab_cursor base;
  void *vTabCursor;
};

static int cXOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor) {
  void *vTabCursor = goVOpen(((goVTab*)pVTab)->vTab, &(pVTab->zErrMsg));
  goVTabCursor *pCursor = (goVTabCursor *)sqlite3_malloc(sizeof(goVTabCursor));
  if (!pCursor) {
    return SQLITE_NOMEM;
  }
  memset(pCursor, 0, sizeof(goVTabCursor));
  pCursor->vTabCursor = vTabCursor;
  *ppCursor = (sqlite3_vtab_cursor *)pCursor;
	return SQLITE_OK;
}
static int cXClose(sqlite3_vtab_cursor *pCursor) {
  char *pzErr = goVClose(((goVTabCursor*)pCursor)->vTabCursor);
  if (pzErr) {
    if (pCursor->pVtab->zErrMsg)
      sqlite3_free(pCursor->pVtab->zErrMsg);
    pCursor->pVtab->zErrMsg = pzErr;
    return SQLITE_ERROR;
  }
  sqlite3_free(pCursor);
	return SQLITE_OK;
}
static int cXFilter(sqlite3_vtab_cursor *pCursor, int idxNum, const char *idxStr, int argc, sqlite3_value **argv) {
	return 0;
}
static int cXNext(sqlite3_vtab_cursor *pCursor) {
	return goXNext(pCursor);
}
static int cXEof(sqlite3_vtab_cursor *pCursor) {
	return 0;
}
static int cXColumn(sqlite3_vtab_cursor *pCursor, sqlite3_context *ctx, int i) {
	return 0;
}
static int cXRowid(sqlite3_vtab_cursor *pCursor, sqlite3_int64 *pRowid) {
	return 0;
}

static sqlite3_module goModule = {
  0,                       /* iVersion */
  cXCreate,                /* xCreate - create a table */
  cXConnect,               /* xConnect - connect to an existing table */
  cXBestIndex,             /* xBestIndex - Determine search strategy */
  cXDisconnect,            /* xDisconnect - Disconnect from a table */
  cXDestroy,               /* xDestroy - Drop a table */
  cXOpen,                  /* xOpen - open a cursor */
  cXClose,                 /* xClose - close a cursor */
  cXFilter,                /* xFilter - configure scan constraints */
  cXNext,                  /* xNext - advance a cursor */
  cXEof,                   /* xEof */
  cXColumn,                /* xColumn - read data */
  cXRowid,                 /* xRowid - read data */
// TODO
  0,                       /* xUpdate - write data */
  0,                       /* xBegin - begin transaction */
  0,                       /* xSync - sync transaction */
  0,                       /* xCommit - commit transaction */
  0,                       /* xRollback - rollback transaction */
  0,                       /* xFindFunction - function overloading */
  0,                       /* xRename - rename the table */
  0,                       /* xSavepoint */
  0,                       /* xRelease */
  0                        /* xRollbackTo */
};


int goSqlite3CreateModule(sqlite3 *db, const char *zName, void *pClientData) {
	return sqlite3_create_module_v2(db, zName, &goModule, pClientData, goMDestroy);
}
