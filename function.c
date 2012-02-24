#include <sqlite3.h>
#include <stdlib.h>

extern void goXAuxDataDestroy(void *ad);

void goSqlite3SetAuxdata(sqlite3_context *ctx, int N, void *ad) {
	sqlite3_set_auxdata(ctx, N, ad, goXAuxDataDestroy);
}

extern void goXFunc(sqlite3_context *ctx, void *udf, void *goctx, int argc, sqlite3_value **argv);
extern void goXStep(sqlite3_context *ctx, void *udf, int argc, sqlite3_value **argv);
extern void goXFinal(sqlite3_context *ctx, void *udf);
extern void goXDestroy(void *pApp);

static void cXFunc(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	void *udf = sqlite3_user_data(ctx);
	void *goctx = sqlite3_get_auxdata(ctx, 0);
	goXFunc(ctx, udf, goctx, argc, argv);
}

static void cXStep(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	void *udf = sqlite3_user_data(ctx);
	goXStep(ctx, udf, argc, argv);
}

static void cXFinal(sqlite3_context *ctx) {
	void *udf = sqlite3_user_data(ctx);
	goXFinal(ctx, udf);
}

int goSqlite3CreateScalarFunction(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp) {
	return sqlite3_create_function_v2(db, zFunctionName, nArg, eTextRep, pApp, cXFunc, NULL, NULL, goXDestroy);
}
int goSqlite3CreateAggregateFunction(sqlite3 *db, const char *zFunctionName, int nArg, int eTextRep, void *pApp) {
	return sqlite3_create_function_v2(db, zFunctionName, nArg, eTextRep, pApp, NULL, cXStep, cXFinal, goXDestroy);
}