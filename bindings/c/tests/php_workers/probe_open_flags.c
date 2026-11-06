#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>

typedef struct sqlite3 sqlite3;
int sqlite3_open_v2(const char *filename, sqlite3 **db, int flags, const char *vfs) {
    typedef int (*open_fn)(const char *, sqlite3 **, int, const char *);
    open_fn original = (open_fn)dlsym(RTLD_NEXT, "sqlite3_open_v2");
    if (!original) return 21;
    fprintf(stderr, "OPEN_FLAGS=0x%x PATH=%s\n", flags, filename);
    return original(filename, db, flags, vfs);
}
void *dlopen(const char *path, int flags) {
    typedef void *(*open_fn)(const char *, int);
    open_fn original = (open_fn)dlsym(RTLD_NEXT, "dlopen");
    return original(path, flags & ~RTLD_DEEPBIND);
}
