#include <stdio.h>

void test(const char** ss) {
    int j;
    for(j=0;;++j) {
        const char* s = ss[j];
        if(s==NULL) break;
        printf("west %s\n",s);
    }
}

typedef void* (*typeboo)(const char* const*,const char* const*,int);

void* wrapperthing(typeboo PQconnectdbParams, int (*status)(void*), const char** names, const char** values, int expand, int verbosity) {
    printf("names\n");
    test(names);
    printf("values\n");
    test(values);
    void* ret = PQconnectdbParams(names,values,expand);
    printf("Returns %p\n",ret);
    printf("STatus %d\n",status(ret));
    return ret;
}

void setthingy(void (*call)(void*,int), void* conn, int verbosity) {
    printf("Verbo %p %d\n",conn,verbosity);
    call(conn,verbosity);
    puts("yay?\n");
}

