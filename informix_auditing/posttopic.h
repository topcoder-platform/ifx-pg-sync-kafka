#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <curl/curl.h>
char* concat(const char *s1, const char *s2);
char* gettimestamp();
int posttopic(char *jsondata);



