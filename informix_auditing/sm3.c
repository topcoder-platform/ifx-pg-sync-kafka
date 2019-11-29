#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <curl/curl.h>

int posttopic(char *jsondata)
{

   char *postinfo = getenv("POSTTOPIC");
   char *localurl= "http://localhost:8080/events";
   char *posturl = getenv("POSTURL");
   if (!postinfo)
   {
     //printf("no post topic set");
    return 0;
   }
   else
    {
            if (strcmp(postinfo, "true") == 0)
            {
                printf("posting topic");
                 if (!posturl)
                    {
                        posturl = localurl;
                        //printf("PATH : %s\n",ap);
                        printf("no url provide in environment . So it is taking localurl");
                    }
                    printf("%s",posturl);
                    CURL *hnd = curl_easy_init();
                    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, "POST");
                    curl_easy_setopt(hnd, CURLOPT_URL, posturl);
                    struct curl_slist *headers = NULL;
                    headers = curl_slist_append(headers, "cache-control: no-cache");
                    headers = curl_slist_append(headers, "Content-Type: application/json");
                    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, headers);

                    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS,jsondata);

                    CURLcode ret = curl_easy_perform(hnd);
                    if(ret != CURLE_OK)
                        fprintf(stderr, "curl_easy_perform() failed: %s\n",
                                curl_easy_strerror(ret));                      

            }
            else
            {
                     printf(" post topic set as false");
                    return 0;
            }
    }
    return 0;
}

int main () {
   char *ap= getenv("ABC");
   char *data="{\"msg\" : \"welcome\"}";
   int ret=posttopic(data);
   return(0);
}
                            
