#include "posttopic.h"
char* concat(const char *s1, const char *s2)
{
    char *result = malloc(strlen(s1) + strlen(s2) + 1); // +1 for the null-terminator
    // in real code you would check for errors in malloc here
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}
char* gettimestamp()
{
  char timebuffer[30];
  struct timeval tv;
  char timedate[30];
  time_t curtime;
  gettimeofday(&tv, NULL); 
  curtime=tv.tv_sec;
  strftime(timebuffer,30,"%Y-%m-%dT%T.",localtime(&curtime));
  //printf("%s%ld\n",buffer,tv.tv_usec);
  sprintf(timedate,"%s%ldZ",timebuffer,tv.tv_usec);
  char *returnstr = malloc(strlen(timedate) + 1);
  strcpy(returnstr,timedate);
  //printf("%s",timedate);
  return returnstr;
}

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



