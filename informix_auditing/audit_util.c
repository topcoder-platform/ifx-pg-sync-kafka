/*(c) Copyright IBM Corp. 2004  All rights reserved.                 */
/*                                                                   */
/*This sample program is owned by International Business Machines    */
/*Corporation or one of its subsidiaries ("IBM") and is copyrighted  */
/*and licensed, not sold.                                            */
/*                                                                   */
/*You may copy, modify, and distribute this sample program in any    */
/*form without payment to IBM,  for any purpose including developing,*/
/*using, marketing or distributing programs that include or are      */
/*derivative works of the sample program.                            */
/*                                                                   */
/*The sample program is provided to you on an "AS IS" basis, without */
/*warranty of any kind.  IBM HEREBY  EXPRESSLY DISCLAIMS ALL         */
/*WARRANTIES EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO*/
/*THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTIC-*/
/*ULAR PURPOSE. Some jurisdictions do not allow for the exclusion or */
/*limitation of implied warranties, so the above limitations or      */
/*exclusions may not apply to you.  IBM shall not be liable for any  */
/*damages you suffer as a result of using, modifying or distributing */
/*the sample program or its derivatives.                             */
/*                                                                   */
/*Each copy of any portion of this sample program or any derivative  */
/*work,  must include a the above copyright notice and disclaimer of */
/*warranty.                                                          */
/*                                                                   */
/*********************************************************************/

/*
 * Utility function to generate XML output.
 */
#include "audit_util.h"
/*--------------------------------------------------------------*/
void fixname(mi_string *in)
{
  int i, j;

  j = strlen(in);
  for (i = 0; i < j; i++) {
	if (in[i] < ' ')
	  in[i] = 0;
  }
  return;
}

mi_string *do_castl(MI_CONNECTION *conn, MI_DATUM *datum,
                   MI_TYPEID *tid, MI_TYPEID *lvar_id, mi_integer collen)
{
  MI_FUNC_DESC 	*fn;
  MI_FPARAM    	*fp;
  MI_DATUM     	new_datum;
  MI_TYPEID    	*ftid;
  MI_TYPE_DESC  *td;
  mi_integer   	i;
  mi_integer   	ret;
  mi_char      	status, *pbuf;

  //ADDED 
  MI_TYPEID	*typeid;
  MI_TYPE_DESC 	*tdesc;
  mi_integer	precision;
  
 /* -------------------------------------------- */

    /* Decide SRC-TYPE from tid */
    MI_TYPE_DESC* dsc = mi_type_typedesc(conn, tid);
    mi_string* srcType = mi_type_typename(dsc);
    DPRINTF("logger",95,("-- typeName=%s --",srcType));
    printf("-- typeName=%s --",srcType);
    if ((strcmp("blob",   srcType) == 0) || (strcmp("clob",   srcType) == 0) || (strcmp("text",   srcType) == 0) || (strcmp("byte",   srcType) == 0)) {
            return("unsupportedtype");
     }
   else{ 
   if (strcmp("date",   srcType) == 0) {
     return (mi_date_to_string((mi_date *)datum));
   }
   if (strcmp("datetime",   srcType) == 0) {
    return (mi_datetime_to_string((mi_datetime *)datum));
   }
   if ((strcmp("integer",   srcType) == 0) ||
                 (strcmp("bigint",    srcType) == 0) ||
                 (strcmp("int8",      srcType) == 0) ||
                 (strcmp("serial",    srcType) == 0) ||
                 (strcmp("bigserial", srcType) == 0) ||
                 (strcmp("serial8",   srcType) == 0) ||
                 (strcmp("smallint",  srcType) == 0)) {
    collen = 30;
   }
  fn = mi_cast_get(conn, tid, lvar_id,  &status);
  if (NULL == fn) {
    switch(status) {
    case MI_ERROR_CAST:
    case MI_NO_CAST:
    case MI_SYSTEM_CAST:
    case MI_UDR_CAST:
    case MI_IMPLICIT_CAST:
    case MI_EXPLICIT_CAST:
         return("error");
         break;
    case MI_NOP_CAST:
         return (mi_lvarchar_to_string((mi_lvarchar *)datum));
         break;
    } /* end switch */
  }
  fp = mi_fparam_get(conn, fn);
  for (i = 0; i < mi_fp_nargs(fp); i++)
    mi_fp_setargisnull(fp, i, MI_FALSE);
  /* in this case, we know it is int to lvarchar.  It is a system cast */
  /* that is done by dosyscast(datum *, int, int) */
  /* arguments 2 and 3 represent length and precision of the return value */
  typeid = mi_fp_argtype(fp, 0);
  tdesc = mi_type_typedesc(conn, typeid); 
  precision = mi_type_precision(tdesc);

  printf("rputine read initiated \n");
  printf("rputine read initiated %ld\n",collen);                   
  new_datum = mi_routine_exec(conn, fn, &ret, datum, collen, precision, fp);
  printf("routine read completed \n");
  pbuf = mi_lvarchar_to_string(new_datum);
  //pbuf = mi_date_to_string((mi_date *)datum);
  //printf("\ndate data %s \n",pbuf);
  mi_routine_end(conn, fn); 
	//return mi_type_typename(mi_type_typedesc(conn, my_type_id));
  }
	
  return(pbuf);
}
/*--------------------------------------------------------------*/
mi_string *doInsertCN()
{
  MI_CONNECTION *conn;
  MI_ROW        *row;
  MI_TYPEID     *tid, *lvarTid;
  MI_TYPE_DESC  *td;
  MI_ROW_DESC   *rd;
  MI_DATUM      datum;
  mi_lvarchar   *lvarret;
  mi_integer    i, len, posi, colCount, collen;
  mi_string     tabname[128], *buffer, *ptabname, *pcolname, *pcast, *pdbname;
  mi_integer	nc;
  char uniquedatatype[10];
  nc = 0;
  conn = mi_get_session_connection();
  /* Get the table name and the row */
  row = mi_trigger_get_new_row();
  rd = mi_get_row_desc(row);
  colCount = mi_column_count(rd);
  tid = mi_rowdesc_typeid(rd);
  lvarTid = mi_typename_to_id(conn, mi_string_to_lvarchar("lvarchar"));
  td = mi_type_typedesc(NULL, tid);
  /* prepare the output buffer */
  buffer = (mi_string *)mi_alloc(BUFSIZE);
  ptabname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_TABLENAME);
  strcpy(tabname, ptabname);
  char *cdatetime = gettimestamp();
  sprintf(buffer, "{\"TIME\": \"%s\", ", cdatetime);
  pdbname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_DBASENAME);
  posi = strlen(buffer);
  //fixname(pdbname);
  sprintf(&buffer[posi], "\"SCHEMANAME\": \"%s\", ", pdbname);     
  posi = strlen(buffer);
  printf("\"TABLENAME\": \"%s\", ", tabname);
  sprintf(&buffer[posi], "\"TABLENAME\": \"%s\", ", tabname);
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"OPERATION\": \"INSERT\", ");       
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"DATA\":  {");
  /* Process each column */
  for (i = 0; i < colCount; i++) {
    /* get column name and type id */
    pcolname = mi_column_name(rd, i);
DPRINTF("logger", 90, ("insert: colname: (0x%x) [%s]", pcolname, pcolname));
    tid = mi_column_type_id(rd, i);
    switch(mi_value(row, i, &datum, &collen)) {
    /* we should do this test */
    case MI_NULL_VALUE:
         break;
    case MI_NORMAL_VALUE:
         pcast = do_castl(conn, datum, tid, lvarTid, collen);
		 posi = strlen(buffer);
	 if( nc == 1) 
   	 {
	   sprintf(&buffer[posi], ", ");
	   posi = strlen(buffer);
	 }	
         sprintf(&buffer[posi], "\"%s\" : \"%s\"", pcolname, escapecharjson(pcast));
          if (strcmp("unsupportedtype",   pcast) == 0)  {
            strcpy(uniquedatatype, "true");
          }  

	 nc = 1;
         break;
    case MI_ROW_VALUE:
         break;
    } /* end switch */
  } /* end for */
  /* close the record */
  posi = strlen(buffer);
  if (strcmp("true",   uniquedatatype) == 0)  {
      sprintf(&buffer[posi], "},  \n \"uniquedatatype\" : \"true\" \n }");
  } else {
      sprintf(&buffer[posi], "},  \n \"uniquedatatype\" : \"false\" \n }");
  }
  free(cdatetime);
 return(buffer);
}
/*--------------------------------------------------------------*/
mi_string *doSelectCN()
{
  MI_CONNECTION *conn;
  MI_ROW        *row;
  MI_TYPEID     *tid, *lvarTid;
  MI_TYPE_DESC  *td;
  MI_ROW_DESC   *rd;
  MI_DATUM      datum;
  mi_lvarchar   *lvarret;
  mi_integer    i, len, posi, colCount, collen;
  mi_string     tabname[128], *buffer, *ptabname, *pcolname, *pcast, *pdbname;
  mi_integer	nc;
	
  nc = 0;
  conn = mi_get_session_connection();
  /* Get the table name and the row */
  row = mi_trigger_get_new_row();
  rd = mi_get_row_desc(row);
  colCount = mi_column_count(rd);
  tid = mi_rowdesc_typeid(rd);
  lvarTid = mi_typename_to_id(conn, mi_string_to_lvarchar("lvarchar"));
  td = mi_type_typedesc(NULL, tid);
  /* prepare the output buffer */
  buffer = (mi_string *)mi_alloc(BUFSIZE);
  ptabname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_TABLENAME);
  strcpy(tabname, ptabname);
  char *cdatetime = gettimestamp();
  sprintf(buffer, "{\"TIME\": \"%s\", ", cdatetime);
  pdbname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_DBASENAME);
  posi = strlen(buffer);
  //fixname(pdbname);
  sprintf(&buffer[posi], "\"SCHEMANAME\": \"%s\", ", pdbname);  
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"TABLENAME\": \"%s\", ", tabname);  
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"OPERATION\": \"SELECT\", ");       
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"DATA\":  {");     
  /* Process each column */
  for (i = 0; i < colCount; i++) {
    /* get column name and type id */
    pcolname = mi_column_name(rd, i);
DPRINTF("logger", 90, ("insert: colname: (0x%x) [%s]", pcolname, pcolname));
    tid = mi_column_type_id(rd, i);
    switch(mi_value(row, i, &datum, &collen)) {
    /* we should do this test */
    case MI_NULL_VALUE:
         break;
    case MI_NORMAL_VALUE:
         pcast = do_castl(conn, datum, tid, lvarTid, collen);
		 posi = strlen(buffer);
	 if( nc == 1)
         {
           sprintf(&buffer[posi], ", ");
           posi = strlen(buffer);
         }
         sprintf(&buffer[posi], "\"%s\" : \"%s\"", pcolname, pcast);
	 nc = 1;
         break;
    case MI_ROW_VALUE:
         break;
    } /* end switch */
  } /* end for */
  /* close the record */
  posi = strlen(buffer);
  sprintf(&buffer[posi], "} }");
  free(cdatetime);
 return(buffer);
}
/*--------------------------------------------------------------*/
mi_string *doDeleteCN()
{
  MI_CONNECTION *conn;
  MI_ROW        *row;
  MI_TYPEID     *tid, *lvarTid;
  MI_TYPE_DESC  *td;
  MI_ROW_DESC   *rd;
  MI_DATUM      datum;
  mi_lvarchar   *lvarret;
  mi_integer    i, len, posi, colCount, collen;
  mi_string     *buffer, *ptabname, *pcolname, *pcast, *pdbname;
  mi_integer	nc;
   char uniquedatatype[10];

  nc = 0;
  conn = mi_get_session_connection();
  /* Get the row */
  row = mi_trigger_get_old_row();
  rd = mi_get_row_desc(row);
  colCount = mi_column_count(rd);
  tid = mi_rowdesc_typeid(rd);
  lvarTid = mi_typename_to_id(conn, mi_string_to_lvarchar("lvarchar"));
  td = mi_type_typedesc(NULL, tid);
  /* prepare the output buffer */
  ptabname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_TABLENAME);
  len = strlen(ptabname);
  fixname(ptabname);
  buffer = (mi_string *)mi_alloc(BUFSIZE);
  char *cdatetime = gettimestamp();
  sprintf(buffer, "{\"TIME\": \"%s\", ", cdatetime);
  pdbname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_DBASENAME);
  posi = strlen(buffer);
  //fixname(pdbname);
  sprintf(&buffer[posi], "\"SCHEMANAME\": \"%s\", ", pdbname);   
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"TABLENAME\": \"%s\", ", ptabname);  
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"OPERATION\": \"DELETE\", ");       
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"DATA\":  {");         
  /* Process each column */
  for (i = 0; i < colCount; i++) {
    /* get column name and type id */
    pcolname = mi_column_name(rd, i);
DPRINTF("logger", 90, ("delete: colname: (0x%x) [%s]", pcolname, pcolname));
    tid = mi_column_type_id(rd, i);
    switch(mi_value(row, i, &datum, &collen)) {
    /* we should do this test */
    case MI_NULL_VALUE:
         break;
    case MI_NORMAL_VALUE:
         pcast = do_castl(conn, datum, tid, lvarTid, collen);
		 posi = strlen(buffer);
	 if( nc == 1)
         {
           sprintf(&buffer[posi], ", ");
           posi = strlen(buffer);
         }
         //printf("%s",pcast);
  
         //pcast = escapecharjson(pcast);
         //printf("%s",pcast);
         sprintf(&buffer[posi], "\"%s\" : \"%s\"", pcolname, escapecharjson(pcast));
          if (strcmp("unsupportedtype",   pcast) == 0)  {
            strcpy(uniquedatatype, "true");
          }
	 nc = 1;
         break;
    case MI_ROW_VALUE:
         break;
    } /* end switch */
  } /* end for */
  /* close the record */
  posi = strlen(buffer);
  fixname(ptabname);
  //sprintf(&buffer[posi], "}  }");
    if (strcmp("true",   uniquedatatype) == 0)  {
      sprintf(&buffer[posi], "},  \n \"uniquedatatype\" : \"true\" \n }");
  } else {
      sprintf(&buffer[posi], "},  \n \"uniquedatatype\" : \"false\" \n }");
  }
 free(cdatetime); 
 return(buffer);
}
/*--------------------------------------------------------------*/
mi_string *doUpdateCN()
{
  MI_CONNECTION *conn;
  MI_ROW        *oldRow, *newRow;
  MI_TYPEID     *tid, *lvarTid;
  MI_TYPE_DESC  *td;
  MI_ROW_DESC   *rdOld, *rdNew;
  MI_DATUM      datum;
  mi_lvarchar   *lvarret;
  mi_integer    i, j, len, posi, colCountOld, colCountNew, collen;
  mi_string     *buffer, *ptabname, *poldcolname, *pnewcolname, *pcast, *pcast2, *pdbname;
  mi_integer    pbufLen;
  mi_integer	nc;
 char uniquedatatype[10];

  nc = 0;
  DPRINTF("logger", 90, ("Entering doUpdateCN()"));
  conn = mi_get_session_connection();
  /* get the rows */
  oldRow = mi_trigger_get_old_row();
  newRow = mi_trigger_get_new_row();
  rdOld = mi_get_row_desc(oldRow);
  rdNew = mi_get_row_desc(newRow);
  colCountOld = mi_column_count(rdOld);
  colCountNew = mi_column_count(rdNew);
  DPRINTF("logger", 90, ("Column count before: %d, after: %d",
			  colCountOld, colCountNew));
  tid = mi_rowdesc_typeid(rdOld);
  lvarTid = mi_typename_to_id(conn, mi_string_to_lvarchar("lvarchar"));
  td = mi_type_typedesc(NULL, tid);

  /* prepare the output buffer */
  ptabname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_TABLENAME);
  len = strlen(ptabname);
  for (i = 0; i < len; i++)
    if (0 == isgraph(ptabname[i])) {
      ptabname[i] = 0;
      DPRINTF("logger", 90, ("Found a non-printable character in tabname"));
	}
  buffer = (mi_string *)mi_alloc(BUFSIZE);
  char *cdatetime = gettimestamp();
  sprintf(buffer, "{\"TIME\": \"%s\", ", cdatetime);
  pdbname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_DBASENAME);
  posi = strlen(buffer);
  //fixname(pdbname);
  sprintf(&buffer[posi], "\"SCHEMANAME\": \"%s\", ", pdbname);  
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"TABLENAME\": \"%s\", ", ptabname);    
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"OPERATION\": \"UPDATE\", ");       
  posi = strlen(buffer);
  sprintf(&buffer[posi], "\"DATA\":  {");        

  /* Process each column */
  j = 0;
  for (i = 0; i < colCountOld; i++) {
    /* get column name and type id */
    poldcolname = mi_column_name(rdOld, i);
    if (j < colCountNew)
      pnewcolname = mi_column_name(rdNew, j);
    tid = mi_column_type_id(rdOld, i);
    switch(mi_value(oldRow, i, &datum, &collen)) {
    /* we should do this test */
    case MI_NULL_VALUE:
		 pcast = "NULL";
         break;
    case MI_NORMAL_VALUE:
         pcast = do_castl(conn, datum, tid, lvarTid, collen);
         break;
    } /* end switch */
    if (0 == strcmp(poldcolname, pnewcolname) ) {
      switch (mi_value(newRow, j, &datum, &collen)) {
		case MI_NULL_VALUE:
		  pcast2 = "NULL";
		  break;
		case MI_NORMAL_VALUE:
		  pcast2 = do_castl(conn, datum, tid, lvarTid, collen);
		  break;
	} /* end switch */
      j++;
    } else {
	pcast2 = pcast;
    }
    pbufLen = strlen(buffer);
    if( nc == 1)
    {
      sprintf(&buffer[pbufLen], ", ");
      pbufLen = strlen(buffer);
    }
    sprintf(&buffer[pbufLen], "\"%s\" : { \"old\" : \"%s\", \"new\" : \"%s\" }", poldcolname, escapecharjson(pcast), escapecharjson(pcast2));
    if (strcmp("unsupportedtype",   pcast2) == 0)  {
            strcpy(uniquedatatype, "true");
      }
     nc = 1;

    pbufLen = strlen(buffer);
  } /* end for */
  pbufLen = strlen(buffer);
  //sprintf(&buffer[pbufLen], "}  }");
    if (strcmp("true",   uniquedatatype) == 0)  {
      sprintf(&buffer[pbufLen], "},  \n \"uniquedatatype\" : \"true\" \n }");
      } else {
      sprintf(&buffer[pbufLen], "},  \n \"uniquedatatype\" : \"false\" \n }");
  }
  DPRINTF("logger", 90, ("Exiting doUpdateCN()"));
  free(cdatetime);
  return(buffer);
}
/*--------------------------------------------------------------*/
mi_integer set_tracing(mi_lvarchar *class, mi_integer lvl,
                       mi_lvarchar *tfile, MI_FPARAM *fparam)
{
  mi_integer   ret;
  mi_string *str, buffer[80];

  /* if there is a trace file provided */
  if (mi_fp_argisnull(fparam, 2) != MI_TRUE) {
    str = mi_lvarchar_to_string(tfile);
    ret = mi_tracefile_set(str);
  }
  /* if both the class and level are not NULL */
  if (mi_fp_argisnull(fparam, 0) != MI_TRUE &&
     (mi_fp_argisnull(fparam, 1)) != MI_TRUE) {
    str = mi_lvarchar_to_string(class);
    sprintf(buffer, "%s %d ", str, lvl);
    ret = mi_tracelevel_set(buffer);
  }
  return ret;
}
/*--------------------------------------------------------------*/
/*-------------------------- */
/*  getting timme */
/*-------------------------- */
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
/*--------------------------------------------------------------*/
/* post topic base don condition*/

int posttopic(char *jsondata, char *posturl)
{
  // char *postinfo = getenv("POSTTOPIC");
 //  char *fileeventsurl = "http://ifxpg-migrator.topcoder-dev.com/fileevents";
 //  char *kafkaeventsurl = "http://ifxpg-migrator.topcoder-dev.com/kafkaevents";
   //char *localurl= "http://host.docker.internal:8080/events";
   //char *localurl= "http://localhost:8080/events";
  /* char *posturl = getenv("POSTURL");
   if (!postinfo)
   {
     printf("no post topic set true or false. defualt it will post topic \n");
    // return 0;
   }
   else
    {

      if (strcmp(postinfo, "false") == 0)
       {
           return 0;
       }
    }
                printf("posting topic \n");
                 if (!posturl)
                    {
                         posturl = fileeventsurl;
                        //posturl = localurl;
                        //printf("PATH : %s\n",ap);
                        printf("no url provide in environment . So it is taking localurl");
                    }
*/                    
                    printf("posting topic to url %s \n", posturl);
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
                       {
                        fprintf(stderr, "curl_easy_perform() failed: %s\n",
                                curl_easy_strerror(ret));  
                       }
  /*                  curl_easy_setopt(hnd, CURLOPT_URL, kafkaeventsurl);
                    if(ret != CURLE_OK)
                       {
                        fprintf(stderr, "curl_easy_perform() failed: %s\n",
                                curl_easy_strerror(ret));  
                       }                    
    */
    return 0;
}

/*--------------------------------------------------------------*/
char * escapecharjson( char *jsonvalue_org)
{
    char *jsonvalue_copy; // first copy the pointer to not change the original
    char *escjsonvalue;
    int posi = 0;
    //char *p = jsonvalue_org;
    //for (; *p != '\0'; p++) {}
    //printf("length of string : %ld",(p - jsonvalue_org));
    //escjsonvalue = (char *)malloc(10000);
    escjsonvalue = (char *)calloc(10000, sizeof(char));
    for (jsonvalue_copy = jsonvalue_org; *jsonvalue_copy != '\0'; jsonvalue_copy++) {

           printf("%c:%d\n", *jsonvalue_copy,*jsonvalue_copy);
			if (*jsonvalue_copy == '"') {
				posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\\"") ;  
			} else if (*jsonvalue_copy == '\t') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\t") ;
			} else if (*jsonvalue_copy == '\f') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\f") ; 
			} else if (*jsonvalue_copy == '\n') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\n") ;
			} else if (*jsonvalue_copy == '\r') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\r") ;
			} else if (*jsonvalue_copy == '\\') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\\\") ;
			} else if (*jsonvalue_copy == '/') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\/") ; 
			} else if (*jsonvalue_copy == '\b') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%s","\\b") ;  
			} else if ('\x00' >= *jsonvalue_copy && *jsonvalue_copy <= '\x1f') {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "\\u%4x",(int)*jsonvalue_copy) ; 
            } else {
                posi = strlen(escjsonvalue);
                sprintf(&escjsonvalue[posi], "%c",*jsonvalue_copy) ;  
			}
			
        
    }
    //p=NULL;
    jsonvalue_copy=NULL;
    printf("%s", escjsonvalue);
        return(escjsonvalue);
    }
