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
 * Include file for utility functions
 */
#include <string.h>
#include <mi.h>
#include <minmdur.h>
#include <minmmem.h>
#include <miami.h>
#include <fcntl.h>

#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <stdio.h>
#include <curl/curl.h>

#define BUFSIZE 29900

void fixname(mi_string *in);
mi_string *do_castl(MI_CONNECTION *conn, MI_DATUM *datum,
                   MI_TYPEID *tid, MI_TYPEID *lvar_id, mi_integer collen);
mi_string *doSelectCN();
mi_string *doInsertCN();
mi_string *doDeleteCN();
mi_string *doUpdateCN();
mi_integer set_tracing(mi_lvarchar *class, mi_integer lvl,
                       mi_lvarchar *tfile, MI_FPARAM *fparam);
char* gettimestamp();
int posttopic(char *jsondata);                       
