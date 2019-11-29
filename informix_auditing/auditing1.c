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
 * Auditing function that records the changes in the auditTable table
 *
 * This audit runs within the context of the current transaction.
 * This means that the record disappears if there is a rollback.
 */
#include "audit_util.h"
/*--------------------------------------------------------------*/
void do_auditing1(MI_FPARAM *fp)
{
  MI_CONNECTION *sessionConnection;
  mi_integer trigger_operation, ret, i, len;
  mi_string *pdata, *tabname, *psql;

  DPRINTF("logger", 80, ("Entering do_auditing1()"));

  /* Get the trigger event and make sure we are in a trigger */
  trigger_operation = mi_trigger_event();
  if (trigger_operation & MI_TRIGGER_NOT_IN_EVENT) {
    /* not in a trigger! generate an exception */
    mi_db_error_raise(NULL, MI_EXCEPTION, 
      "do_auditing1() can only be called within a trigger!", NULL);
	return;
  }
  /* Make sure this is in a FOR EACH type of trigger */
  if (0 == (trigger_operation & MI_TRIGGER_FOREACH_EVENT) ) {
    /* not in a for each trigger! generate an exception */
    mi_db_error_raise(NULL, MI_EXCEPTION, 
      "do_auditing1() must be in a FOR EACH trigger operation", NULL);
	return;
  }
  /* keep only the SQL operation */
  trigger_operation &= (MI_TRIGGER_INSERT_EVENT | MI_TRIGGER_UPDATE_EVENT |
				  MI_TRIGGER_DELETE_EVENT | MI_TRIGGER_SELECT_EVENT);

  /* Call the appropriate function */
  switch (trigger_operation) {
    case MI_TRIGGER_SELECT_EVENT:
	  pdata = doSelectCN();
	  break;
    case MI_TRIGGER_INSERT_EVENT:
	  pdata = doInsertCN();
	  break;
    case MI_TRIGGER_UPDATE_EVENT:
	  pdata = doUpdateCN();
	  break;
    case MI_TRIGGER_DELETE_EVENT:
	  pdata = doDeleteCN();
	  break;
    default: /* Only SELECT left, ignore it */
	  break;
  }
  /* Write the audit trace to the auditTable table */
  psql = (mi_string *)mi_alloc(strlen(pdata) + 80);
  tabname = mi_trigger_tabname(MI_TRIGGER_CURRENTTABLE | MI_TRIGGER_TABLENAME);
  fixname(tabname);
  sprintf(psql, "INSERT INTO auditTable VALUES(0, '%s', '%s')",
    tabname, pdata);
  DPRINTF("logger", 98, (psql));
  sessionConnection = mi_get_session_connection();
  ret = mi_exec(sessionConnection, psql, MI_QUERY_NORMAL);
  mi_free(psql);
  mi_free(pdata);
  DPRINTF("logger", 80, ("Exiting do_auditing1()"));
  return;
}
