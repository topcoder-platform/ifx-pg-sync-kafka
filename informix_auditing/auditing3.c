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
 * Auditing function that records the changes inan external file.
 * It saves the records in named memory. A callback function is 
 * called at the end of the transaction and, if it is a commit,
 * records the audit to a file.
 * The difference with do_auditing2 is that we call a Java UDR
 * to do the writing to files.
 */
#include "audit_util.h"

#define LOGGERFILEPREFIX "/tmp/audit"

typedef struct chains {
  mi_integer seq;
  mi_string *xml;
  struct chains *next;
} chains_t;

typedef struct NamedMemory {
  mi_integer gothandle, sessionId, sequence;
  chains_t *operations; /* insert, update, delete */
} NamedMemory_t;

MI_CALLBACK_STATUS MI_PROC_CALLBACK
  cbfunc2(MI_EVENT_TYPE event_type, MI_CONNECTION *conn,
				  void *event_data, void *user_data);
/*--------------------------------------------------------------*/
void do_auditing3(MI_FPARAM *fp)
{
  MI_CONNECTION *sessionConnection;
  MI_CALLBACK_HANDLE *cbhandle;
  mi_integer trigger_operation, sessionId;
  NamedMemory_t *pmem;
  chains_t *curChain;
  mi_string buffer[32], *pdata;

  DPRINTF("logger", 80, ("Entering do_auditing2()"));
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

  sessionConnection = mi_get_session_connection();
  sessionId = mi_get_id(sessionConnection, MI_SESSION_ID);
  /* Retrieve or create session memory */
  sprintf(buffer, "logger%d", sessionId);
  if (MI_OK != mi_named_get(buffer, PER_SESSION, (void **)&pmem)) {
	/* wasn't there, allocate it */
    if (MI_OK != mi_named_zalloc(sizeof(NamedMemory_t), buffer,
				PER_SESSION, (void **)&pmem)) {
      mi_db_error_raise(NULL, MI_EXCEPTION,
			"Logger memory allocation error", NULL);
    }
    /* initialize the values */
    pmem->sessionId = sessionId;
    pmem->gothandle = 0;
    pmem->sequence = 0;
	pmem->operations = NULL;
  }
  /* allocate a block for the row */
  curChain = mi_dalloc(sizeof(chains_t), PER_SESSION);
  curChain->seq = pmem->sequence++;
  curChain->next = NULL;
  if (pmem->operations == NULL) {
	pmem->operations = curChain;
  } else {
    curChain->next = pmem->operations;
	pmem->operations = curChain;
  }
  /* Call the appropriate function */
  switch (trigger_operation) {
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
  /* Write the record in the allocated block */
  curChain->xml = mi_dalloc(strlen(pdata) + 1, PER_SESSION);
  strcpy(curChain->xml, pdata);

  /* Prep a session-duration memory block and copy all the info */
  /* register the callback */
  if (pmem->gothandle == 0) {
	  cbhandle = mi_register_callback(NULL, MI_EVENT_END_XACT, cbfunc2,
					  (void *)pmem, NULL);
	  if (cbhandle == NULL)
		mi_db_error_raise(NULL, MI_EXCEPTION,
						"Callback registration failed", NULL);
      pmem->gothandle = 1;
  }
  DPRINTF("logger", 80, ("Exiting do_auditing2()"));
  return;
}
/*--------------------------------------------------------------*/
MI_CALLBACK_STATUS MI_PROC_CALLBACK
  cbfunc2(MI_EVENT_TYPE event_type, MI_CONNECTION *conn,
				  void *event_data, void *user_data)
{
  MI_FUNC_DESC *fn;
  NamedMemory_t *pmem;
  chains_t *p, *ptmp, *pcur;
  mi_integer i, fd, ret;
  mi_string buffer[80];

  DPRINTF("logger", 80, ("Entering cbfunc()"));
  pmem = (NamedMemory_t *)user_data;
  /* Check the type of transaction */
  if (event_type == MI_EVENT_END_XACT) {
	mi_integer change_type;
	change_type = mi_transition_type(event_data);
	switch(change_type) {
	  case MI_NORMAL_END:
        DPRINTF("logger", 80, ("cbfunc(): MI_NORMAL_END"));
		/* write out all the records */
		if (pmem->operations == NULL) {
          DPRINTF("logger", 80, ("cbfunc(): operations chain is empty"));
		}
		fn = mi_routine_get(conn, 0, "writeFile(lvarchar, lvarchar)");
		for (p = pmem->operations; p != NULL;) {
			if (p->next != NULL) {
			  ptmp = p->next->next;
			  pcur = p->next;
			  p->next = ptmp;
			} else {
			  pcur = p;
			  p  = 0;
			}
			/* write the record out */
			if (pcur == NULL) {
DPRINTF("logger", 80, ("cbfunc(): pcur is null"));
            } else {
			  sprintf(buffer, "%s%d_%d.xml", LOGGERFILEPREFIX,
					pmem->sessionId, pcur->seq);
DPRINTF("logger", 80, ("cbfunc(): about to open file %s", buffer));
			  if (pcur->xml == NULL) {
DPRINTF("logger", 80, ("cbfunc(): pcur->xml is null"));
              } else {
			  (void) mi_routine_exec(conn, fn, &ret, buffer, pcur->xml);
			  mi_free(pcur->xml);
			  }
			  mi_free(pcur);
			}
		} /* end for chain */
		mi_routine_end(conn, fn);
		pmem->operations = 0;
		break;
	  case MI_ABORT_END:
        DPRINTF("logger", 80, ("cbfunc(): MI_ABORT_END"));
		/* get rid of all the records */
		if (pmem->operations == NULL) {
          DPRINTF("logger", 80, ("cbfunc(): operations chain is empty"));
		}
		  for (p = pmem->operations; p != NULL;) {
			if (p->next != NULL) {
			  ptmp = p->next->next;
			  mi_free((void *)p->next->xml);
			  mi_free((void *)p->next);
			  p->next = ptmp;
			} else {
			  mi_free((void *)p->xml);
			  mi_free((void *)p);
			  p = 0;
			}
		  } /* end for */
		  pmem->operations = 0;
		break;
	  default:
		sprintf(buffer, "cbfunc: hit the default with change_type = %d", change_type);
        DPRINTF("logger", 80, (buffer));
		break;
	} /* end switch */
  } else {
	/* Other event types ? */
		sprintf(buffer, "cbfunc: hit the else part with event_type = %d", event_type);
    DPRINTF("logger", 80, (buffer));
  }
  pmem->gothandle = 0;
  DPRINTF("logger", 80, ("Exiting cbfunc()"));
  return(MI_CB_CONTINUE);
}
