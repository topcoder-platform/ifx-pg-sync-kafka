Fine-Grained Auditing testing function

IDS 9.50.xC4 has added functionality for trigger introspection. This allows
us to write a generic trigger function that can record the changes made
to any table in the database.

This directory implement three versions of this UDR. One version (do_auditing1)
uses a table to log the changes. The other versions (do_auditing2, do_auditing3) uses
SESSION memory to keep track of the rows and a callback writes the
result to files if the transaction was successful.

Both versions should implement more testing on buffer usage. The current
buffer has a max length of 29900 bytes. If the result is larger, we will get
a stack trace due to memory problems.

This directory contains:

README            : This file
WinNT.mak         : Makefile for Windows
do_auditing1.c    : do_auditing1 UDR source code
do_auditing1.sql  : register the do_auditing1 functions
do_auditing1_d.sql: deregister the do_auditing1 functions
do_auditing2.c    : do_auditing2 UDR source code
do_auditing2.sql  : register the do_auditing2 functions
do_auditing2_d.sql: deregister the do_auditing2 functions
do_auditing3.c    : do_auditing3 UDR source code
do_auditing3.sql  : register the do_auditing3 functions
do_auditing3_d.sql: deregister the do_auditing3 functions
RecordAudit.java  : Java procedure used by do_auditing3
tryit.sql         : test the functionality for do_auditing1
                   (writing to table auditTable)
tryit2.sql        : test the functionality for do_auditing2
                    (writing to files in /tmp, names: audit_xx.xml)
tryit2.sql        : test the functionality for do_auditing2
                    (writing to files in /tmp, names: audit_xx.xml)
UNIX.mak          : Generic makefile for Unix platforms

You register do_auditing1, do_auditing2, or do_auditing3 as follows:

 - Execute the Makefile: nmake -f winnt.mak
 - Create $INFORMIX/extend/auditing
 - Execute the Makefile for install: nmake -f winnt.mak install
 - Register do_auditing1, do_auditing2, or do_auditing3:
    . dbaccess -e <dbname> do_auditing1
    . dbaccess -e <dbname> do_auditing2
    . dbaccess -e <dbname> do_auditing3
 - Execute the test script:
    . dbaccess -e <dbname> tryit
    . dbaccess -e <dbname> tryit2
    . dbaccess -e <dbname> tryit3
 - De-register the UDR:
    . dbaccess -e <dbname> do_auditing1_d
    . dbaccess -e <dbname> do_auditing2_d
    . dbaccess -e <dbname> do_auditing3_d

By default, tracing is turned off. you can turn it on in the testit,
testit2, and testit3 scripts. You also need to change the make file to remove
the definition of -DMITRACE_OFF=1 from the compilation.

If you are trying this code on a Unix platform, uncomment and modify line 32 of
the Unix.mak file: uncomment and use your platform-specific include file.

onspaces -c -S sbsp4 -p /tmp/rawdev1 -o 500 -s 20480 -m /tmp/rawdev2 500 -Ms 150 -Mo 200 -Df "AVG_LO_SIZE=32"
