-- (c) Copyright IBM Corp. 2004  All rights reserved.                 */
--                                                                    */
-- This sample program is owned by International Business Machines    */
-- Corporation or one of its subsidiaries ("IBM") and is copyrighted  */
-- and licensed, not sold.                                            */
--                                                                    */
-- You may copy, modify, and distribute this sample program in any    */
-- form without payment to IBM,  for any purpose including developing,*/
-- using, marketing or distributing programs that include or are      */
-- derivative works of the sample program.                            */
--                                                                    */
-- The sample program is provided to you on an "AS IS" basis, without */
-- warranty of any kind.  IBM HEREBY  EXPRESSLY DISCLAIMS ALL         */
-- WARRANTIES EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO*/
-- THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTIC-*/
-- ULAR PURPOSE. Some jurisdictions do not allow for the exclusion or */
-- limitation of implied warranties, so the above limitations or      */
-- exclusions may not apply to you.  IBM shall not be liable for any  */
-- damages you suffer as a result of using, modifying or distributing */
-- the sample program or its derivatives.                             */
--                                                                    */
-- Each copy of any portion of this sample program or any derivative  */
-- work,  must include a the above copyright notice and disclaimer of */
-- warranty.                                                          */
--                                                                    */
-- ********************************************************************/

EXECUTE FUNCTION set_tracing("logger", 99, "/tmp/logging.out");
{
-- Example of SELECT trigger
CREATE TRIGGER custseltrig SELECT OF name ON test 
REFERENCING OLD AS pre
FOR EACH ROW (INSERT INTO auditTable VALUES(0, USER, pre.lname));
}

CREATE TRIGGER custinstrig INSERT ON test 
FOR EACH ROW (EXECUTE PROCEDURE do_auditing1() );

CREATE TRIGGER custupdtrig UPDATE ON test 
FOR EACH ROW (EXECUTE PROCEDURE do_auditing1() );

CREATE TRIGGER custdeltrig DELETE ON test 
FOR EACH ROW (EXECUTE PROCEDURE do_auditing1() );

INSERT INTO test  
VALUES (0, 'name', 'Very long text 1234567890123456789012345678901234567890-----------01234567890',
        'Long lvarchar 111111222222223333344444455556666777778888899990',
	5, 10.03,  FILETOBLOB("/tmp/test.log", "server"), FILETOCLOB("/tmp/test.log", "server"));

SELECT * FROM test WHERE name = 'name';

UPDATE test set intcol = 3 WHERE name = 'name';

DELETE FROM test where name = 'name'; 

DROP TRIGGER custinstrig;
DROP TRIGGER custupdtrig;
DROP TRIGGER custdeltrig;
