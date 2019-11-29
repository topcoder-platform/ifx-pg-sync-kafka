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

-- EXECUTE FUNCTION set_tracing("logger", 99, "/initiative/MyArticles/Code/Auditing/logging.out");
CREATE TRIGGER custinstrig INSERT ON customer
FOR EACH ROW (EXECUTE PROCEDURE do_auditing3() );

CREATE TRIGGER custupdtrig UPDATE ON CUSTOMER
FOR EACH ROW (EXECUTE PROCEDURE do_auditing3() );

CREATE TRIGGER custdeltrig DELETE ON CUSTOMER
FOR EACH ROW (EXECUTE PROCEDURE do_auditing3() );

INSERT INTO customer
VALUES (0, 'Jacques', 'Roy', 'IBM', '4700 South Syracuse',
        'Suite 500', 'Denver', 'CO', '80237', '303-555-1212');

UPDATE customer set company = 'IBM CORP'
WHERE fname = 'Jacques' AND lname = 'Roy';

DELETE FROM customer
WHERE fname = 'Jacques' AND lname = 'Roy';

DROP TRIGGER custinstrig;
DROP TRIGGER custupdtrig;
DROP TRIGGER custdeltrig;
