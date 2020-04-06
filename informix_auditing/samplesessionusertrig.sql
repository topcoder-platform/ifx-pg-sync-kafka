CREATE PROCEDURE informix.do_auditing2(sessionusername LVARCHAR)

EXTERNAL NAME "$INFORMIXDIR/extend/auditing/auditing.bld(do_auditing2)"
LANGUAGE C;    

CREATE TRIGGER informix.access_trigger_insert insert on "informix".access    for each row
        (
        execute procedure "informix".do_auditing2(USER));
