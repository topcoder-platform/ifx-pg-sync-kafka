CREATE TABLE testtable (
    dserial8 serial8,    
    dchar char(1),
    dvarchar varchar(255),
    dlvarchar lvarchar(20000),
    ddecimal decimal(12,0),
    dfloat float,
    dint8 int8,
    dinteger integer,
    dmoney money(16,2),
    dnchar nchar,
    dnvarchar nvarchar,
    dsmallint smallint
);

CREATE TRIGGER informix.testtable_trigger_insert insert on "informix".testtable    
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );


CREATE TABLE testtable1 (
    dserial serial,    
    dlvarchar lvarchar(20000)
);  

CREATE TABLE testtable2 (
    dbigserial bigserial,    
    dlvarchar lvarchar(20000)
);  


CREATE TRIGGER informix.testtable1_trigger_insert insert on "informix".testtable1 
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );
CREATE TRIGGER informix.testtable12_trigger_insert insert on "informix".testtable2   
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );    

CREATE TABLE testtable3 ( 
    dlvarchar lvarchar(20000),
    ddate date
); 

CREATE TABLE testtable4 ( 
  dlvarchar lvarchar,
  ddatetime DATETIME YEAR TO SECOND
);

CREATE TRIGGER informix.testtable3_trigger_insert insert on "informix".testtable3 
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );
CREATE TRIGGER informix.testtable14_trigger_insert insert on "informix".testtable4   
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );

CREATE TABLE testtable5 (
    dserial8 serial8,    
    dchar char(1),
    dvarchar varchar(255),
    dlvarchar lvarchar(20000),
    ddecimal decimal(12,0),
    dfloat float,
    dint8 int8,
    dinteger integer,
    dmoney money(16,2),
    dnchar nchar,
    dnvarchar nvarchar,
    dsmallint smallint,
    ddate date,
    ddatetime DATETIME YEAR TO SECOND
);

CREATE TRIGGER informix.testtable5_trigger_insert insert on "informix".testtable5 
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );

CREATE TRIGGER informix.testtable5_trigger_update update on "informix".testtable5 
for each row
    (
        execute procedure "informix".do_auditing2(USER)
    );
