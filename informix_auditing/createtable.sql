CREATE TABLE test (
  id         	SERIAL PRIMARY KEY,
  name      	VARCHAR(128),
  longvarchar  	VARCHAR(128),
  longlvarchar  LVARCHAR(128),
  intcol	INTEGER,
  floatcol	FLOAT,
  blobcol	BLOB,	
  clobcol	CLOB(1000)
)PUT blobcol IN (sbsp4);
