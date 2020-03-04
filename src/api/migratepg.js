const dbs = require('../models')
const Sequelize = require('sequelize')
const Joi = require('joi')
const config = require('config');
const pg_dbname = config.get('POSTGRES.database')
//insert payload
async function migratepgInsert(dbpool, payload) {
  console.log(payload);
  const table = payload.TABLENAME
  const dbname = payload.SCHEMANAME
  payload = payload.DATA
  try {
    //const client = await dbpool.connect();
    console.log("db name : " +  dbname);
    console.log("table name : " +  table);

if (config.has(`EXEMPTIONDATATYPE.MONEY.${dbname}_${table}`)) 
{
fieldname = config.get(`EXEMPTIONDATATYPE.MONEY.${dbname}_${table}`)
console.log("Exemption File Name : " + fieldname);
//payload[fieldname] = (payload.fieldname.toUpperCase == 'NULL') ? payload.fieldname:payload.fieldname.substr(1);
payload[fieldname] = (payload[fieldname].toUpperCase == 'NULL') ? payload[fieldname]:payload[fieldname].substr(1);
console.log(payload[fieldname])
}
    const client = dbpool;
    console.log("=========== pg insert without unique datatype ==============");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
    sql = `insert into "${table}" (\"${columnNames.join('\", \"')}\") values (${columnNames.map((k) => `'${payload[k]}'`).join(', ')});` // "insert into <schema>:<table> (col_1, col_2, ...) values (val_1, val_2, ...)"
    console.log("Executing query : " + sql);
    // sql = "insert into test6 (cityname) values ('verygoosdsdsdsd');";
    await client.query(sql);
    //await client.release(true);
    console.log(`end connection of postgres for database`);
  } catch (e) {
    throw e;
  }
}
//update payload
async function migratepgUpdate(dbpool, payload) {
  console.log("-----------------------old update migratepgUpdate----------------");
  console.log(payload);
  const table = payload.TABLENAME
  const dbname = payload.SCHEMANAME
  payload = payload.DATA  
  try {
    //const client = await dbpool.connect();
    console.log("db name : " +  dbname);
    console.log("table name : " +  table);

if (config.has(`EXEMPTIONDATATYPE.MONEY.${dbname}_${table}`)) 
{
fieldname = config.get(`EXEMPTIONDATATYPE.MONEY.${dbname}_${table}`)
console.log("Exemption File Name : " + fieldname);
//payload[fieldname] = (payload.fieldname.toUpperCase == 'NULL') ? payload.fieldname:payload.fieldname.substr(1);
//payload[fieldname] = (payload[fieldname].toUpperCase == 'NULL') ? payload[fieldname]:payload[fieldname].substr(1);
//console.log(payload[fieldname])
payload[fieldname]['old'] = (payload[fieldname]['old'].toUpperCase == 'NULL') ? payload[fieldname]['old']:payload[fieldname]['old'].substr(1);
console.log(payload[fieldname]['old'])
payload[fieldname]['new'] = (payload[fieldname]['new'].toUpperCase == 'NULL') ? payload[fieldname]['new']:payload[fieldname]['new'].substr(1);
console.log(payload[fieldname]['old'])
}    
    const client = dbpool;
    console.log("=========== pg update without unique datatype ==============");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    var datatypeobj = new Object();
    const sqlfetchdatatype = 'SELECT column_name, udt_name FROM information_schema.COLUMNS WHERE table_schema=$1 and TABLE_NAME = $2';
    const sqlfetchdatatypevalues = [ schemaname , table ];
    await client.query(sqlfetchdatatype, sqlfetchdatatypevalues ).then(res => {
      console.log("datatype fetched---------------------");
   //   console.log(res);
      const data = res.rows; 
      data.forEach(row => datatypeobj[ row['column_name'] ]= row['udt_name'] ); 
    })    
//  console.log(datatypeobj['dmoney']);
    //Primary key retrival
    var datapk = [];
    //const sqlfetchdatatype = 'SELECT column_name, udt_name FROM information_schema.COLUMNS WHERE table_schema=$1 and TABLE_NAME = $2';
    const sqlfetchdatapk = 'SELECT c.column_name, c.ordinal_position FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.constraint_schema=$1 AND t.table_name = $2 AND t.constraint_type = $3';
    const sqlfetchdatapkvalues = [ schemaname , table , 'PRIMARY KEY' ];
    await client.query(sqlfetchdatapk, sqlfetchdatapkvalues ).then(res => {
      console.log("primary fetched---------------------");
      //console.log(res);
      const data = res.rows; 
      data.forEach(row =>  datapk.push(row['column_name']) ); 
    }) 
    console.log
    console.log("BBuidling condtion")
    buffferoldcond = 0
    bufferforsetdatastr = 0
    var setdatastr = ""
    var oldconditionstr = ""
    columnNames.forEach((colName) => {
     // console.log(colName);
      colobj = payload[colName]
     // if (buffferoldcond == 1) {
     //     oldconditionstr = oldconditionstr + " and "
     // } 
      if (bufferforsetdatastr == 1) {
          setdatastr = setdatastr + " , "
      } 
      if ( (datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date' )  && colobj['new'].toUpperCase() == 'NULL' )
      {
          setdatastr = setdatastr +  "\"" + colName + "\"= NULL "
      }
      else 
      {
        setdatastr = setdatastr +   "\"" + colName + "\"= '" + colobj.new + "' "
      }  
  if (datapk.length == 0)
  {
      if (buffferoldcond == 1) {
          oldconditionstr = oldconditionstr + " and "
      }   
      if ( ( datatypeobj[colName] == 'timestamp'  || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date' ) && colobj['old'].toUpperCase() == 'NULL' )
      {
          oldconditionstr = oldconditionstr  + "\"" + colName + "\" is NULL "
      }
      else 
      {
        oldconditionstr = oldconditionstr +   "\"" + colName + "\"= '" + colobj.old + "' "
      }
      buffferoldcond = 1
  }
  else
  {
     if( datapk.includes(colName) )
      {
             if (buffferoldcond == 1) {
                  oldconditionstr = oldconditionstr + " and "
              } 
              if ( ( datatypeobj[colName] == 'timestamp'  || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date' ) && colobj['old'].toUpperCase() == 'NULL' )
              {
                  oldconditionstr = oldconditionstr  + "\"" + colName + "\" is NULL "
              }
              else 
              {
                oldconditionstr = oldconditionstr +   "\"" + colName + "\"= '" + colobj.old + "' "
              }
              buffferoldcond = 1
      }

  }     
      // if ( ( datatypeobj[colName] == 'timestamp'  || datatypeobj[colName] == 'numeric' ) && colobj['old'].toUpperCase() == 'NULL' )
      // {
      //    oldconditionstr = oldconditionstr  + "\"" + colName + "\" is NULL "
      // }
      // else 
      // {
      //   oldconditionstr = oldconditionstr +   "\"" + colName + "\"= '" + colobj.old + "' "
      // }
       // buffferoldcond = 1
        bufferforsetdatastr = 1
    });
    console.log(oldconditionstr);
    console.log(setdatastr);
    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
//    sql = `update ${table} set ${Object.keys(payload).map((key) => `\"${key}\"='${payload[key]['new']}'`).join(', ')} where ${Object.keys(payload).map((key) => `\"${key}\"='${payload[key]['old']}'`).join(' AND ')} ;` // "update <schema>:<table> set col_1=val_1, col_2=val_2, ... where primary_key_col=primary_key_val"
    sql = `update "${table}" set ${setdatastr} where ${oldconditionstr} ;`
    console.log("sqlstring .............................."); 
    console.log(sql);
    //update test5 set id='[object Object].new', cityname='[object Object].new' where id='[object Object].old' AND cityname='[obddject Object].old' ;    
    // sql = "insert into test6 (cityname) values ('verygoosdsdsdsd');";
    await client.query(sql);
    //await client.release(true);
    console.log(`end connection of postgres for database`);
  } catch (e) {
    throw e;
  }
}

//delete payload.id
async function migratepgDelete(dbpool, payload) {

  console.log(payload);
  const table = payload.TABLENAME
  const tablename = payload.TABLENAME
  const dbname = payload.SCHEMANAME
  columns = payload.DATA
  payload = payload.DATA  
  try {

    //const client = await dbpool.connect();
    const client = dbpool;
    console.log("=========== pg delete without unique datatype ==============");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    console.log("retriving data type ------")
    var datatypeobj = new Object();
    const sqlfetchdatatype = 'SELECT column_name, udt_name FROM information_schema.COLUMNS WHERE table_schema=$1 and TABLE_NAME = $2';
    const sqlfetchdatatypevalues = [schemaname, tablename];
    await client.query(sqlfetchdatatype, sqlfetchdatatypevalues).then(res => {
      console.log("datatype fetched---------------------");
      //console.log(res);
      const data = res.rows;
      data.forEach(row => datatypeobj[row['column_name']] = row['udt_name']);
    })
    //Primary key retrival
    var datapk = [];
    //const sqlfetchdatatype = 'SELECT column_name, udt_name FROM information_schema.COLUMNS WHERE table_schema=$1 and TABLE_NAME = $2';
    const sqlfetchdatapk = 'SELECT c.column_name, c.ordinal_position FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.constraint_schema=$1 AND t.table_name = $2 AND t.constraint_type = $3';
    const sqlfetchdatapkvalues = [schemaname, tablename, 'PRIMARY KEY'];
    await client.query(sqlfetchdatapk, sqlfetchdatapkvalues).then(res => {
      console.log("primary fetched---------------------");
      //console.log(res);
      const data = res.rows;
      data.forEach(row => datapk.push(row['column_name']));
    })    

    var conditionstr = ""
    bufffercond = 0  
    columnNames.forEach((colName) => {
      if (datapk.length == 0)
      {
        if (bufffercond == 1) {
          conditionstr = conditionstr + " and "
        }
        if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date' ) && columns[colName].toUpperCase() == 'NULL') {
          conditionstr = conditionstr + tablename + "." + colName + " is NULL "
        } else {
          conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
        }
        bufffercond = 1    
      } else {

          if (datapk.includes(colName)) {
            if (bufffercond == 1) {
              conditionstr = conditionstr + " and "
            }
            if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date' ) && columns[colName].toUpperCase() == 'NULL') {
              conditionstr = conditionstr + tablename + "." + colName + " is NULL "
            } else {
              conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
            }
            bufffercond = 1
          }      
        
    
      }
    });
    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
    sql = `delete from "${table}" where ${conditionstr}  ;` // "delete query
    //sql = `delete from "${table}" where ${Object.keys(payload).map((key) => `${key}='${payload[key]}'`).join('  AND  ')}  ;` // "delete query
    console.log(sql);
    // sql = "insert into test6 (cityname) values ('verygoosdsdsdsd');";
    await client.query(sql);
    //await client.release(true);
    console.log(`end connection of postgres for database`);
  } catch (e) {
    throw e;
  }

}

module.exports = {
  migratepgDelete,
  migratepgInsert,
  migratepgUpdate
}
