const Promise = require('bluebird');
isUtf8 = require('is-utf8');
const _ = require('underscore');
const config = require('config');
//const fs = require('fs-extra');
const logger = require('../common/logger');
//const {
//  executeQueryAsync
//} = require('../common/informixWrapper');

const {
  getInformixConnection,
  prepare,
  wrapTransaction
} = require('../common/informix_ifxnjs')
// const {
//   createPool,
//   extractPostgresTablesInfoAsync,
// } = require('../common/postgresWrapper');

const pg_dbname = config.get('POSTGRES.database')
async function migrateifxinsertdata(payload, client) {
  //retrive data and construc query
  console.log("=========== pg insert with unique datatype ==============")
  console.log(payload)
  const columns = payload.DATA
  console.log(columns)
  const columnNames = Object.keys(columns)
  const tablename = payload.TABLENAME
  console.log("work2---------------------------------------")
  const db_schema = payload.SCHEMANAME
  console.log("retriving data type ------")
  var datatypeobj = new Object();
  const sqlfetchdatatype = 'SELECT column_name, udt_name FROM information_schema.COLUMNS WHERE table_schema=$1 and TABLE_NAME = $2';
  const sqlfetchdatatypevalues = [ db_schema , tablename ];
  await client.query(sqlfetchdatatype, sqlfetchdatatypevalues ).then(res => {
    console.log("datatype fetched---------------------");
    //console.log(res);
    const data = res.rows; 
    data.forEach(row => datatypeobj[ row['column_name'] ]= row['udt_name'] ); 
  })    
  var conditionstr = ""
  const paramSql = Array.from(Array(columnNames.length).keys(), x => `$${x + 1}`).join(',');
  const insertSql = `insert into "${tablename}" (${columnNames.map(x => `"${x}"`).join(',')}) values(${paramSql})`;
  bufffercond = 0
  console.log("work2---------------------------------------")
  columnNames.forEach((colName) => {
    console.log(colName)
    tempvar = columns[colName]
    console.log(tempvar)
    if (columns[colName] != 'unsupportedtype') {
      if (bufffercond == 1) {
        conditionstr = conditionstr + " and "
      }
      console.log(columns[colName])
      tempvar = columns[colName]
      //conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
      if ( datatypeobj[colName] == 'timestamp'   && colobj['new'].toUpperCase() == 'NULL' )
      {
        conditionstr = conditionstr + tablename + "." + colName + " is NULL "
      }
      else 
      {
        conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
      }         
      bufffercond = 1
    }
  });
  infsql = `select * from ${tablename} where ${conditionstr};` // "insert into <schema>:<table> (col_1, col_2, ...) values (val_1, val_2, ...)"
  console.log(`informix query ${infsql}`);
  logger.debug(`informix query ${infsql}`);
  //sql = `insert into ${payload.payload.schema}:${payload.payload.table} (${columnNames.join(', ')}) values (${columnNames.map((k) => `'${columns[k]}'`).join(', ')});` // "insert into <schema>:<table> (col_1, col_2, ...) values (val_1, val_2, ...)"
  //const data = await executeQueryAsync(db_schema, infsql);
  // if (!data.length) {
  //   break;
  // }
  try {
    const connection = await getInformixConnection(db_schema)
    const queryStmt = await prepare(connection, infsql)
    const queryResult = Promise.promisifyAll((await queryStmt.executeAsync()))
    const data = await queryResult.fetchAllAsync();
    //console.log(data);
    connection.closeAsync();
    //} catch(e) {console.log('Error', e )}

    for (const row of data) {
      const values = [];
      columnNames.forEach((colName) => {
        if (row[colName])
        {
        if (isUtf8(row[colName])) {
          console.log(`utf8 format ${colName}`);
          //         values.push(new Buffer.from(row[colName],'binary'));
          values.push(row[colName]);
        } else {
          //  values.push(row[colName]);
          values.push(new Buffer.from(row[colName], 'binary'));
        }
      } else {
        values.push(row[colName]);         
      }
        //values.push(new Buffer.from(row[colName],'binary'));
      });
      let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
      sql = `SET search_path TO ${schemaname};`;
      console.log(sql);
      await client.query(sql);
      logger.debug(`postgres insert sql ${insertSql} with values[${JSON.stringify(values)}`);
      console.log(client);
      await client.query(insertSql, values);
    }
  } catch (e) {
    console.log('Error', e);
    throw e;
  }


}
async function migrateifxupdatedata(payload, client) {
  console.log("=========== pg update with unique datatype ==============");
  console.log(payload);
  //console.log("work1---------------------------------------")
  //console.log(payload)
  const columns = payload.DATA
  console.log(columns)
  const columnNames = Object.keys(columns)
  console.log(columnNames);
  const tablename = payload.TABLENAME
  console.log("work2---------------------------------------")
  const db_schema = payload.SCHEMANAME
  console.log(tablename);
  console.log("retriving data type ------")
  var datatypeobj = new Object();
  const sqlfetchdatatype = 'SELECT column_name, udt_name FROM information_schema.COLUMNS WHERE table_schema=$1 and TABLE_NAME = $2';
  const sqlfetchdatatypevalues = [ db_schema , tablename ];
  await client.query(sqlfetchdatatype, sqlfetchdatatypevalues ).then(res => {
    console.log("datatype fetched---------------------");
    //console.log(res);
    const data = res.rows; 
    data.forEach(row => datatypeobj[ row['column_name'] ]= row['udt_name'] ); 
  })  
  var conditionstr = ""
  var updatestr = ""
  bufffercond = 0
  buffferupcond = 0
  console.log("work2---------------------------------------")
  bufffernewcond = 0
  buffferoldcond = 0
  var oldconditionstr = ""
  columnNames.forEach((colName) => {
    console.log(colName)
    colobj = columns[colName]
    //console.log(typeof (colobj))
    //console.log(colobj)
    //console.log(colobj.new)   
    if (colobj.new != 'unsupportedtype') {
      if (bufffernewcond == 1) {
        conditionstr = conditionstr + " and "
      }
      if ( datatypeobj[colName] == 'timestamp'   && colobj['new'].toUpperCase() == 'NULL' )
      {
        conditionstr = conditionstr + tablename + "." + colName + " is NULL "
      }
      else 
      {
        conditionstr = conditionstr + tablename + "." + colName + "= '" + colobj.new + "' "
      }        
      bufffernewcond = 1
    }
    if (colobj['old'] != 'unsupportedtype') {
      if (buffferoldcond == 1) {
        oldconditionstr = oldconditionstr + " and "
      }
      //console.log(colobj.old);
      //oldconditionstr = oldconditionstr + tablename + "." + colName + "= '" + colobj.old + "' "
      if ( datatypeobj[colName] == 'timestamp'   && colobj['old'].toUpperCase() == 'NULL' )
      {
          oldconditionstr = oldconditionstr  + "\"" + colName + "\" is NULL "
      }
      else 
      {
        oldconditionstr = oldconditionstr +   "\"" + colName + "\"= '" + colobj.old + "' "
      }      
      buffferoldcond = 1
    }

  });

  console.log(conditionstr)
  console.log(oldconditionstr);
  infsql = `select * from ${tablename} where ${conditionstr};`
  console.log(infsql)

  try {
    const connection = await getInformixConnection(db_schema)
    const queryStmt = await prepare(connection, infsql)
    const queryResult = Promise.promisifyAll((await queryStmt.executeAsync()))
    const data = await queryResult.fetchAllAsync();
    //console.log(data);
    connection.closeAsync();

    var updatesql = ""
    updatesql = `UPDATE ${tablename} SET `
    counter = 1
    for (const row of data) {
      const values = [];
      columnNames.forEach((colName) => {
        if (buffferupcond == 1) {
          updatestr = updatestr + " , "
        }
        if (row[colName])
        {
        if (isUtf8(row[colName])) {
          //console.log(`utf8 format ${colName}`);
          values.push(row[colName]);
          updatestr = updatestr + "\"" + colName + "\"= \$" + counter + " "
          buffferupcond = 1
          counter = counter + 1
        } else {
          values.push(new Buffer.from(row[colName], 'binary'));
          updatestr = updatestr + "\"" + colName + "\"= \$" + counter + " "
          buffferupcond = 1
          counter = counter + 1
        }
        } else {
          values.push(row[colName]);
          updatestr = updatestr + "\"" + colName + "\"= \$" + counter + " "
          buffferupcond = 1
          counter = counter + 1          
        }
      });
      //logger.debug(`postgres insert sql ${insertSql} with values[${JSON.stringify(values)}`);

      let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
      sql = `SET search_path TO ${schemaname};`;
      console.log(sql);
      await client.query(sql);

      updatesql = updatesql + updatestr + " where " + oldconditionstr + " ;"
      console.log(updatesql)
      await client.query(updatesql, values)
    }
  } catch (e) {
    console.log('Error', e);
    throw e;
  }

}
// async function migrateupdatedata(client, database, tableName, informixTable, postgresTable) {
// }
// async function migratedeletedata(client, database, tableName, informixTable, postgresTable) {
// }



module.exports = {
  migrateifxinsertdata,
  migrateifxupdatedata
};