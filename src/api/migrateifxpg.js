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
  let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
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
  const paramSql = Array.from(Array(columnNames.length).keys(), x => `$${x + 1}`).join(',');
  const insertSql = `insert into "${tablename}" (${columnNames.map(x => `"${x}"`).join(',')}) values(${paramSql})`;
  bufffercond = 0
  console.log("work2---------------------------------------")
  usepkforcond = 0
  if (datapk.length != 0) {
    columnNames.forEach((colName) => {
      if (datapk.includes(colName)) {
        if (columns[colName] != 'unsupportedtype') {
          usepkforcond = usepkforcond + 1
        }
      }

    });
  }

  columnNames.forEach((colName) => {
    console.log(colName)
    //tempvar = columns[colName]
    //console.log(tempvar)
    if (usepkforcond == 0) {
      if (columns[colName] != 'unsupportedtype') {
        if (bufffercond == 1) {
          conditionstr = conditionstr + " and "
        }
        //console.log(columns[colName])
        //conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
        if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && columns[colName].toUpperCase() == 'NULL') {
          conditionstr = conditionstr + tablename + "." + colName + " is NULL "
        } else {
          conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
        }
        bufffercond = 1
      }
    } else {
      if (datapk.includes(colName)) {
        if (columns[colName] != 'unsupportedtype') {
          if (bufffercond == 1) {
            conditionstr = conditionstr + " and "
          }
          //console.log(columns[colName])
          //conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
          if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && columns[colName].toUpperCase() == 'NULL') {
            conditionstr = conditionstr + tablename + "." + colName + " is NULL "
          } else {
            conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
          }
          bufffercond = 1
        }
      }
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
        if (row[colName]) {
          if (isUtf8(row[colName]) || datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'date') {
            console.log(`utf8 or datetime format ${colName}`);
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
  let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
  console.log(tablename);
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
  var updatestr = ""
  bufffercond = 0
  buffferupcond = 0
  console.log("work2---------------------------------------")
  bufffernewcond = 0
  buffferoldcond = 0
  var oldconditionstr = ""
  usepkforcond = 0
  if (datapk.length != 0) {
    columnNames.forEach((colName) => {
      if (datapk.includes(colName)) {
        colobj = columns[colName]
        if (colobj.new != 'unsupportedtype') {
          usepkforcond = usepkforcond + 1
        }
      }

    });
  }

  columnNames.forEach((colName) => {
    console.log(colName)
    colobj = columns[colName]
    //console.log(typeof (colobj))
    //console.log(colobj)
    //console.log(colobj.new)  
    if (usepkforcond == 0) {
      if (colobj.new != 'unsupportedtype') {
        if (bufffernewcond == 1) {
          conditionstr = conditionstr + " and "
        }
        if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['new'].toUpperCase() == 'NULL') {
          conditionstr = conditionstr + tablename + "." + colName + " is NULL "
        } else {
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
        if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['old'].toUpperCase() == 'NULL') {
          oldconditionstr = oldconditionstr + "\"" + colName + "\" is NULL "
        } else {
          oldconditionstr = oldconditionstr + "\"" + colName + "\"= '" + colobj.old + "' "
        }
        buffferoldcond = 1
      }
    } else {
      if (datapk.includes(colName)) {
        if (colobj.new != 'unsupportedtype') {
          if (bufffernewcond == 1) {
            conditionstr = conditionstr + " and "
          }
          if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['new'].toUpperCase() == 'NULL') {
            conditionstr = conditionstr + tablename + "." + colName + " is NULL "
          } else {
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
          if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['old'].toUpperCase() == 'NULL') {
            oldconditionstr = oldconditionstr + "\"" + colName + "\" is NULL "
          } else {
            oldconditionstr = oldconditionstr + "\"" + colName + "\"= '" + colobj.old + "' "
          }
          buffferoldcond = 1
        }
      }
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
        if (row[colName]) {
          if (isUtf8(row[colName]) || datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'date') {
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

async function migrateifxdeletedata(payload, client) {
  console.log(payload);
  const table = payload.TABLENAME
  const tablename = payload.TABLENAME
  const dbname = payload.SCHEMANAME
  columns = payload.DATA
  payload = payload.DATA
  try {

    //const client = await dbpool.connect();
    //const client = dbpool;
    //console.log("welcome123");
    console.log("=========== pg delete with unique datatype ==============");
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
    console.log("work2---------------------------------------")
    usepkforcond = 0
    if (datapk.length != 0) {
      columnNames.forEach((colName) => {
        if (datapk.includes(colName)) {
          if (columns[colName] != 'unsupportedtype') {
            usepkforcond = usepkforcond + 1
          }
        }

      });
    }
    var conditionstr = ""
    //const paramSql = Array.from(Array(columnNames.length).keys(), x => `$${x + 1}`).join(',');
    //const insertSql = `insert into "${tablename}" (${columnNames.map(x => `"${x}"`).join(',')}) values(${paramSql})`;
    bufffercond = 0
    columnNames.forEach((colName) => {
      console.log(colName)
      //tempvar = columns[colName]
      //console.log(tempvar)
      if (usepkforcond == 0) {
        if (columns[colName] != 'unsupportedtype') {
          if (bufffercond == 1) {
            conditionstr = conditionstr + " and "
          }
          //console.log(columns[colName])
          //conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
          if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && columns[colName].toUpperCase() == 'NULL') {
            conditionstr = conditionstr + tablename + "." + colName + " is NULL "
          } else {
            conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
          }
          bufffercond = 1
        }
      } else {
        if (datapk.includes(colName)) {
          if (columns[colName] != 'unsupportedtype') {
            if (bufffercond == 1) {
              conditionstr = conditionstr + " and "
            }
            //console.log(columns[colName])
            //conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
            if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && columns[colName].toUpperCase() == 'NULL') {
              conditionstr = conditionstr + tablename + "." + colName + " is NULL "
            } else {
              conditionstr = conditionstr + tablename + "." + colName + "= '" + columns[colName] + "' "
            }
            bufffercond = 1
          }
        }
      }
    });

    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
    sql = `delete from "${table}" where ${conditionstr}  ;` // "delete query
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
  migrateifxinsertdata,
  migrateifxupdatedata,
  migrateifxdeletedata
};