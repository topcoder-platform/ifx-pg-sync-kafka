const dbs = require('../models')
const Sequelize = require('sequelize')
const Joi = require('joi')
const config = require('config');
const pg_dbname = config.get('POSTGRES.database')
//insert payload
async function migratepgInsert(dbpool, payload, dbname, table) {
  console.log(payload);
  try {
    //const client = await dbpool.connect();
    const client = dbpool;
    //console.log("welcome123");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
    sql = `insert into ${table} (\"${columnNames.join('\", \"')}\") values (${columnNames.map((k) => `'${payload[k]}'`).join(', ')});` // "insert into <schema>:<table> (col_1, col_2, ...) values (val_1, val_2, ...)"
    console.log(sql);
    // sql = "insert into test6 (cityname) values ('verygoosdsdsdsd');";
    await client.query(sql);
    //await client.release(true);
    console.log(`end connection of postgres for database`);
  } catch (e) {
    throw e;
  }
}
//update payload
async function migratepgUpdate(dbpool, payload, dbname, table) {
  console.log("-----------------------old update migratepgUpdate----------------");
  console.log(payload);
  try {
    //const client = await dbpool.connect();
    const client = dbpool;
    //console.log("welcome123");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
    sql = `update ${table} set ${Object.keys(payload).map((key) => `\"${key}\"='${payload[key]['new']}'`).join(', ')} where ${Object.keys(payload).map((key) => `\"${key}\"='${payload[key]['old']}'`).join(' AND ')} ;` // "update <schema>:<table> set col_1=val_1, col_2=val_2, ... where primary_key_col=primary_key_val"
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
async function migratepgDelete(dbpool, payload, dbname, table) {

  console.log(payload);
  try {

    //const client = await dbpool.connect();
    const client = dbpool;
    //console.log("welcome123");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    sql = `SET search_path TO ${schemaname};`;
    console.log(sql);
    await client.query(sql);
    sql = `delete from ${table} where ${Object.keys(payload).map((key) => `${key}='${payload[key]['new']}'`).join('  AND  ')}  ;` // "delete query
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