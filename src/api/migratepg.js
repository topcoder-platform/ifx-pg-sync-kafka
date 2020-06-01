const dbs = require('../models')
const Sequelize = require('sequelize')
const Joi = require('joi')
const config = require('config');
const dbcommonfunction = require('../common/dbmigratefunctions')
const logger = require('../common/logger')
const pg_dbname = config.get('POSTGRES.database')
//insert payload
async function migratepgInsert(dbpool, payload) {
  try {
    logger.debug(payload);
    const tablename = payload.TABLENAME
    const dbname = payload.SCHEMANAME
    payload = payload.DATA
    logger.debug("db name : " + dbname);
    logger.debug("table name : " + tablename);
    payload = await dbcommonfunction.ivalidateexemptiondatatype(dbname, tablename, payload);
    const client = dbpool;
    logger.info("=========== pg insert without unique datatype ==============");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    datatypeobj = await dbcommonfunction.pgfetchdatatype(client, schemaname, tablename);
    payload = await dbcommonfunction.hextoutf_insertpayload(columnNames, datatypeobj, payload)
    logger.debug("Payload after hex validation : " + JSON.stringify(payload))
    sql = `SET search_path TO ${schemaname};`;
    logger.info(sql);
    await client.query(sql);
    const paramSql = Array.from(Array(columnNames.length).keys(), x => `$${x + 1}`).join(',');
    sql = `insert into "${tablename}" (${columnNames.map(x => `"${x}"`).join(',')}) values(${paramSql})`;
    const values = await dbcommonfunction.db_datavalues_from_insert_datapayload(columnNames, payload);
    logger.info("Executing query : " + sql);
    await client.query(sql, values);
    logger.info(`end connection of postgres for database after insert`);
  } catch (e) {
    throw e;
  }
}
//update payload
async function migratepgUpdate(dbpool, payload) {
  try {
    logger.debug(payload);
    const tablename = payload.TABLENAME
    const dbname = payload.SCHEMANAME
    payload = payload.DATA
    logger.debug("db name : " + dbname);
    logger.debug("table name : " + tablename);
    payload = await dbcommonfunction.uvalidateexemptiondatatype(dbname, tablename, payload);
    const client = dbpool;
    logger.info("=========== pg update without unique datatype ==============");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    //fetch data type
    datatypeobj = await dbcommonfunction.pgfetchdatatype(client, schemaname, tablename);
    //Primary key retrival
    datapk = await dbcommonfunction.pgfetchprimarykey(client, schemaname, tablename);
    payload = await dbcommonfunction.hextoutf_updatepayload(columnNames, datatypeobj, payload)
    var setdatastr = ""
    var oldconditionstr = ""
    setdatastr = await dbcommonfunction.updatesetdatastr(columnNames, payload, datatypeobj)
    if (datapk.length == 0) {
      oldconditionstr = await dbcommonfunction.updatedatacondition_withoutpk(columnNames, payload, datatypeobj)
    } else {
      oldconditionstr = await dbcommonfunction.updatedatacondition_withpk(columnNames, payload, datatypeobj, datapk)
    }
    logger.debug(oldconditionstr);
    logger.debug(setdatastr);
    sql = `SET search_path TO ${schemaname};`;
    logger.info(sql);
    await client.query(sql);

    var updatestr = await dbcommonfunction.createupdatestr(columnNames);
    const values = await dbcommonfunction.db_datavalues_from_update_datapayload(columnNames, payload);

    sql = `update "${tablename}" set ${updatestr} where ${oldconditionstr} ;`
    logger.debug("sqlstring ..............................");
    logger.info(sql);
    await client.query(sql, values);
    logger.info(`end connection of postgres for database after update`);
  } catch (e) {
    throw e;
  }
}

//delete payload.id
async function migratepgDelete(dbpool, payload) {
  try {
    logger.debug(payload);
    const tablename = payload.TABLENAME
    const dbname = payload.SCHEMANAME
    const client = dbpool;
    payload = payload.DATA
    payload = await dbcommonfunction.ivalidateexemptiondatatype(dbname, tablename, payload);
    logger.info("=========== pg delete without unique datatype ==============");
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    //console.log("retriving data type ------")
    //var datatypeobj = new Object();
    datatypeobj = await dbcommonfunction.pgfetchdatatype(client, schemaname, tablename);
    //Primary key retrival
    //var datapk = [];
    datapk = await dbcommonfunction.pgfetchprimarykey(client, schemaname, tablename);
    payload = await dbcommonfunction.hextoutf_insertpayload(columnNames, datatypeobj, payload)
    var conditionstr = ""
    if (datapk.length == 0) {
      conditionstr = await dbcommonfunction.deletedatacondition_withoutpk(columnNames, payload, datatypeobj, tablename)
    } else {
      conditionstr = await dbcommonfunction.deletedatacondition_withpk(columnNames, payload, datatypeobj, tablename, datapk)
    }
    sql = `SET search_path TO ${schemaname};`;
    logger.info(sql);
    await client.query(sql);
    sql = `delete from "${tablename}" where ${conditionstr}  ;` // "delete query
    logger.info(sql);
    await client.query(sql);
    logger.info(`end connection of postgres for database after delete`);
  } catch (e) {
    throw e;
  }

}

module.exports = {
  migratepgDelete,
  migratepgInsert,
  migratepgUpdate
}