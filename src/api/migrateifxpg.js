const Promise = require('bluebird');
isUtf8 = require('is-utf8');
const _ = require('underscore');
const config = require('config');
const logger = require('../common/logger');
const ifxcore = require('../common/informix_ifxnjs')
const dbcommonfunction = require('../common/dbmigratefunctions')
const pg_dbname = config.get('POSTGRES.database')
async function migrateifxinsertdata(payload, client) {
  //retrive data and construc query
  try {
    logger.info("=========== pg insert with unique datatype ==============")
    logger.debug(payload)
    const columns = payload.DATA
    logger.debug(columns)
    const columnNames = Object.keys(columns)
    const tablename = payload.TABLENAME
    const db_schema = payload.SCHEMANAME
    payload = payload.DATA
    let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
    logger.debug("retriving data type ------")
    //var datatypeobj = new Object();
    var datatypeobj = await dbcommonfunction.pgfetchdatatype(client, schemaname, tablename);
    //Primary key retrival
    //var datapk = [];
    var datapk = await dbcommonfunction.pgfetchprimarykey(client, schemaname, tablename);
    payload = await dbcommonfunction.hextoutf_insertpayload(columnNames, datatypeobj, payload)
    const paramSql = Array.from(Array(columnNames.length).keys(), x => `$${x + 1}`).join(',');
    const insertSql = `insert into "${tablename}" (${columnNames.map(x => `"${x}"`).join(',')}) values(${paramSql})`;
    var conditionstr = ""
    usepkforcond = 0
    if (datapk.length != 0) {
      usepkforcond = await dbcommonfunction.ivalidatetousepkcondition(columnNames, payload, datapk)
    }
    if (usepkforcond == 0) {
      conditionstr = await dbcommonfunction.insertretrivalcondition_withoutpk(columnNames, payload, datatypeobj, tablename)
    } else {
      conditionstr = await dbcommonfunction.insertretrivalcondition_withpk(columnNames, payload, datatypeobj, tablename, datapk)
    }
    infsql = `select * from ${tablename} where ${conditionstr};` // "insert into <schema>:<table> (col_1, col_2, ...) values (val_1, val_2, ...)"
    logger.info(`informix query ${infsql}`);
    const connection = await ifxcore.getInformixConnection(db_schema)
    const queryStmt = await ifxcore.prepare(connection, infsql)
    const queryResult = Promise.promisifyAll((await queryStmt.executeAsync()))
    const data = await queryResult.fetchAllAsync();
    logger.debug(data);
    connection.closeAsync();
    //missed exemption condition need to check
    for (const row of data) {
      values = await dbcommonfunction.db_datavalues_from_fetched_row(columnNames, row, db_schema, tablename, datatypeobj);
      let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
      sql = `SET search_path TO ${schemaname};`;
      logger.info(sql);
      await client.query(sql);
      logger.debug(`postgres insert sql ${insertSql} with values[${JSON.stringify(values)}`);
      logger.debug(client);
      await client.query(insertSql, values);
    }
  } catch (e) {
    logger.error('Error : migrate ifx to pg insert');
    logger.logFullError(e);
    throw e;
  }


}
async function migrateifxupdatedata(payload, client) {
  try {
    logger.info("=========== pg update with unique datatype ==============");
    logger.debug(payload);
    const columns = payload.DATA
    logger.debug(columns)
    const columnNames = Object.keys(columns)
    logger.debug(columnNames);
    const tablename = payload.TABLENAME
    const db_schema = payload.SCHEMANAME
    const dbname = payload.SCHEMANAME
    let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
    logger.debug(tablename);
    payload = payload.DATA;
    //console.log("retriving data type ------")
    //var datatypeobj = new Object();
    datatypeobj = await dbcommonfunction.pgfetchdatatype(client, schemaname, tablename);
    //Primary key retrival
    // var datapk = [];
    datapk = await dbcommonfunction.pgfetchprimarykey(client, schemaname, tablename);
    payload = await dbcommonfunction.hextoutf_updatepayload(columnNames, datatypeobj, payload)
    var conditionstr = ""
    var updatestr = ""
    var oldconditionstr = ""
    usepkforcond = 0
    if (datapk.length != 0) {
      usepkforcond = await dbcommonfunction.uvalidatetousepkcondition(columnNames, payload, datapk)
    }
    //REtrival contion
    if (usepkforcond == 0) {
      conditionstr = await dbcommonfunction.updateretrivalcondition_withoutpk(columnNames, payload, datatypeobj, tablename)
    } else {
      conditionstr = await dbcommonfunction.updateretrivalcondition_withpk(columnNames, payload, datatypeobj, tablename, datapk)
    }
    //Exemption added it should always in sequence
    payload = await dbcommonfunction.uvalidateexemptiondatatype(dbname, tablename, payload);
    //constructing update condition
    if (usepkforcond == 0) {
      oldconditionstr = await dbcommonfunction.updatedatacondition_withoutpk(columnNames, payload, datatypeobj)
    } else {
      oldconditionstr = await dbcommonfunction.updatedatacondition_withpk(columnNames, payload, datatypeobj, datapk)
    }

    logger.debug(conditionstr)
    logger.debug(oldconditionstr);
    infsql = `select * from ${tablename} where ${conditionstr};`
    logger.info(infsql)
    const connection = await ifxcore.getInformixConnection(db_schema)
    const queryStmt = await ifxcore.prepare(connection, infsql)
    const queryResult = Promise.promisifyAll((await queryStmt.executeAsync()))
    const data = await queryResult.fetchAllAsync();
    logger.debug(data);
    connection.closeAsync();

    var updatesql = ""
    updatesql = `UPDATE ${tablename} SET `
    updatestr = await dbcommonfunction.createupdatestr(columnNames);
    for (const row of data) {
      const values = await dbcommonfunction.db_datavalues_from_fetched_row(columnNames, row, db_schema, tablename, datatypeobj);
      let schemaname = (db_schema == pg_dbname) ? 'public' : db_schema;
      sql = `SET search_path TO ${schemaname};`;
      logger.info(sql);
      await client.query(sql);
      updatesql = updatesql + updatestr + " where " + oldconditionstr + " ;"
      logger.info(updatesql)
      await client.query(updatesql, values)
    }
  } catch (e) {
    logger.error('Error : migrate ifx to pg update');
    logger.logFullError(e);
    throw e;
  }

}

async function migrateifxdeletedata(payload, client) {
  try {
    logger.debug(payload);
    const table = payload.TABLENAME
    const tablename = payload.TABLENAME
    const dbname = payload.SCHEMANAME
    columns = payload.DATA
    payload = payload.DATA
    payload = await dbcommonfunction.ivalidateexemptiondatatype(dbname, tablename, payload);
    logger.info("=========== pg delete with unique datatype ==============");
    // exemption type missing
    const columnNames = Object.keys(payload)
    let schemaname = (dbname == pg_dbname) ? 'public' : dbname;
    datatypeobj = await dbcommonfunction.pgfetchdatatype(client, schemaname, tablename);
    datapk = await dbcommonfunction.pgfetchprimarykey(client, schemaname, tablename);
    payload = await dbcommonfunction.hextoutf_insertpayload(columnNames, datatypeobj, payload)
    usepkforcond = 0
    if (datapk.length != 0) {
      usepkforcond = await dbcommonfunction.ivalidatetousepkcondition(columnNames, payload, datapk)
    }
    var conditionstr = ""
    if (usepkforcond == 0) {
      conditionstr = await dbcommonfunction.deletedatacondition_withoutpk(columnNames, payload, datatypeobj, tablename)
    } else {
      conditionstr = await dbcommonfunction.deletedatacondition_withpk(columnNames, payload, datatypeobj, tablename, datapk)
    }
    sql = `SET search_path TO ${schemaname};`;
    logger.info(sql);
    await client.query(sql);
    sql = `delete from "${table}" where ${conditionstr}  ;` // "delete query
    logger.info(sql);
    await client.query(sql);
    logger.info(`end connection of postgres for database - mig ifx pg delete`);
  } catch (e) {
    throw e;
  }
}

module.exports = {
  migrateifxinsertdata,
  migrateifxupdatedata,
  migrateifxdeletedata
};