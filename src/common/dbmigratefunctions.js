const config = require('config');

async function pgfetchdatatype(client, schemaname, tablename) {
    try {
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
        return datatypeobj;
    } catch (err) {
        throw "pgfetchdatatype : " + err
    }
}
async function pgfetchprimarykey(client, schemaname, tablename) {
    try {
        console.log("retriving primary key ------")
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
        return datapk;
    } catch (err) {
        throw "pgfetchprimarykey : " + err
    }
}
async function ivalidateexemptiondatatype(dbname, tablename, payload) {
    try {

        if (config.has(`EXEMPTIONDATATYPE.MONEY.${dbname}_${tablename}`)) {
            fieldname = config.get(`EXEMPTIONDATATYPE.MONEY.${dbname}_${tablename}`)
            console.log("Exemption File Name : " + fieldname);
            payload[fieldname] = (payload[fieldname].toUpperCase == 'NULL') ? payload[fieldname] : payload[fieldname].substr(1);
            console.log(payload[fieldname])
        }
        return payload;
    } catch (err) {
        throw "ivalidateexemptiondatatype : " + err
    }

}
async function uvalidateexemptiondatatype(dbname, tablename, payload) {
    try {
        if (config.has(`EXEMPTIONDATATYPE.MONEY.${dbname}_${tablename}`)) {
            fieldname = config.get(`EXEMPTIONDATATYPE.MONEY.${dbname}_${tablename}`)
            console.log("Exemption File Name : " + fieldname);
            payload[fieldname]['old'] = (payload[fieldname]['old'].toUpperCase == 'NULL') ? payload[fieldname]['old'] : payload[fieldname]['old'].substr(1);
            console.log(payload[fieldname]['old'])
            payload[fieldname]['new'] = (payload[fieldname]['new'].toUpperCase == 'NULL') ? payload[fieldname]['new'] : payload[fieldname]['new'].substr(1);
            console.log(payload[fieldname]['new'])
        }
        return payload;
    } catch (err) {
        throw "uvalidateexemptiondatatype : " + err
    }
}
async function checkdataishex(hexdata) {
    try {
        if (Buffer.from(hexdata, 'ascii').toString() === hexdata) {
            console.log("It is ascii format")
            return false;
        } else {
            console.log("it is hex format")
            return true;
        }
    } catch (err) {
        throw "checkdataishex : " + err
    }

}
async function converthextoutf(hexdata) {
    try {
        hexdata = hexdata.match(/[0-9A-Fa-f]*/g).filter(function (el) {
            return el != "";
        });
        hexdata = hexdata.map(function (item) {
            return parseInt(item, 16);
        });
        var bytes = new Uint8Array(hexdata);
        var encoding = 'utf-8';
        var returndata = new TextDecoder(encoding, {
            NONSTANDARD_allowLegacyEncoding: true
        });
        //console.log("decoded", returndata.decode(bytes));
        return returndata.decode(bytes);
    } catch (err) {
        throw "converthextoutf : " + err
    }
}
async function updateretrivalcondition_withpk(columnNames, payload, datatypeobj, datapk) {
    try {
        var conditionstr = ""
        bufffernewcond = 0
        columnNames.forEach((colName) => {
            colobj = payload[colName]

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
            }
        });
        return conditionstr;

    } catch (err) {
        throw "updateretrivalcondition_withpk : " + err
    }

}
async function insertretrivalcondition_withpk(columnNames, payload, datatypeobj, datapk) {
    try {

        var conditionstr = ""
        bufffercond = 0
        columnNames.forEach((colName) => {
            console.log(colName)
            if (datapk.includes(colName)) {
                if (payload[colName] != 'unsupportedtype') {
                    if (bufffercond == 1) {
                        conditionstr = conditionstr + " and "
                    }
                    if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && payload[colName].toUpperCase() == 'NULL') {
                        conditionstr = conditionstr + tablename + "." + colName + " is NULL "
                    } else {
                        conditionstr = conditionstr + tablename + "." + colName + "= '" + payload[colName] + "' "
                    }
                    bufffercond = 1
                }
            }

        });
        return conditionstr;
    } catch (err) {
        throw "insertretrivalcondition_withpk : " + err
    }
}
async function updatedatacondition_withpk(columnNames, payload, datatypeobj, datapk) {
    try {
        console.log("Buidling condtion with primary key")
        buffferoldcond = 0
        var conditionstr = ""
        columnNames.forEach((colName) => {
            colobj = payload[colName]
            if (datapk.includes(colName)) {
                if (colobj['old'] != 'unsupportedtype') {
                    if (buffferoldcond == 1) {
                        conditionstr = conditionstr + " and "
                    }
                    if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['old'].toUpperCase() == 'NULL') {
                        conditionstr = conditionstr + "\"" + colName + "\" is NULL "
                    } else {
                        conditionstr = conditionstr + "\"" + colName + "\"= '" + colobj.old + "' "
                    }
                    buffferoldcond = 1
                }
            }
        });
        return conditionstr;
    } catch (err) {
        throw "updatedatacondition_withpk : " + err
    }
}
async function updateretrivalcondition_withoutpk(columnNames, payload, datatypeobj) {
    try {
        var conditionstr = ""
        bufffernewcond = 0
        columnNames.forEach((colName) => {
            console.log(colName)
            colobj = payload[colName]
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

        });
        return conditionstr;
    } catch (err) {
        throw "updateretrivalcondition_withoutpk : " + err
    }
}
async function insertretrivalcondition_withoutpk(columnNames, payload, datatypeobj) {
    try {
        var conditionstr = ""
        bufffercond = 0
        columnNames.forEach((colName) => {
            console.log(colName)
            if (payload[colName] != 'unsupportedtype') {
                if (bufffercond == 1) {
                    conditionstr = conditionstr + " and "
                }
                if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && payload[colName].toUpperCase() == 'NULL') {
                    conditionstr = conditionstr + tablename + "." + colName + " is NULL "
                } else {
                    conditionstr = conditionstr + tablename + "." + colName + "= '" + payload[colName] + "' "
                }
                bufffercond = 1
            }

        });
        return conditionstr;
    } catch (err) {
        throw "insertretrivalcondition_withoutpk : " + err
    }
}
async function updatedatacondition_withoutpk(columnNames, payload, datatypeobj) {
    try {
        //columnNames,payload,datatypeobj
        console.log("Buidling condtion without primary key")
        buffferoldcond = 0
        var conditionstr = ""
        columnNames.forEach((colName) => {
            colobj = payload[colName]
            if (colobj['old'] != 'unsupportedtype') {
                if (buffferoldcond == 1) {
                    conditionstr = conditionstr + " and "
                }
                if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['old'].toUpperCase() == 'NULL') {
                    conditionstr = conditionstr + "\"" + colName + "\" is NULL "
                } else {
                    conditionstr = conditionstr + "\"" + colName + "\"= '" + colobj.old + "' "
                }
                buffferoldcond = 1
            }
        });
        return conditionstr;
    } catch (err) {
        throw "updatedatacondition_withoutpk : " + err
    }
}
async function updatesetdatastr(columnNames, payload, datatypeobj) {
    try {
        console.log("Buidling data string")
        bufferforsetdatastr = 0
        var setdatastr = ""
        columnNames.forEach((colName) => {
            colobj = payload[colName]
            if (bufferforsetdatastr == 1) {
                setdatastr = setdatastr + " , "
            }
            if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && colobj['new'].toUpperCase() == 'NULL') {
                setdatastr = setdatastr + "\"" + colName + "\"= NULL "
            } else {
                setdatastr = setdatastr + "\"" + colName + "\"= '" + colobj.new + "' "
            }
            bufferforsetdatastr = 1
        });
        return setdatastr;
    } catch (err) {
        throw "updatesetdatastr : " + err
    }
}
async function deletedatacondition_withoutpk(columnNames, payload, datatypeobj) {
    try {
        var conditionstr = ""
        bufffercond = 0
        columnNames.forEach((colName) => {
            if (payload[colName] != 'unsupportedtype') { //condition unique data type alone
                if (bufffercond == 1) {
                    conditionstr = conditionstr + " and "
                }
                if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && payload[colName].toUpperCase() == 'NULL') {
                    conditionstr = conditionstr + tablename + "." + colName + " is NULL "
                } else {
                    conditionstr = conditionstr + tablename + "." + colName + "= '" + payload[colName] + "' "
                }
                bufffercond = 1
            }
        });
        return conditionstr;

    } catch (err) {
        throw "deletedatacondition_withoutpk : " + err
    }
}
async function deletedatacondition_withpk(columnNames, payload, datatypeobj, datapk) {
    try {
        var conditionstr = ""
        bufffercond = 0
        columnNames.forEach((colName) => {
            if (datapk.includes(colName)) {
                if (payload[colName] != 'unsupportedtype') { //condition unique data type alone
                    if (bufffercond == 1) {
                        conditionstr = conditionstr + " and "
                    }
                    if ((datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'numeric' || datatypeobj[colName] == 'date') && payload[colName].toUpperCase() == 'NULL') {
                        conditionstr = conditionstr + tablename + "." + colName + " is NULL "
                    } else {
                        conditionstr = conditionstr + tablename + "." + colName + "= '" + payload[colName] + "' "
                    }
                    bufffercond = 1
                }
            }
        });
        return conditionstr;
    } catch (err) {
        throw "deletedatacondition_withpk : " + err
    }
}
async function ivalidatetousepkcondition(columnNames, payload, datapk) {
    try {
        usepkforcond = 0
        columnNames.forEach((colName) => {
            if (datapk.includes(colName)) {
                if (payload[colName] != 'unsupportedtype') {
                    usepkforcond = usepkforcond + 1
                }
            }
        });
        return usepkforcond;
    } catch (err) {
        throw "ivalidatetousepkcondition : " + err
    }
}
async function uvalidatetousepkcondition(columnNames, payload, datapk) {
    try {
        usepkforcond = 0
        columnNames.forEach((colName) => {
            colobj = payload[colName]
            if (datapk.includes(colName)) {
                if (colobj.new != 'unsupportedtype') {
                    usepkforcond = usepkforcond + 1
                }
            }
        });
        return usepkforcond;
    } catch (err) {
        throw "uvalidatetousepkcondition : " + err
    }
}
async function createupdatestr(columnNames) {
    try {
        var updatestr = ""
        counter = 1
        buffferupcond = 0
        columnNames.forEach((colName) => {
            if (buffferupcond == 1) {
                updatestr = updatestr + " , "
            }
            updatestr = updatestr + "\"" + colName + "\"= \$" + counter + " "
            buffferupcond = 1
            counter = counter + 1
        });
        return updatestr;
    } catch (err) {
        throw "createupdatestr : " + err
    }
}
async function db_datavalues_from_fetched_row(columnNames, row, dbname, tablename, datatypeobj) {
    try {
        const values = [];
        fieldname = '';
        if (config.has(`EXEMPTIONDATATYPE.MONEY.${dbname}_${tablename}`)) {
            fieldname = config.get(`EXEMPTIONDATATYPE.MONEY.${dbname}_${tablename}`)
        }
        columnNames.forEach((colName) => {
            if (row[colName]) {
                //Exemption function need to be integrated
                if (fieldname == colName) {
                    row[colName] = row[colName].substr(1)
                }
                if ( datatypeobj[colName] == 'varchar' ) {
                    if (checkdataishex(row[colName])) {
                        row[colName] = await converthextoutf(row[colName])
                    }
                }
                if (isUtf8(row[colName]) || datatypeobj[colName] == 'timestamp' || datatypeobj[colName] == 'date') {
                    console.log(`utf8 or datetime format ${colName}`);
                    values.push(row[colName]);
                } else {
                    values.push(new Buffer.from(row[colName], 'binary'));
                }
            } else {
                values.push(row[colName]);
            }
        });
        return values;
    } catch (err) {
        throw "db_datavalues_from_fetched_row : " + err
    }
}
async function db_datavalues_from_insert_datapayload(columnNames, payload) {
    try {
        const values = [];
        columnNames.forEach((colName) => {
            if (payload[colName].toUpperCase() == 'NULL') {
                values.push(null);
            } else {
                values.push(payload[colName]);
            }
        });
        return values;
    } catch (err) {
        throw "db_datavalues_from_insert_datapayload : " + err
    }
}
async function db_datavalues_from_update_datapayload(columnNames, payload) {
    try {
        const values = [];
        columnNames.forEach((colName) => {
            colobj = payload[colName]
            if (colobj['new'].toUpperCase() == 'NULL') {
                values.push(null);
            } else {
                values.push(colobj.new);
            }
        });
        return values;
    } catch (err) {
        throw "db_datavalues_from_update_datapayload : " + err
    }
}
async function hextoutf_insertpayload(columnNames, datatypeobj, payload) {
    try {
        columnNames.forEach((colName) => {
            console.log(`colName : ${colName}`)
            if (payload[colName] != 'unsupportedtype') {
                if (datatypeobj[colName] == 'varchar' && payload[colName].toUpperCase() != 'NULL') {
                    if (checkdataishex(payload[colName])) {
                        payload[colName] = await converthextoutf(payload[colName])
                    }
                }
            }
        });
        return payload;
    } catch (err) {
        throw "hextoutf_insertpayload : " + err
    }
}
async function hextoutf_updatepayload(columnNames, datatypeobj, payload) {
    try {
        columnNames.forEach((colName) => {
            //colobj = payload[colName]
            payload[colName]['new']
            payload[colName]['old']
            if (payload[colName]['old'] != 'unsupportedtype') {
                if (datatypeobj[colName] == 'varchar' && payload[colName]['old'].toUpperCase() != 'NULL') {
                    if (checkdataishex(payload[colName]['old'])) {
                        payload[colName]['old'] =await converthextoutf(payload[colName]['old'])
                    }
                }
            }
            if (payload[colName]['new'] != 'unsupportedtype') {
                if (datatypeobj[colName] == 'varchar' && payload[colName]['new'].toUpperCase() != 'NULL') {
                    if (checkdataishex(payload[colName]['new'])) {
                        payload[colName]['new'] = await converthextoutf(payload[colName]['new'])
                    }
                }
            }

        });
        return payload;
    } catch (err) {
        throw "hextoutf_updatepayload : " + err
    }
}
module.exports = {
    pgfetchdatatype,
    pgfetchprimarykey,
    checkdataishex,
    converthextoutf,
    ivalidateexemptiondatatype,
    uvalidateexemptiondatatype,
    updateretrivalcondition_withpk,
    insertretrivalcondition_withpk,
    updatedatacondition_withpk,
    updateretrivalcondition_withoutpk,
    insertretrivalcondition_withoutpk,
    updatedatacondition_withoutpk,
    updatesetdatastr,
    deletedatacondition_withoutpk,
    deletedatacondition_withpk,
    ivalidatetousepkcondition,
    uvalidatetousepkcondition,
    createupdatestr,
    db_datavalues_from_fetched_row,
    db_datavalues_from_insert_datapayload,
    db_datavalues_from_update_datapayload,
    hextoutf_insertpayload,
    hextoutf_updatepayload
}