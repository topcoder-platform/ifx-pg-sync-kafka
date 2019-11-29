const config = require('config');
const _ = require('underscore');
const { Pool } = require('pg');
//const { Pool } = require('pg').native; // use native binding to have better performance you may use pure js binding require('pg') directly here
const createPool = (database) => !database ? new Pool(config.POSTGRES): new Pool(_.extend(config.POSTGRES, {database}));

module.exports = {
    createPool
};
