/**
 * Schema for consumer_log.
 */
module.exports = (sequelize, DataTypes) =>
  sequelize.define('consumer_log', {
    SEQ_ID: { type: DataTypes.STRING, primaryKey: true },
    CONSUMER_UPDATE_RETRY_COUNT:{ type: DataTypes.INTEGER ,defaultValue: 0, primaryKey: true },    
    CONSUMER_PAYLOAD: { type: DataTypes.JSON, allowNull: false },
    CONSUMER_FAILURE_LOG: { type: DataTypes.JSON }
  }, {
    tableName: 'consumer_log',
    paranoid: true,
    timestamps: false,
  });
