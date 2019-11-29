/**
 * Schema for audit_log.
 */
module.exports = (sequelize, DataTypes) => 
  sequelize.define('audit_log', {
    SEQ_ID: { type: DataTypes.INTEGER, primaryKey: true },
    PRODUCER_PUBLISH_STATUS: { type: DataTypes.STRING },
    PRODUCER_FAILURE_LOG: { type: DataTypes.JSON },
    PRODUCER_PUBLISH_TIME: { type: DataTypes.DATE },
    CONSUMER_DEPLOY_STATUS: { type: DataTypes.STRING },
    CONSUMER_FAILURE_LOG: { type: DataTypes.JSON },
    CONSUMER_UPDATE_TIME:{ type: DataTypes.DATE }
  }, {
    tableName: 'audit_log',
    paranoid: true,
    timestamps: false,
  });
