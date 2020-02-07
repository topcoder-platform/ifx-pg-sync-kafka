/**
 * Schema for audit_log.
 */
module.exports = (sequelize, DataTypes) => 
  sequelize.define('audit_log', {  
    SEQ_ID: { type: DataTypes.STRING, primaryKey: true },
    PAYLOAD_TIME: { type: DataTypes.DATE },
    PRODUCER_PAYLOAD: { type: DataTypes.JSON, allowNull: false },
    PRODUCER_PUBLISH_STATUS: { type: DataTypes.STRING },
    PRODUCER_FAILURE_LOG: { type: DataTypes.JSON },
    PRODUCER_PUBLISH_TIME: { type: DataTypes.DATE },
    PODUCER_PUBLISH_RETRY_COUNT:{ type: DataTypes.INTEGER ,defaultValue: 0 },    
    CONSUMER_PAYLOAD: { type: DataTypes.JSON, allowNull: false },    
    CONSUMER_DEPLOY_STATUS: { type: DataTypes.STRING },
    CONSUMER_FAILURE_LOG: { type: DataTypes.JSON },
    CONSUMER_UPDATE_TIME:{ type: DataTypes.DATE },
    CONSUMER_UPDATE_RETRY_COUNT:{ type: DataTypes.INTEGER ,defaultValue: 0 },
    OVERALL_STATUS:{ type: DataTypes.STRING },
    RECONCILE_STATUS:{ type: DataTypes.INTEGER },
    MISC:{ type: DataTypes.STRING }            
  }, {
    tableName: 'audit_log',
    paranoid: true,
    timestamps: false,
  });


