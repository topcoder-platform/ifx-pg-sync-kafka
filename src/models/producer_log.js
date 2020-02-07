/**
 * Schema for producer_log.
 */
module.exports = (sequelize, DataTypes) =>
  sequelize.define('producer_log', {
    SEQ_ID: { type: DataTypes.STRING, primaryKey: true },
    PODUCER_PUBLISH_RETRY_COUNT:{ type: DataTypes.INTEGER ,defaultValue: 0, primaryKey: true },
    PRODUCER_PAYLOAD: { type: DataTypes.JSON, allowNull: false },
    PRODUCER_FAILURE_LOG: { type: DataTypes.JSON }    
  }, {
    tableName: 'producer_log',
    paranoid: true,
    timestamps: false,
  });
