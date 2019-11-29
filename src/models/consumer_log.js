/**
 * Schema for consumer_log.
 */
module.exports = (sequelize, DataTypes) =>
  sequelize.define('consumer_log', {
    SEQ_ID: { type: DataTypes.INTEGER, primaryKey: true },
    TOPICNAME: { type: DataTypes.STRING, allowNull: false },
    SCHEMA_NAME:{ type: DataTypes.STRING, allowNull: false },
    DESTINATION:{ type: DataTypes.STRING, allowNull: false },
    CONSUMAER_QUERY: { type: DataTypes.JSON, allowNull: false }
  }, {
    tableName: 'consumer_log',
    paranoid: true,
    timestamps: false,
  });
