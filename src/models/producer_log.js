/**
 * Schema for producer_log.
 */
module.exports = (sequelize, DataTypes) =>
  sequelize.define('producer_log', {
    SEQ_ID: { type: DataTypes.INTEGER, autoIncrement:true, primaryKey: true },
    TOPICNAME: { type: DataTypes.STRING, allowNull: false },
    SOURCE: { type: DataTypes.STRING, allowNull: false },
    SCHEMA_NAME:{ type: DataTypes.STRING, allowNull: false },
    TABLE_NAME:{ type: DataTypes.STRING, allowNull: false },
    PRODUCER_PAYLOAD: { type: DataTypes.JSON, allowNull: false },
    OPERATION: { type: DataTypes.STRING, allowNull: false }
  }, {
    tableName: 'producer_log',
    paranoid: true,
    timestamps: false,
  });
