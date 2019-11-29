/**
 * Schema for test
 */
module.exports = (sequelize, DataTypes) => {
  return   sequelize.define('test', {
      id: { type: DataTypes.INTEGER, primaryKey: true },
      name: { type: DataTypes.STRING },
      longvarchar: { type: DataTypes.STRING },
      longlvarchar: { type: DataTypes.STRING },
      intcol: { type: DataTypes.INTEGER },
      floatcol:{ type: DataTypes.FLOAT },
      blobcol: { type: DataTypes.BLOB },
      clobcol: { type: DataTypes.STRING.BINARY}
    }, {
      tableName: 'test',
      paranoid: true,
      timestamps: false,
    })
}
