const ifxcore = require('./src/common/informix_ifxnjs')
async function ab()
{
const connection = await ifxcore.getInformixConnection('jive')
connection.closeAsync();
}
ab()