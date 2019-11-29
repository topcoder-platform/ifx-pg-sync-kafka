const models = require('../models')
const model=models.auditlog
const Joi = require('joi')

producerLog.schema = Joi.object().keys({
  TOPICNAME: Joi.string().required(),
  SOURCE: Joi.string().required(),
  SCHEMA_NAME: Joi.string().required(),
  TABLE_NAME: Joi.string().required(),
  PRODUCER_PAYLOAD: Joi.object().required(),
  OPERATION: Joi.string().valid('INSERT','UPDATE','DELETE').required()
})

//add producer_log
function producerLog(payload) {
  const result = Joi.validate(payload, producerLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Producer' + result.error)
    })
  }

  return  model.producer_log.create(payload)
}

pAuditLog.schema = Joi.object().keys({
  SEQ_ID: Joi.number().required(),
  PRODUCER_PUBLISH_STATUS: Joi.string().valid('success','failure').required(),
  PRODUCER_FAILURE_LOG: Joi.object(),
  PRODUCER_PUBLISH_TIME: Joi.date().required()
})

//add audit_log
function pAuditLog(payload) {
  const result = Joi.validate(payload, pAuditLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Audit' + result.error)
    })
  }
  return model.audit_log.create(payload)
}

consumerLog.schema = Joi.object().keys({
  SEQ_ID: Joi.number().required(),
  TOPICNAME: Joi.string().required(),
  SCHEMA_NAME: Joi.string().required(),
  CONSUMAER_QUERY: Joi.object().required(),
  DESTINATION: Joi.string().required()
})

//add consumer_log
function consumerLog(payload) {
  const result = Joi.validate(payload, consumerLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Consumer' + result.error)
    })
  }

  return  model.consumer_log.create(payload)
}

cAuditLog.schema = Joi.object().keys({
  SEQ_ID: Joi.number().required(),
  CONSUMER_DEPLOY_STATUS: Joi.string().valid('success','failure').required(),
  CONSUMER_FAILURE_LOG: Joi.object(),
  CONSUMER_UPDATE_TIME: Joi.date().required()
})

//add audit_log
function cAuditLog(payload) {
  const result = Joi.validate(payload, cAuditLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Audit' + result.error)
    })
  }

  const { SEQ_ID, ...change } = payload
  return model.audit_log.update(change, { where: { SEQ_ID: payload.SEQ_ID }})
}

module.exports = {
  producerLog,
  pAuditLog,
  consumerLog,
  cAuditLog
}
