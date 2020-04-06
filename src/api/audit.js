const models = require('../models')
const model=models.auditlog
const Joi = require('joi')
  

producerLog.schema = Joi.object().keys({
  SEQ_ID: Joi.string().required(),
  PODUCER_PUBLISH_RETRY_COUNT: Joi.number(),
  PRODUCER_PAYLOAD: Joi.object(),
  PRODUCER_FAILURE_LOG: Joi.object()
})

//add producer_log = used for data update about producer received
function producerLog(payload) {
  const result = Joi.validate(payload, producerLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Producer log create ' + result.error)
    })
  }
  return  model.producer_log.create(payload)
}

//update producer_log = used for failure update in case of success, it will not be executed
function producerLog_update(payload) {
  const result = Joi.validate(payload, producerLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Producer log create ' + result.error)
    })
  }
  //return  model.producer_log.update(payload)
  const { SEQ_ID,PODUCER_PUBLISH_RETRY_COUNT, ...change } = payload
  return model.producer_log.update(change, { where: { SEQ_ID: payload.SEQ_ID, PODUCER_PUBLISH_RETRY_COUNT : payload.PODUCER_PUBLISH_RETRY_COUNT  }})  
}



pAuditLog.schema = Joi.object().keys({
  SEQ_ID: Joi.string().required(),
  REQUEST_CREATE_TIME: Joi.date(),
  PRODUCER_PAYLOAD: Joi.object(),
  PRODUCER_PUBLISH_STATUS: Joi.string().valid('success','failure'),
  PRODUCER_FAILURE_LOG: Joi.object(),
  PRODUCER_PUBLISH_TIME: Joi.date(),
  PODUCER_PUBLISH_RETRY_COUNT: Joi.number(),
  OVERALL_STATUS: Joi.string(),
  RECONCILE_STATUS: Joi.number()
})

//add audit_log about the producer details 
function pAuditLog(payload) {
  const result = Joi.validate(payload, pAuditLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Audit' + result.error)
    })
  }
  return model.audit_log.create(payload)
}

//updated audit_log about the producer status
function pAuditLog_update(payload) {
  const result = Joi.validate(payload, pAuditLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Audit' + result.error)
    })
  }
  //return model.audit_log.create(payload)
  const { SEQ_ID, ...change } = payload
  return model.audit_log.update(change, { where: { SEQ_ID: payload.SEQ_ID }})    
}

consumerLog.schema = Joi.object().keys({
  SEQ_ID: Joi.string().required(),
  CONSUMER_UPDATE_RETRY_COUNT: Joi.number(),
  CONSUMER_PAYLOAD: Joi.object(),
  CONSUMER_FAILURE_LOG: Joi.object()
})

//add consumer_log = Entering received record
function consumerLog(payload) {
  const result = Joi.validate(payload, consumerLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Consumer' + result.error)
    })
  }

  return  model.consumer_log.create(payload)
}

//update consumer_log = used for failure log update 
function consumerLog_update(payload) {
  const result = Joi.validate(payload, consumerLog.schema)
  if(result.error !== null) {
    return Promise.resolve().then(function () {
      throw new Error('Consumer' + result.error)
    })
  }
  //return  model.consumer_log.create(payload)
  const { SEQ_ID,CONSUMER_UPDATE_RETRY_COUNT, ...change } = payload
  return model.consumer_log.update(change, { where: { SEQ_ID: payload.SEQ_ID, CONSUMER_UPDATE_RETRY_COUNT : payload.CONSUMER_UPDATE_RETRY_COUNT  }})    
}

cAuditLog.schema = Joi.object().keys({
  SEQ_ID: Joi.string().required(),
  CONSUMER_PAYLOAD: Joi.object(),
  CONSUMER_DEPLOY_STATUS: Joi.string().valid('success','failure'),
  CONSUMER_FAILURE_LOG: Joi.object(),
  CONSUMER_UPDATE_TIME: Joi.date(),
  CONSUMER_UPDATE_RETRY_COUNT: Joi.number(),
  OVERALL_STATUS: Joi.string()
})

//add audit_log = only update is possible
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
  producerLog_update,
  pAuditLog,
  pAuditLog_update,
  consumerLog,
  consumerLog_update,
  cAuditLog
}
