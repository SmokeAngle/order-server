#!/usr/bin/python
# -*- coding:utf-8 -*-

from twisted.python import log
from klein import Klein
from utils import *


class orderResource(object):
    app = Klein()

    def __init__(self, dbPools, mqPools):
        self.dbPools = dbPools
        self.mqPools = mqPools

    @app.route('/<path:catchall>')
    def catchAll(self, request, catchall):
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(404)
        return responseJson(
            API_RESPONSE_CODE_ENDPOINT_NOT_EXISTS,
            API_RESPONSE_MSG.get(API_RESPONSE_CODE_ENDPOINT_NOT_EXISTS)
        )

    @app.route('/')
    def index(self, request):
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(404)
        return responseJson(
            API_RESPONSE_CODE_SUCCESS,
            API_RESPONSE_MSG.get(API_RESPONSE_CODE_SUCCESS)
        )

    @app.route('/order/create/<string:name>', methods=['POST'])
    def create(self, request, name):

        def createOrderSuccess(result, tradeNo):
            if False != result:
                log.msg('[apiService, %s] %s: order create success' % ( name, tradeNo, ))
                publishData = { 'agentUid': result[0], 'tradeNo': result[1]}
                self.publishOrderData(name, tradeNo, dictToJson(publishData))
            else:
                log.msg('[apiService, %s] %s: order create success' % (name, tradeNo,))
                log.msg('[apiService, %s] %s: result:' % (name, tradeNo,), result)

        def createOrderFail(error, tradeNo):
            log.msg('[apiService, %s] %s: order create fail' % (name, tradeNo,))
            log.msg('[apiService, %s] %s: error:' % (name, tradeNo,), error)


        def createOrder( txn, productId, agentUid, tradeNo):
            ret = False
            tradeGoodsTbName = getAppConfig('%s.table.trade_goods' % ( name, ))
            tradeOrderTbName = getAppConfig('%s.table.trade_order' % (name,))
            agentUserTbName = getAppConfig('%s.table.agent_user' % (name,))
            qUserTbName = getAppConfig('%s.table.q_user' % (name,))
            txn.execute('SELECT name,price,amount,bonus,gameid FROM ' + tradeGoodsTbName +  ' WHERE autoid = %s AND status = 1 AND level = 2', ( productId, ))
            goodsData = txn.fetchone()
            if goodsData is not None:
                txn.execute('SELECT u.uid, u.username, m.uid AS pagentid, m.code AS pcode FROM ' + agentUserTbName + ' AS m INNER JOIN '+ qUserTbName +' AS u ON u.agentid = m.uid WHERE m.uid = %s', ( agentUid, ))
                userData = txn.fetchone()
                if userData is not None:
                    txn.execute('INSERT INTO ' + tradeOrderTbName + '(`uid`, `appuname`, `goodid`, `tradeno`, `tradetype`, `coin`, `pagentid`, `pcode`, `amount`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (userData.get('uid'), userData.get('username'), productId, tradeNo, 'AGENTPAY', goodsData.get('price'),
                                 userData.get('pagentid'), userData.get('pcode'), goodsData.get('amount') + goodsData.get('bonus')))
                    ret = (agentUid, tradeNo)
            return ret

        tradeNo = "%s%s%s" % (10, getGameIdBySymbol(name), getTradeNo(),)
        log.msg('[apiService, %s] %s: order create parms' % (name, tradeNo,), request.args)
        parmsRule = {
            'agent_uid': {'name': requestValidation.RULE_NAME_REQUIRED, 'error' : API_RESPONSE_CODE_INVALID_AGENT_UID },
            'good_id': {'name': requestValidation.RULE_NAME_REQUIRED, 'error': API_RESPONSE_CODE_INVALID_PRODUCTID},
            'timestamp': {'name': requestValidation.RULE_NAME_NUMBER, 'error': API_RESPONSE_CODE_INVALID_TIMESTAMP },
            'sign': {'name': requestValidation.RULE_NAME_REQUIRED, 'error': API_RESPONSE_CODE_INVALID_SIGN }
        }
        gameId = getGameIdBySymbol(name)
        if gameId is None:
            ret = API_RESPONSE_CODE_INVALID_GAMEID
        else:
            _requestValidation = requestValidation(request, parmsRule)
            if  True == _requestValidation.validation():
                parmsSign = request.args.get('sign')[0]
                parms = request.args
                parms.pop('sign')
                if True != identifySign( parms, parmsSign ):
                    ret = API_RESPONSE_CODE_INVALID_SIGN
                else:
                    productId = request.args.get('good_id')[0]
                    agentUid = request.args.get('agent_uid')[0]
                    log.msg(self.dbPools)
                    dbDefer = self.dbPools[name].runInteraction(createOrder, productId, agentUid, tradeNo)
                    dbDefer.addCallback(createOrderSuccess, tradeNo)
                    dbDefer.addErrback(createOrderFail, tradeNo)
                    ret = API_RESPONSE_CODE_SUCCESS
            else:
                ret = _requestValidation.getErrorCode()

        request.setHeader('Content-Type', 'application/json')
        return responseJson(
            ret,
            API_RESPONSE_MSG.get(ret)
        )

    @app.route('/order/info/<string:name>')
    def info(self, request, name):
        request.setHeader('Content-Type', 'application/json')
        return responseJson(
            API_RESPONSE_CODE_SUCCESS,
            API_RESPONSE_MSG.get(API_RESPONSE_CODE_SUCCESS)
        )


    def publishOrderData(self, game, tradeNo, data):
        try:
            mqConn = self.mqPools[game]
            mqChannel = mqConn.channel()
            queueConfig = getMqQueueConfig(game)
            exchange = queueConfig.get('exchange')
            queueName = queueConfig.get('queueName')
            routingKey = queueConfig.get('routingKey')
            mqChannel.confirm_delivery()
            mqChannel.exchange_declare(exchange=exchange, durable=True)
            mqChannel.queue_declare(queue=queueName, durable=True)
            mqChannel.queue_bind(queue=queueName, exchange=exchange, routing_key=routingKey)
            mqChannel.publish(exchange=exchange, routing_key=routingKey, body=data)
            log.msg('[apiService, %s] %s: order publish success:' % (game, tradeNo,), data)
        except Exception,e:
            log.msg('[apiService, %s] %s: order publish fail: %s' % (game, tradeNo,), data, e.message)
            log.msg(e)
        finally:
            mqChannel.close()

