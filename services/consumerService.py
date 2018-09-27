#!/usr/bin/python
# -*- coding:utf-8 -*-
import pika
import _mysql_exceptions
import traceback
import urlparse
from httplib import HTTPConnection
from MySQLdb.cursors import DictCursor
from pika.adapters import twisted_connection
from twisted.application import service
from twisted.internet import protocol,reactor,defer,task
from twisted.python import log
from utils import *
from libs.mySqlConnectionPool import reconnectingMySQLConnectionPool



class consumerService(service.MultiService):

    def __init__(self, games=[]):
        service.MultiService.__init__(self)
        self.dbPools = {}
        self.games = games

    def initServer(self):
        self.dbPools = self.getDBPools(self.games)

    def getDBPools(self, games):
        dbPools = {}
        def makeConnection(dbConfig={}):
            host = dbConfig.get('host')
            user = dbConfig.get('user')
            passwd = dbConfig.get('password')
            db = dbConfig.get('database')
            port = dbConfig.get('port')
            charset = dbConfig.get('charset')
            # log.msg(dbConfig)
            dbpool = reconnectingMySQLConnectionPool('MySQLdb', host=host, user=user,
                                           cursorclass=DictCursor,
                                           passwd=passwd, db=db, port=port, charset=charset, autocommit=False,
                                           cp_reconnect=True)
            return dbpool
        for game in games:
            gameDbConfig = getMysqlConfig(game)
            dbPools[game] = makeConnection(gameDbConfig)
            log.msg('[consumerService, %s] mysql connection pools init' % (game,))
        return dbPools

    def orderProcess(self, txn, game, tradeNo, agentUid):
        log.msg('[consumerService, %s] %s: orderProcess agentUid = %s' % (game, tradeNo, agentUid))
        ret = None
        tradeGoodsTbName = getAppConfig('%s.table.trade_goods' % (game,))
        tradeOrderTbName = getAppConfig('%s.table.trade_order' % (game,))
        agentUserTbName = getAppConfig('%s.table.agent_user' % (game,))
        qUserTbName = getAppConfig('%s.table.q_user' % (game,))
        tradeChargeTbName = getAppConfig('%s.table.trade_charge' % (game,))
        qTradeChargeTbName = getAppConfig('%s.table.q_trade_charge' % (game,))
        txn.execute('SELECT tradeno, coin, uid,goodid,order_status,tradetype,pagentid,amount,appuname FROM ' + tradeOrderTbName + ' WHERE tradeno = %s;', ( tradeNo, ))
        orderData = txn.fetchone()
        if orderData is not None and orderData.get('order_status') == 0:
            orderCoin = orderData.get('coin')
            orderAmount = orderData.get('amount')
            orderUid = orderData.get('uid')
            orderGoodId = orderData.get('goodid')
            orderStatus = orderData.get('order_status')
            orderTradeType = orderData.get('tradetype')
            orderAgentId = orderData.get('pagentid')
            orderAppUname = orderData.get('appuname')

            txn.execute('SELECT coin,level FROM ' + agentUserTbName + ' WHERE uid = %s;', ( agentUid, ))
            agentUserData = txn.fetchone()
            if agentUserData is not None and agentUserData.get('coin') > orderCoin:
                agentUserTotalCoin = agentUserData.get('coin')
                agentUserLevel = agentUserData.get('level')

                txn.execute('SELECT username,uid,agentid,coin,isupuser FROM ' + qUserTbName + ' WHERE agentid = %s', (agentUid, ))
                qUserData = txn.fetchone()
                txn.execute('SELECT price,description FROM ' + tradeGoodsTbName + ' WHERE autoid = %s;', (orderGoodId, ))
                productData = txn.fetchone()
                if qUserData is not None and productData is not None:

                    agentUserInGameName = qUserData.get('username')
                    agentUserInGameUid = qUserData.get('uid')
                    agentUserInGameCoin = qUserData.get('coin')
                    agentUserInGameIsUpUser = qUserData.get('isupuser')

                    productPrice = productData.get('price')
                    productDesc = productData.get('description')
                    gameId = getGameIdBySymbol(game)
                    #1、扣款
                    remainCoin = agentUserTotalCoin - orderCoin
                    txn.execute('UPDATE ' + agentUserTbName + ' SET coin = %s WHERE uid = %s', (remainCoin, agentUid))
                    #2、记录扣款流水
                    chargeSql = 'INSERT INTO ' + tradeChargeTbName + '(`uid`, `reuid`, `reappuname`, `serialno`, `tradeno`, `gameid`, `tradeaction`, `begincoin`, `tradecoin`, `endcoin`, `totalamount`, `tradedesc`, `type`)  VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    tradeChargeSerialno = "%s%s%s" % (agentUid, gameId, int(time.time()))
                    txn.execute(chargeSql, (agentUid, orderUid, agentUserInGameName, tradeChargeSerialno, tradeNo, gameId, 'PAYMENT', agentUserTotalCoin, -orderCoin, remainCoin,
                                            orderCoin, "购买%s" % (productDesc,), 2))
                    #3、支付完成更新订单数据
                    timeEnd = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
                    updateTradeOrderSql = 'UPDATE ' + tradeOrderTbName + ' SET order_status = 1,cash_coin=%s,time_end=%s,transaction_id=%s WHERE tradeno = %s'
                    txn.execute(updateTradeOrderSql, (orderCoin, timeEnd, '', tradeNo))

                    #4、增加用户钻石
                    txn.execute('UPDATE ' + qUserTbName + ' SET coin = coin + %s, sumcoin = sumcoin + %s WHERE username = %s ', ( orderAmount, int(orderCoin/100), orderAppUname))

                    #5、增加用户钻石流水记录
                    isBindAgent = False
                    if orderAgentId > 0 and str(orderTradeType).upper() != 'APPLE':
                        isBindAgent = True
                    qTradeChargeSerialno = "%s%s%s" % (orderAppUname, gameId, int(time.time()))
                    txn.execute('INSERT INTO ' + qTradeChargeTbName + '(`uid`, `appuname`, `serialno`, `tradeno`, `gameid`, `tradeaction`, `begincoin`, `tradecoin`, `endcoin`, `tradedesc`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                (agentUserInGameUid, orderAppUname, qTradeChargeSerialno, tradeNo, gameId, 'RECHARGE', agentUserInGameCoin - orderAmount, orderAmount, agentUserInGameCoin, '玩家充值' if isBindAgent else '未绑定推荐人,玩家充值'))
                    #6、计算返利
                    ret = {
                        'uid': agentUserInGameUid,
                        'currcoin': agentUserInGameCoin,
                        'num': orderAmount,
                        'gameid': gameId,
                        'order_coin': orderCoin,
                        'bind_agent': isBindAgent,
                        'app_uname': orderAppUname,
                        'pagentid': orderAgentId,
                        'agent_level': agentUserLevel,
                        'agent_is_up': agentUserInGameIsUpUser
                    }

        if ret is None:
            log.msg('[consumerService, %s] %s: orderProcess ret is None agentUid = %s' % (game, tradeNo, agentUid), orderData)
        return ret

    def orderProcessComplete(self, result, game, tradeNo):
        gameId = getGameIdBySymbol(game)
        notifyRetryCnt = 10
        retry = 0
        agentTypes = [ '区域', '一级', '二级', '三级' ]
        log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete : ' % (game, tradeNo), result)
        def notifyNodeServer(result):
            ret = False
            notifyPayload = dictToJson(result)
            log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:notifyNodeServer parms: ' % (game, tradeNo), notifyPayload)
            try:
                nodeHttpServer = urlparse.urlparse(getAppConfig('node.httpserver', orderServerOption.get('env')))
                nodeHttpHost = nodeHttpServer.hostname
                nodeHttpPort = nodeHttpServer.port
                httpClient = HTTPConnection(host=nodeHttpHost, port=nodeHttpPort)
                endPoint = "/api/games/reCharge"
                httpClient.request('POST', endPoint, notifyPayload, headers={'Content-Type':'application/json'})
                response = httpClient.getresponse()
                log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:notifyNodeServer : ' % (game, tradeNo), response.status, response.reason)
                log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:notifyNodeServer : ' % (game, tradeNo), response.read())
                httpClient.close()
                ret = True
            except Exception,e:
                log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:notifyNodeServer fail: ' % (game, tradeNo), e.messag)
                log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:notifyNodeServer trace: ' % (game, tradeNo), e)
            return ret

        def calculateCommission(txn, game, orderCoin, orderAppUname, pagentid, agentIsUp, tradeNo):
            agentId = int(pagentid) if pagentid is not None else None
            agentUserTbName = getAppConfig('%s.table.agent_user' % (game,))
            tradeChargeTbName = getAppConfig('%s.table.trade_charge' % (game,))
            isOwner = True
            childCommission = 0
            log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:calculateCommission : agentid =  ' % (game, tradeNo), agentId)
            while agentId is not None:
                log.msg('[consumerService, %s] %s: orderProcess:orderProcessComplete:calculateCommission : agentid =  ' % (game, tradeNo), agentId)
                txn.execute('SELECT uid,puid,level,commission,oldpcode,tooldcom,coin,sumcoin FROM ' +  agentUserTbName + ' WHERE uid = %s', (agentId, ))
                agentUserData = txn.fetchone()
                if agentUserData is None or ( agentUserData is not None and agentUserData.get('level') < 1 ):
                    break
                else:
                    agentUserId = agentUserData.get('uid')
                    agentUserParentId = agentUserData.get('puid')
                    agentUserLevel = agentUserData.get('level')
                    agentUserCommissionRate = agentUserData.get('commission')
                    agentUserParentInvitCode = agentUserData.get('oldpcode')
                    agentUserTooldcom = agentUserData.get('tooldcom')
                    agentUserTotalCoin = agentUserData.get('coin')
                    agentUserTotalSumCoin = agentUserData.get('sumcoin')
                    isOldUpAgent = False

                    if True == isOwner and 2 == agentUserLevel and agentUserParentInvitCode != 0 and agentUserTooldcom > 0 and agentIsUp == 0:
                        txn.execute('SELECT uid FROM '+ agentUserTbName + ' WHERE oldpcode = %s', ( agentUserParentInvitCode, ))
                        agentUserParentData = txn.fetchone()
                        if agentUserParentData is not None:
                            agentUserCommissionRate = agentUserCommissionRate - agentUserTooldcom
                            agentUserParentId = agentUserParentData.get('uid')
                            isOldUpAgent = True
                    actualCommissionRate = agentUserCommissionRate - childCommission
                    if actualCommissionRate < 0:
                        raise _mysql_exceptions.DatabaseError('actualCommissionRate < 0')
                    else:
                        actualCommissionAmount = actualCommissionRate * orderCoin
                        childCommission = childCommission + actualCommissionRate
                        agentUserFinallyCoin = agentUserTotalCoin + actualCommissionAmount
                        agentUserFinallySumCoin = agentUserTotalSumCoin + actualCommissionAmount
                        txn.execute('UPDATE ' + agentUserTbName + ' SET coin = %s, sumcoin = %s WHERE uid = %s', ( agentUserFinallyCoin, agentUserFinallySumCoin, agentUserId ))
                        serialNo = "%s%s%s" % (pagentid, gameId, int(time.time()))
                        tradedesc = "%s%s代理返佣" % ('老' if isOldUpAgent else '', agentTypes[agentUserLevel])
                        txn.execute('INSERT INTO ' + tradeChargeTbName + '(`uid`, `reappuname`, `serialno`, `tradeno`, `gameid`, `tradeaction`, `begincoin`, `tradecoin`, `endcoin`, `totalamount`, `commrate`, `tradedesc`, `type`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                    (agentId, orderAppUname, serialNo, tradeNo, gameId, 'REBATE', agentUserFinallyCoin - actualCommissionAmount, actualCommissionAmount, agentUserFinallyCoin, orderCoin, actualCommissionRate,
                                     tradedesc, 0 if isOwner else 1))
                    agentId = agentUserParentId

        if result is not None:
            isBindAgent = result.get('bind_agent')
            orderCoin = result.get('order_coin')
            orderAppUname = result.get('app_uname')
            pagentid = result.get('pagentid')
            agentIsUp = result.get('agent_is_up')

            gemeUserId = result.get('uid')
            gameUserCoin = result.get('currcoin')
            orderAmount = result.get('num')

            notifyData = {
                'uid': gemeUserId,
                'currcoin': gameUserCoin,
                'num': orderAmount,
                'gameid': gameId
            }
            #通知node服务器充值完成
            while retry < notifyRetryCnt:
                notifyRet = notifyNodeServer(notifyData)
                if True == notifyRet:
                    break
                retry += 1

            #绑定代理，充值返利
            if isBindAgent:
                dbDerfer = self.dbPools[game].runInteraction(calculateCommission, game, orderCoin, orderAppUname, pagentid, agentIsUp, tradeNo)

        else:
            self.orderProcessError(None, game, tradeNo)





    def orderProcessError(self, error, game, tradeNo):
        if tradeNo is not None and len(tradeNo) > 0:
            dbDefer = self.dbPools[game].runQuery('UPDATE o_trade_order SET order_status = -1 WHERE tradeno = %s;', (tradeNo, ))
            dbDefer.addCallback(lambda x:log.msg("tradeNo: %s update order_status = -1 success " % (tradeNo,)))
            dbDefer.addErrback(lambda x:log.msg("tradeNo: %s update order_status = -1 fail: " % (tradeNo)))
            log.msg('[consumerService, %s] %s: orderProcess:orderProcessError: ' % (game, tradeNo), error)
        else:
            log.msg('[consumerService, %s] %s: orderProcess:orderProcessError: tradeNo is empty ' % (game, tradeNo), error)

    @defer.inlineCallbacks
    def run(self, connection, game=None, exchangeName=None, queueName=None, routingKey=None):
        # log.msg("game = %s exchangeName = %s queueName = %s routingKey = %s " % ( game, exchangeName, queueName, routingKey ))
        channel = yield connection.channel()
        exchange = yield channel.exchange_declare(exchange=exchangeName, durable=True)
        queue = yield channel.queue_declare(queue=queueName, durable=True)
        yield channel.queue_bind(exchange=exchangeName, queue=queueName, routing_key=routingKey)
        yield channel.basic_qos(prefetch_count=1)
        queue_object, consumer_tag = yield channel.basic_consume(queue=queueName, no_ack=False)
        l = task.LoopingCall(self.orderTask, queue_object, game)
        l.start(0.01)



    @defer.inlineCallbacks
    def orderTask(self, queue_object, game):
        ch, method, properties, body = yield queue_object.get()
        if body:
            log.msg('[consumerService, %s] orderTask :' % (game,), body)
            orderData = strToDict(body)
            if orderData is not None:
                tradeNo = orderData.get('tradeNo')
                agentUid = orderData.get('agentUid')
                dbDerfer = self.dbPools[game].runInteraction(self.orderProcess, game, tradeNo, agentUid)
                dbDerfer.addCallback(self.orderProcessComplete, game, tradeNo)
                dbDerfer.addErrback(self.orderProcessError, game, tradeNo)
        yield ch.basic_ack(delivery_tag=method.delivery_tag)

    def startService(self):
        self.initServer()
        mqServerConfig = getRabbitmqServerConfig()
        userCredentials = pika.PlainCredentials(username=mqServerConfig.get('user'), password=mqServerConfig.get('password'))
        for game in self.games:
            queueConfig = getMqQueueConfig(game)
            vhost = queueConfig.get('vhost')
            exchange = queueConfig.get('exchange')
            queueName = queueConfig.get('queueName')
            routingKey = queueConfig.get('routingKey')
            connParam = pika.ConnectionParameters(host=mqServerConfig.get('host'), port=mqServerConfig.get('port'), credentials=userCredentials, virtual_host=queueConfig.get('vhost'), heartbeat_interval=0)
            mqConn = protocol.ClientCreator(reactor, twisted_connection.TwistedProtocolConnection, connParam)
            mqConnDefer = mqConn.connectTCP(mqServerConfig.get('host'), mqServerConfig.get('port'))
            mqConnDefer.addCallback(lambda protocol: protocol.ready)
            mqConnDefer.addCallback(self.run,  game=game, exchangeName=exchange, queueName=queueName, routingKey=routingKey)
            log.msg('[consumerService, %s] rabbitmq connection pools init' % (game,))
        log.msg('[consumerService] start done')