#!/usr/bin/python
# -*- coding:utf-8 -*-

import pika
from MySQLdb.cursors import DictCursor
from twisted.application import service
from twisted.web.server import Site
from twisted.internet import endpoints, reactor
from twisted.python import log
from resource.orderResource import orderResource
from utils import getMysqlConfig,getRabbitmqServerConfig,getMqQueueConfig
from libs.mySqlConnectionPool import reconnectingMySQLConnectionPool

class apiService(service.MultiService):
    def __init__(self, bindAddress='0.0.0.0', port=80, games=[] ):
        service.MultiService.__init__(self)
        self.host = bindAddress
        self.port = port
        self.games = games
        self.dbPools = {}
        self.mqPools = {}
    def getDBPools(self, games):
        dbPools = {}
        def makeConnection(dbConfig={}):
            host = dbConfig.get('host')
            user = dbConfig.get('user')
            passwd = dbConfig.get('password')
            db = dbConfig.get('database')
            port = dbConfig.get('port')
            charset = dbConfig.get('charset')
            dbpool = reconnectingMySQLConnectionPool('MySQLdb', host=host, user=user, cursorclass=DictCursor,
                                           passwd=passwd, db=db, port=port, charset=charset, autocommit=False,
                                           cp_reconnect=True)
            return dbpool
        for game in games:
            gameDbConfig = getMysqlConfig(game)
            dbPools[game] = makeConnection(gameDbConfig)
            log.msg('[apiService, %s] mysql connection pools init' % (game, ) )
        return dbPools

    def getMqPools(self, games):
        mqPools = {}
        mqServerConfig = getRabbitmqServerConfig()
        userCredentials = pika.PlainCredentials(username=mqServerConfig.get('user'), password=mqServerConfig.get('password'))

        for game in games:
            queueConfig = getMqQueueConfig(game)
            connParam = pika.ConnectionParameters(host=mqServerConfig.get('host'), port=mqServerConfig.get('port'), credentials=userCredentials, virtual_host=queueConfig.get('vhost'), heartbeat_interval=0)
            mqConn = pika.BlockingConnection(parameters=connParam)
            mqPools[game] = mqConn
            log.msg('[apiService, %s] rabbitmq connection init ' % (game,))
        return mqPools

    def initServer(self):
        self.dbPools = self.getDBPools(self.games)
        self.mqPools = self.getMqPools(self.games)

    def startService(self):
        log.msg('[apiService] apiService start')
        log.msg('[apiService] apiService bind address:', '%s:%s' % ( self.host, self.port ))
        self.initServer()
        _orderResource = orderResource(dbPools=self.dbPools, mqPools=self.mqPools).app.resource()
        factory = Site(_orderResource)
        endpoint = endpoints.TCP4ServerEndpoint(reactor, self.port)
        endpoint.listen(factory)
        log.msg('[apiService] start done')