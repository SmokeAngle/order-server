#!/usr/bin/python
# -*- coding:utf-8 -*-
import json
import re
import time
import random
from pika import BlockingConnection,BasicProperties
from ConfigParser import ConfigParser
from twisted.python import log
from hashlib import md5
from const import *

from twisted.python.runtime import platformType
if platformType == "win32":
    from twisted.scripts._twistw import ServerOptions
else:
    from twisted.scripts._twistd_unix import ServerOptions

server_run_env = [ ENV_DEV, ENV_STG, ENV_PROD ]

configParser = ConfigParser()
configParser.read(APP_CONFIG)

class orderServerOptions(ServerOptions):
    optParameters = [
        ['env', 'e', ENV_DEV , "当前运行环境：[ %s ] "  % ( ','.join(server_run_env) ) ],
    ]

orderServerOption = orderServerOptions()

class rabbitmqClient(object):
    def __init__(self, exchange = "", routeKey = ""):
        self.connection = BlockingConnection()
        self.channel = self.connection.channel()
        self.exchange = exchange
        self.routeKey = routeKey

    def pushMsg(self, msg):
        self.channel.exchange_declare()
        self.channel.queue_declare()
        self.channel.basic_publish(self.channel, self.routeKey, msg, BasicProperties(content_type='text/plain', delivery_mode=1))




def getAppConfig( name = "", group = None ):
    groupName = 'common' if group is None else group
    if configParser.has_option(groupName, name):
        return configParser.get(groupName, name)
    return None

def getRabbitmqServerConfig():

    serverIp = getAppConfig("rabbitmq_server.ip", orderServerOption.get('env'))
    serverPort = getAppConfig("rabbitmq_server.port", orderServerOption.get('env'))
    serverUser = getAppConfig("rabbitmq_server.user", orderServerOption.get('env'))
    serverPassword = getAppConfig("rabbitmq_server.password", orderServerOption.get('env'))

    return {
        'host': serverIp,
        'port': int(serverPort),
        'user': serverUser,
        'password': serverPassword
    }

def getMqQueueConfig( game ):
    vHost = getAppConfig('%s.mq.vhost' % (game,))
    exchangeName = getAppConfig('%s.mq.order_exchange' % (game, ))
    queueName = getAppConfig('%s.mq.order_queue' % (game, ))
    routingKey = getAppConfig('%s.mq.order_route_key' % (game, ))


    return {
            'vhost': vHost,
            'exchange': exchangeName,
            'queueName': queueName,
            'routingKey': routingKey
    }

def getMysqlConfig( game ):
    currentEnv = orderServerOption.get('env')
    mysqlHost = getAppConfig('%s.mysql_server.host' % (game, ), currentEnv)
    mysqlPort = getAppConfig('%s.mysql_server.port' % (game,), currentEnv)
    mysqlUser = getAppConfig('%s.mysql_server.user' % (game,), currentEnv)
    mysqlPassword = getAppConfig('%s.mysql_server.password' % (game,), currentEnv)
    mysqlDefaultDabase = getAppConfig('%s.mysql_server.database' % (game,), currentEnv)
    mysqlClientCharset = getAppConfig('%s.mysql_server.charset' % (game,), currentEnv)
    return {
        'host': mysqlHost,
        'port': int(mysqlPort),
        'user': mysqlUser,
        'password': mysqlPassword,
        'database': mysqlDefaultDabase,
        'charset': mysqlClientCharset
    }


def responseJson(ret=API_RESPONSE_CODE_SUCCESS, msg=API_RESPONSE_MSG.get(API_RESPONSE_CODE_SUCCESS) ,extraData={} ):
    ret = {'ret': ret, 'msg': msg}
    if len(extraData.keys()) > 0:
        ret['data'] = extraData
    responseData = json.dumps(ret)
    return responseData


def getGameIdBySymbol( symbol ):
    configKey = "%s.game_id" % (symbol, )
    gameId = getAppConfig(configKey)
    return gameId


class requestValidation(object):

    RULE_NAME_REQUIRED = 'require'
    RULE_NAME_NUMBER = 'number'
    RULES = {
        RULE_NAME_REQUIRED: lambda a, b: True if b in a else False,
        RULE_NAME_NUMBER: lambda a,b: True if b in a and re.match('\d+', a.get(b)[0]) is not None else False
    }

    def __init__(self, request, rule={} ):
        self.data = request.args
        self.rule = rule
        self.errorCode = None

    def validation(self):
        ret = True
        for key in self.rule.keys():
            currentRule = self.rule.get(key)
            if False == self.RULES.get(currentRule.get('name'))(self.data, key):
                self.errorCode = currentRule.get('error')
                ret = False
                break
        return ret
    def getErrorCode(self):
        return self.errorCode

def makeSign( data = {} ):
    dataKey = sorted(data)
    signParms = []
    signParmsStr = "%s&key=%s"
    for _key in dataKey:
        signParms.append("%s=%s" % (_key, data.get(_key)[0]))
    signParmsStr = signParmsStr % ("&".join(signParms), getAppConfig('api_server.sercertKey'))
    # log.msg('signParmsStr = %s' % ( signParmsStr, ))
    md5Handle = md5()
    md5Handle.update(signParmsStr.encode(encoding='utf-8'))
    signStr = md5Handle.hexdigest()
    return signStr


def identifySign( data = {}, signStr = "" ):
    serverSign = makeSign(data)
    # log.msg('serverSign = %s signStr = %s' % ( serverSign, signStr ))
    if serverSign == signStr:
        return  True
    return False

def getTradeNo():
    tradeNoPrefix = int(time.time() * 1000)
    return "%s%s" % ( tradeNoPrefix, random.randint(11, 99))


def dictToJson(data={}):
    return json.dumps(data)


def strToDict(data=""):

    ret = None
    try:
        ret = json.loads(data)
    except Exception,e:
        pass
    return ret





