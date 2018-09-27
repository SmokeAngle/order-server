#!/usr/bin/python
# -*- coding:utf-8 -*-
import os
from twisted.application import service
from utils import getAppConfig,orderServerOption
from services.apiService import apiService
from services.consumerService import consumerService
from twisted.python import log,logfile



def createApplication():
    appName = getAppConfig('app_name')
    apiServerIp = getAppConfig('api_server.bind_address')
    apiServerPort = int(getAppConfig('api_server.port'))
    games = getAppConfig('games').split(",")
    application = service.Application(name=appName)
    
   # log.msg(orderServerOption.get('nodaemon')) 
   # log.msg(orderServerOption.keys())
   # log.msg(orderServerOption.values())

   # if orderServerOption.get('nodaemon') != 0:
    logDir = os.path.realpath(os.path.dirname(getAppConfig('logdir')))
    logName = "%s.log" % (  appName, )
    logFile = logfile.DailyLogFile(logName, logDir)
    #log.msg(logDir)

    application.setComponent(log.ILogObserver, log.FileLogObserver(logFile).emit)

    log.msg("load application: ", appName)
    log.msg("load games:", games)

    appSrv = service.IServiceCollection(application)
    _apiService = apiService(bindAddress=apiServerIp, port=apiServerPort, games=games)
    _apiService.setServiceParent(appSrv)
    _consumerService = consumerService(games=games)
    _consumerService.setServiceParent(appSrv)
    return application

application = createApplication()
