#!/usr/bin/python
# -*- coding:utf-8 -*-

from twisted.scripts.twistd import runApp
from os.path import join,dirname
from sys import argv
from utils import orderServerOptions,orderServerOption
from const import *
import sys

reload(sys)
sys.setdefaultencoding('utf-8')



if __name__ == '__main__':
    orderServerOption.parseOptions()
    if orderServerOption.get('env') == ENV_DEV:
        # argv[1:1] = ['-n', '-b', '-y', join(dirname(__file__), 'orderApp.py')]
        argv[1:1] = [ '-y', join(dirname(__file__), 'orderApp.py')]
    else:
        argv[1:1] = ['-y', join(dirname(__file__), 'orderApp.py')]
    orderServerOption = orderServerOptions()
    orderServerOption.parseOptions()
    runApp(orderServerOption)




