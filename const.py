#!/usr/bin/python
# -*- coding:utf-8 -*-

from os.path import dirname,realpath,pathsep

ENV_DEV = 'dev'
ENV_STG = 'stg'
ENV_PROD = 'prod'

GAME_CODE_NY = 'ny'
GAME_CODE_DJN = 'djn'
GAME_CODE_ZJH = 'zjh'
GAME_CODE_SSS = 'sss'
GAME_CODE_JXSG = 'jxsg'
GAME_CODE_YDN = 'ydn'
GAME_CODE_NZG = 'nzg'
GAME_CODE_NYS = 'nys'

APP_CONFIG = 'config.ini'

API_RESPONSE_CODE_SUCCESS = 0
API_RESPONSE_CODE_FAIL = -1
API_RESPONSE_CODE_ENDPOINT_NOT_EXISTS = 1000
API_RESPONSE_CODE_SIGN_ERROR = 1001

API_RESPONSE_CODE_INVALID_AGENT_UID = 1002
API_RESPONSE_CODE_INVALID_PRODUCTID = 1003
API_RESPONSE_CODE_INVALID_GAMEID = 1004
API_RESPONSE_CODE_INVALID_TIMESTAMP = 1005
API_RESPONSE_CODE_INVALID_SIGN = 1006



API_RESPONSE_MSG = {
    API_RESPONSE_CODE_SUCCESS: 'success',
    API_RESPONSE_CODE_FAIL: 'fail',
    API_RESPONSE_CODE_ENDPOINT_NOT_EXISTS: 'api not exists',
    API_RESPONSE_CODE_SIGN_ERROR: 'sign error',
    API_RESPONSE_CODE_INVALID_AGENT_UID: 'invalid agent_uid',
    API_RESPONSE_CODE_INVALID_PRODUCTID: 'invalid good_id',
    API_RESPONSE_CODE_INVALID_GAMEID: 'invalid gameid',
    API_RESPONSE_CODE_INVALID_TIMESTAMP: 'invalid timestamp',
    API_RESPONSE_CODE_INVALID_SIGN: 'invalid sign'
}

