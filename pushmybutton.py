"""PushMyButton integrates Amazon AWS IoT button to Leading2Lean CloudDISPATCH

An IoT Button can send three different signals to an AWS Lambda function. This
file demonstrates how to translate these signals to increment a production
count, increment a machine cycle time, or launch a Dispatch using the
Leading2Lean API library.
"""

import json
from botocore.vendored import requests
from datetime import datetime
from dateutil import tz

URL = 'https://acme.leading2lean.com/api/1.0/'
AUTH = 'abc123'
SITEID = '1'
TIMEZONE = 'US/Central'

def lambda_handler(event, context):
    """Triggered by the button signal. This method routes the "execute" methods."""
    
    thingData = getThingData(event['serialNumber'])
    print('serialNumber', event['serialNumber'], 'clickType', event['clickType'], 'thingData', thingData)
    
    methods = {
        'SINGLE': executeIncrementProductCountByMachineCode,
        'DOUBLE': executeIncrementScrapCountByMachineCode,
        # 'DOUBLE': executeIncrementMachineCycleCountByMachineCode,
        'LONG': executeLaunchCodeRedDispatchByMachineCode
    }
    
    # perform the action
    methods[event['clickType']](thingData)

    return {
        'statusCode': 200,
        'body': event['clickType'] + ' event run for button ' + event['serialNumber']
    }

def getThingData(serialNumber):
    """Look up data tied to the button. Maps a button serial number to a Machine code in Leading2Lean"""
    
    machineCodeLineMatrix = {
        'BUTTON-01-SERIAL': 'GreenMachine',
        'BUTTON-02-SERIAL': 'BlueMachine',
        'BUTTON-03-SERIAL': 'YellowMachine',
        'BUTTON-04-SERIAL': 'OrangeMachine'
    }
    
    return {
        'machineCode': machineCodeLineMatrix[serialNumber]
    }

def executeIncrementProductCountByMachineCode(thingData):
    """Increments the Actual metric in the Operator Portal
    
    Uses the Line tied to a Machine. Looks up the current Order or next Order,
    then increments the actual number on the line.
    """
    
    machineInfo = getMachineInfoByCode(thingData['machineCode'])
    
    # get the product code
    order = getOrderByLineCode(machineInfo['linecode'])
    if not order:
        return {'success': False, 'error': 'No order found'}
        
    product = getProductInfoById(order['product'])

    result = doPost('pitchdetails/record_details/', {
        'linecode': machineInfo['linecode'],
        'start': getCurrentDateTimeStr(),
        'end': getCurrentDateTimeStr(),
        'productcode': product['code'],
        'actual': 1
    })
    
    return {'success': True, 'result': result}

def executeIncrementScrapCountByMachineCode(thingData):
    """Increments the Scrap metric in the Operator Portal
    
    Uses the Line tied to a Machine. Looks up the current Order or next Order,
    then increments the scrap number on the line.
    """
    
    machineInfo = getMachineInfoByCode(thingData['machineCode'])
    
    # get the product code
    order = getOrderByLineCode(machineInfo['linecode'])
    if not order:
        return {'success': False, 'error': 'No order found'}
        
    product = getProductInfoById(order['product'])

    result = doPost('pitchdetails/record_details/', {
        'linecode': machineInfo['linecode'],
        'start': getCurrentDateTimeStr(),
        'end': getCurrentDateTimeStr(),
        'productcode': product['code'],
        'scrap': 1
    })
    
    return {'success': True, 'result': result}

def executeIncrementMachineCycleCountByMachineCode(thingData):
    """Increments the Machine Cycle Count by one."""
    
    result = doPost('machines/increment_cycle_count/', {
        'code': thingData['machineCode'], 
        'cyclecount': 1, 
        'skip_lastupdated': 1
    })
    
    return {'success': True, 'result': result} 

def executeLaunchCodeRedDispatchByMachineCode(thingData):
    """Launches a Code Red Dispatch for the Machine"""
    
    result = doPost('dispatches/open/', {
        'dispatchtypecode': 'Code Red', 
        'description': 'Launched by IoT button', 
        'machinecode': thingData['machineCode'], 
        'tradecode': 'Mechanic', 
        'user': 'DEMOAPI'
    })
    
    return {'success': True, 'result': result}

def getMachineInfoByCode(machineCode):
    return doGet('machines/', {'code': machineCode})

def getCurrentOrderByLineCode(lineCode):
    return doGet('buildsequence/get_current_order_on_line/', {'linecode': lineCode})
    
def getNextOrderByLineCode(lineCode):
    line = getLineInfoByCode(lineCode)
    if not line:
        return None
        
    return doGet('buildsequence/', {
        'line': line['id'], 
        'order_by': '-schedule_start_date', 
        'status__gte': '2',
        'status__lte': '6'
    })

def getLineInfoByCode(lineCode):
    return doGet('lines/', {'code': lineCode}, 1)

def getOrderByLineCode(lineCode):
    order = getCurrentOrderByLineCode(lineCode)
    if not order:
        order = getNextOrderByLineCode(lineCode)
    if not order:
        return None
    return order

def getProductInfoById(productId):
    return doGet('productcomponents/', {'id': productId}, 1)

def doPost(url, data, limit = 1):
    result = requests.post(URL + url, {'auth': AUTH, 'site': SITEID, 'limit': limit, **data})
    
    return finishRequest(result, limit)
    
def doGet(url, data, limit = 1):
    result = requests.get(URL + url, {'auth': AUTH, 'site': SITEID, 'limit': limit, **data})
    
    return finishRequest(result, limit)
    
def finishRequest(result, limit):
    resultJson = result.json()
    
    if resultJson['success'] and resultJson['data']:
        if limit == 1 and len(resultJson['data']) > 0 and isinstance(resultJson['data'], list):
            return resultJson['data'][0]
        else:
            return resultJson['data']
    else:
        return None

def getCurrentDateTimeStr():
    timeZone = tz.gettz(TIMEZONE)
    now = datetime.now(tz = timeZone)
    return str(now.strftime('%Y-%m-%d %H:%M:%S'))