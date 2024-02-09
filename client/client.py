import requests
import datetime
import signal
import json
from threading import Thread
import atexit

ports = [8081, 8082]
REGISTERED = False
ID = None
ON_IDK_ERROR_MESSAGE = "be ga raftim" # todo maybe change this
TIMEOUT = 10

def get_string_from_value(bytes):
    ret = ''
    for i in bytes:
        ret += chr(i)
    return ret

def string_to_byte_array(s):
    ret = bytearray()
    ret.extend(map(ord, s))
    return ret

def push(key, value):
    body = {
        "key": key, 
        "message": value if isinstance(value, str) else get_string_from_value(value), 
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    }
    print(body)
    done = False
    try:
        requests.post(f'http://localhost:{ports[0]}/message/produce', timeout=TIMEOUT, json=body)
        done = True
    except requests.exceptions.Timeout:
        pass

    if done: # if done is not set try again with broker #2
        return
    
    try:
        requests.post(f'http://localhost:{ports[1]}/message/produce', timeout=TIMEOUT, json=body)
    except requests.exceptions.Timeout:
        print(ON_IDK_ERROR_MESSAGE)

def unregister(sig=0, mig=0):
    print("unreg call shod!")
    done = False
    try:
        ID = requests.post(f'http://localhost:{ports[0]}/message/unregister/{ID}', timeout=TIMEOUT).content
        done = True
    except requests.exceptions.Timeout:
        pass

    if done: # no need to call another time
        exit(0)
    
    try:
        ID = requests.post(f'http://localhost:{ports[1]}/message/unregister/{ID}', timeout=TIMEOUT).content
    except requests.exceptions.Timeout:
        print(ON_IDK_ERROR_MESSAGE)
    
    exit(0)

def register():
    done = False
    try:
        ID = requests.post(f'http://localhost:{ports[0]}/message/register', timeout=TIMEOUT).content
        done = True
    except requests.exceptions.Timeout:
        pass

    if done: # no need to call another time
        REGISTERED  = True
        return
    
    try:
        ID = requests.post(f'http://localhost:{ports[1]}/message/register', timeout=TIMEOUT).content
    except requests.exceptions.Timeout:
        print(ON_IDK_ERROR_MESSAGE)
    
    REGISTERED = True
    signal.signal(signal.SIGINT, unregister)
    atexit.register(unregister)


def pull():
    content = None
    ack = None
    if not REGISTERED:
        register()
    done = False
    try:
        content = requests.post(f'http://localhost:{ports[0]}/message/consume/{ID}', timeout=TIMEOUT).content
        content = json.loads(content)
        ack = content['ack']
        done = True
    except requests.exceptions.Timeout:
        pass

    if not done: # need to call another time
        try:
            content = requests.post(f'http://localhost:{ports[1]}/message/consume/{ID}', timeout=TIMEOUT).content
            content = json.loads(content)
            ack = content['ack']
        except requests.exceptions.Timeout:
            print(ON_IDK_ERROR_MESSAGE)
    


    # call ack

    done = False
    try:
        requests.post(f'http://localhost:{ports[0]}/message/ack/{ack}', timeout=TIMEOUT)
        done = True
    except requests.exceptions.Timeout:
        pass

    if done: # no need to call another time
        return content['key'], string_to_byte_array(content['message'])
    
    try:
        requests.post(f'http://localhost:{ports[1]}/message/ack/{ack}', timeout=TIMEOUT)
    except requests.exceptions.Timeout:
        print(ON_IDK_ERROR_MESSAGE)
    
    return content['key'], string_to_byte_array(content['message'])

def subscribe(f):
    def temp():
        f()
        exit(0)
    Thread(target=temp()).start()

