import requests
import datetime
import signal
import json
from threading import Thread
import atexit
from time import sleep

ports = [8079, 8082]
SUBSCRIBED = False
REGISTERED = False
ID = None
ON_IDK_ERROR_MESSAGE = "There is something seriously wrong!"
TIMEOUT = 10
TIME_BETWEEN_REQUESTS = 0.1
END_OF_MESSAGES = "All messages are consumed"

class CLI_OBJ(): 
    """
    CLI_OBJ used for testing.
    must be unregistered manually.
    """
    def __init__(self):
        self.REGISTERED = False
        self.ID = None
    
    def unregister(self):
        done = False
        try:
            requests.post(f'http://localhost:{ports[0]}/message/unregister/{self.ID}', timeout=TIMEOUT)
            done = True
        except:
            pass

        if done: # no need to call another time
            return
        
        try:
            requests.post(f'http://localhost:{ports[1]}/message/unregister/{self.ID}', timeout=TIMEOUT)
        except:
            print(ON_IDK_ERROR_MESSAGE)

def get_string_from_value(bytes):
    ret = ''
    for i in bytes:
        ret += chr(i)
    return ret

def string_to_byte_array(s):
    ret = bytearray()
    ret.extend(map(ord, s))
    return ret

def push(key, value, clinetObj=None):
    body = {
        "key": key, 
        "message": value if isinstance(value, str) else get_string_from_value(value), 
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    }
    done = False
    try:
        requests.post(f'http://localhost:{ports[0]}/message/produce', timeout=TIMEOUT, json=body)
        done = True
    except:
        pass

    if done: # if done is not set try again with broker #2
        return
    
    try:
        requests.post(f'http://localhost:{ports[1]}/message/produce', timeout=TIMEOUT, json=body)
    except:
        print(ON_IDK_ERROR_MESSAGE)

def unregister(sig=0, mig=0):
    done = False
    try:
        requests.post(f'http://localhost:{ports[0]}/message/unregister/{ID}', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        exit(0)
    
    try:
        requests.post(f'http://localhost:{ports[1]}/message/unregister/{ID}', timeout=TIMEOUT)
    except:
        print(ON_IDK_ERROR_MESSAGE)
    
    exit(0)

def unregister_without_exit():
    done = False
    try:
        requests.post(f'http://localhost:{ports[0]}/message/unregister/{ID}', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        return
    
    try:
        requests.post(f'http://localhost:{ports[1]}/message/unregister/{ID}', timeout=TIMEOUT)
    except:
        print(ON_IDK_ERROR_MESSAGE)

def register(clientObj=None):
    global REGISTERED, ID
    done = False
    try:
        temp = requests.post(f'http://localhost:{ports[0]}/message/register', timeout=TIMEOUT).content
        if not clientObj:
            ID = get_string_from_value(temp)
        else:
            clientObj.ID = get_string_from_value(temp)
        done = True
    except:
        pass

    if done: # no need to call another time
        if not clientObj:
            REGISTERED  = True
            signal.signal(signal.SIGINT, unregister)
            atexit.register(unregister_without_exit)
        else:
            clientObj.REGISTERED = True
        return
    
    try:
        temp = requests.post(f'http://localhost:{ports[1]}/message/register', timeout=TIMEOUT).content
        if not clientObj:
            ID = get_string_from_value(temp)
        else:
            clientObj.ID = get_string_from_value(temp)
    except:
        print(ON_IDK_ERROR_MESSAGE)
    
    if not clientObj:
        REGISTERED = True
        signal.signal(signal.SIGINT, unregister)
        atexit.register(unregister_without_exit)
    else:
        clientObj.REGISTERED = True

def pull(clientObj=None):
    content = None
    ack = None
    if (not REGISTERED) and (not clientObj):
        register()
    elif clientObj is not None:
        if not clientObj.REGISTERED:
            register(clientObj)
    
    done = False
    try:
        if not clientObj:
            content = requests.post(f'http://localhost:{ports[0]}/message/consume/{ID}', timeout=TIMEOUT).content
        else:
            content = requests.post(f'http://localhost:{ports[0]}/message/consume/{clientObj.ID}', timeout=TIMEOUT).content
        content = json.loads(content)
        ack = content['ack']
        done = True
    except:
        pass

    if not done: # need to call another time
        try:
            if not clientObj:
                content = requests.post(f'http://localhost:{ports[1]}/message/consume/{ID}', timeout=TIMEOUT).content
            else:
                content = requests.post(f'http://localhost:{ports[1]}/message/consume/{clientObj.ID}', timeout=TIMEOUT).content
            content = json.loads(content)
            ack = content['ack']
        except:
            print(ON_IDK_ERROR_MESSAGE)
    


    # call ack
    if not ack: # check for null ack, if null offset is finished
        return content['key'], string_to_byte_array(content['message'])

    done = False
    try:
        requests.post(f'http://localhost:{ports[0]}/message{ack}', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        return content['key'], string_to_byte_array(content['message'])
    
    try:
        requests.post(f'http://localhost:{ports[1]}/message{ack}', timeout=TIMEOUT)
    except:
        print(ON_IDK_ERROR_MESSAGE)
    
    return content['key'], string_to_byte_array(content['message'])

def subscribe(f, clinetObj=None):
    def temp():
        while True:
            temp = pull(clinetObj)
            if get_string_from_value(temp[1]) == END_OF_MESSAGES:
                continue
            f(*temp)
            sleep(TIME_BETWEEN_REQUESTS)
            if not clinetObj:
                if not REGISTERED:
                    exit(0)
            else:
                if not clinetObj.REGISTERED:
                    exit(0)
    Thread(target=temp()).start()

def clear():
    done = False
    try:
        requests.post(f'http://localhost:2181/zookeeper/clear', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        return
    
    try:
        requests.post(f'http://localhost:2182/zookeeper/clear', timeout=TIMEOUT)
        done = True
    except:
        pass


# ID = 0
# REGISTERED = True
# unregister()
# register()
# print(ID, REGISTERED)
# push("gooz", "gooz2")
# print(pull())
# clear()
    
