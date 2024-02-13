import requests
import datetime
import signal
import json
from threading import Thread, Lock
import atexit
from time import sleep
from random import randint

ports = [8081, 8082]
zookeeper_ports = [2181, 2182]
REGISTERED = False
SUBSCRIBED = False
ID = None
ON_IDK_ERROR_MESSAGE = "There is something seriously wrong!"
TIMEOUT = 10
TIME_BETWEEN_REQUESTS = 1
END_OF_MESSAGES = "All messages are consumed"
# IP = "localhost"
IP = "65.21.54.41"
SUBS_LOCK = Lock()

class CLI_OBJ(): 
    """
    CLI_OBJ used for testing.
    must be unregistered & unsubscribed manually.
    """
    def __init__(self):
        self.REGISTERED = False
        self.ID = None
        self.SUBSCRIBED = False
    
    def unregister(self):
        zero = randint(0, 1)
        one = 1 - zero
        done = False
        try:
            requests.post(f'http://{IP}:{ports[zero]}/message/unregister/{self.ID}', timeout=TIMEOUT)
            done = True
        except:
            pass

        if done: # no need to call another time
            return
        
        try:
            requests.post(f'http://{IP}:{ports[one]}/message/unregister/{self.ID}', timeout=TIMEOUT)
        except:
            print(ON_IDK_ERROR_MESSAGE)
    
    def unsubscribe(self):
        self.SUBSCRIBED = False

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
    zero = randint(0, 1)
    one = 1 - zero
    body = {
        "key": key, 
        "message": value if isinstance(value, str) else get_string_from_value(value), 
        "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    }
    done = False
    try:
        requests.post(f'http://{IP}:{ports[zero]}/message/produce', timeout=TIMEOUT, json=body)
        done = True
    except:
        pass

    if done: # if done is not set try again with broker #2
        sleep(TIME_BETWEEN_REQUESTS)
        return
    
    try:
        requests.post(f'http://{IP}:{ports[one]}/message/produce', timeout=TIMEOUT, json=body)
    except:
        print(ON_IDK_ERROR_MESSAGE)
    sleep(TIME_BETWEEN_REQUESTS)

def unregister(sig=0, mig=0):
    zero = randint(0, 1)
    one = 1 - zero
    SUBSCRIBED = False
    done = False
    try:
        requests.post(f'http://{IP}:{ports[zero]}/message/unregister/{ID}', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        exit(0)
    
    try:
        requests.post(f'http://{IP}:{ports[one]}/message/unregister/{ID}', timeout=TIMEOUT)
    except:
        print(ON_IDK_ERROR_MESSAGE)
    
    exit(0)

def unregister_without_exit():
    zero = randint(0, 1)
    one = 1 - zero
    SUBSCRIBED = False
    done = False
    try:
        requests.post(f'http://{IP}:{ports[zero]}/message/unregister/{ID}', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        return
    
    try:
        requests.post(f'http://{IP}:{ports[one]}/message/unregister/{ID}', timeout=TIMEOUT)
    except:
        print(ON_IDK_ERROR_MESSAGE)

def register(clientObj=None):
    zero = randint(0, 1)
    one = 1 - zero
    global REGISTERED, ID
    done = False
    try:
        temp = requests.post(f'http://{IP}:{ports[zero]}/message/register', timeout=TIMEOUT).content
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
        sleep(TIME_BETWEEN_REQUESTS + 0.5)
        return
    
    try:
        temp = requests.post(f'http://{IP}:{ports[one]}/message/register', timeout=TIMEOUT).content
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
    sleep(TIME_BETWEEN_REQUESTS + 0.5)

def pull(clientObj=None, sub=False):
    zero = randint(0, 1)
    one = 1 - zero
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
            content = requests.post(f'http://{IP}:{ports[zero]}/message/consume/{ID}', timeout=TIMEOUT).content
        else:
            content = requests.post(f'http://{IP}:{ports[zero]}/message/consume/{clientObj.ID}', timeout=TIMEOUT).content
        content = json.loads(content)
        ack = content['ack']
        done = True
    except:
        pass

    if not done: # need to call another time
        try:
            if not clientObj:
                content = requests.post(f'http://{IP}:{ports[one]}/message/consume/{ID}', timeout=TIMEOUT).content
            else:
                content = requests.post(f'http://{IP}:{ports[one]}/message/consume/{clientObj.ID}', timeout=TIMEOUT).content
            content = json.loads(content)
            ack = content['ack']
        except:
            pass
    

    sleep(TIME_BETWEEN_REQUESTS)
    # call ack
    if not ack: # check for null ack, if null offset is finished
        if sub:
            return "", string_to_byte_array(END_OF_MESSAGES), ""

        if content == b'':
            sleep(TIME_BETWEEN_REQUESTS)
            return pull(clientObj, sub)
        
        if content == None:
            sleep(TIME_BETWEEN_REQUESTS)
            return pull(clientObj, sub)
        
        if not sub:
            return content['key'], string_to_byte_array(content['message'])

    if sub:
        return content['key'], string_to_byte_array(content['message']), content['ack']
    
    send_ack(ack)
    return content['key'], string_to_byte_array(content['message'])

def send_ack(ack):
    zero = randint(0, 1)
    one = 1 - zero
    done = False
    try:
        requests.post(f'http://{IP}:{ports[zero]}/message{ack}', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        sleep(TIME_BETWEEN_REQUESTS)
        return
    
    try:
        requests.post(f'http://{IP}:{ports[one]}/message{ack}', timeout=TIMEOUT)
    except:
        print(ON_IDK_ERROR_MESSAGE)
    
    sleep(TIME_BETWEEN_REQUESTS)

def subscribe(f, clinetObj=None):
    if (not REGISTERED) and (not clinetObj):
        register()
    elif clinetObj is not None:
        if not clinetObj.REGISTERED:
            register(clinetObj)
    def temp():
        while True:
            with SUBS_LOCK:
                temp = pull(clinetObj, True)
                if get_string_from_value(temp[1]) == END_OF_MESSAGES:
                    if not clinetObj:
                        if not SUBSCRIBED:
                            return
                    else:
                        if not clinetObj.SUBSCRIBED:
                            return
                    continue
                f(temp[0], temp[1])
                send_ack(temp[2])
                sleep(TIME_BETWEEN_REQUESTS)
                if not clinetObj:
                    if not SUBSCRIBED:
                        return
                else:
                    if not clinetObj.SUBSCRIBED:
                        return
    global SUBSCRIBED
    if not clinetObj:
        SUBSCRIBED = True
    else:
        clinetObj.SUBSCRIBED = True
    Thread(target=temp).start()

def clear():
    zero = randint(0, 1)
    one = 1 - zero
    done = False
    try:
        requests.post(f'http://{IP}:{zookeeper_ports[zero]}/zookeeper/clear', timeout=TIMEOUT)
        done = True
    except:
        pass

    if done: # no need to call another time
        sleep(TIME_BETWEEN_REQUESTS)
        return
    
    try:
        requests.post(f'http://{IP}:{zookeeper_ports[one]}/zookeeper/clear', timeout=TIMEOUT)
        done = True
    except:
        pass

    sleep(TIME_BETWEEN_REQUESTS)

