from client.client import *
from time import sleep
import random

def run_test(f):
    temp = f()
    print(f'\t\t{f.__name__}', end='')
    print(' ' * (30 - len(f.__name__)), end='')
    print("\033[91mFAILED\033[0m" if temp != 0 else "\033[92mPASSED\033[0m.")

def generate_random_string(l=10):
    ret = ''
    for _ in range(l):
        ret += chr(ord('a') + random.randint(0, 25))
    return ret

@run_test
def Test_Pull_Sanity_Check() -> int:
    clear()
    client_pull1 = CLI_OBJ()

    if not (get_string_from_value(pull(client_pull1)[1]) == END_OF_MESSAGES):
        clear()
        return 1

    clear()
    return 0

@run_test
def Test_Push_Sanity_Check() -> int:
    clear()
    client_push1 = CLI_OBJ()

    client_pull1 = CLI_OBJ()

    push(generate_random_string(), "random message", client_push1)

    temp = get_string_from_value(pull(client_pull1)[1])
    if not (temp == "random message" or temp == END_OF_MESSAGES):
        clear()
        return 1

    clear()
    return 0

@run_test
def Test_Order() -> int:
    """
    Test for order of messages with the same key
    """
    clear()
    messages = ["message 1", "message 2", "message 3", "message 4", "message 5"]
    ret = []
    client_push1 = CLI_OBJ()
    client_push2 = CLI_OBJ()

    client_pull1 = CLI_OBJ()

    push("same key", messages[0], client_push1)
    push("same key", messages[1], client_push2)
    push("same key", messages[2], client_push1)
    push("same key", messages[3], client_push2)
    push("same key", messages[4], client_push1)

    for _ in range(len(messages)):
        ret.append(pull(client_pull1))

    idx = 0
    for k, m in ret:
        if get_string_from_value(m) != messages[idx]:
            client_pull1.unregister()
            clear()
            return 1
        idx += 1

    if not (get_string_from_value(pull(client_pull1)[1]) == END_OF_MESSAGES):
        client_pull1.unregister()
        clear()
        return 1

    client_pull1.unregister()
    clear()
    return 0

@run_test
def Test_Subscribe() -> int:
    clear()
    client_sub1 = CLI_OBJ()

    client_push1 = CLI_OBJ()
    client_push2 = CLI_OBJ()

    messages = ["message 1", "message 2", "message 3"]
    ret = []

    subscribe(lambda x, y: ret.append(get_string_from_value(y)), client_sub1)

    push("same key", messages[0], client_push1)
    push("same key", messages[1], client_push2)
    push("same key", messages[2], client_push1)

    sleep(5)

    if len(ret) != len(messages):
        client_sub1.unsubscribe()
        sleep(0.1)
        client_sub1.unregister()
        clear()
        return 1

    for i, j in zip(messages, ret):
        if i != j:
            client_sub1.unsubscribe()
            sleep(0.1)
            client_sub1.unregister()
            clear()
            return 1

    client_sub1.unsubscribe()
    sleep(0.1)
    client_sub1.unregister()
    clear()
    return 0

@run_test
def Test_Subscribe2() -> int:
    clear()
    client_push1 = CLI_OBJ()
    client_pull1 = CLI_OBJ()
    client_sub1 = CLI_OBJ()

    messages = ["message 1", "message 2", "message 3"]
    ret = []

    subscribe(lambda x, y: ret.append(get_string_from_value(y)), client_sub1)

    push("same key", messages[0], client_push1)
    push("same key", messages[1], client_push1)

    sleep(5)

    if len(ret) != 2:
        client_sub1.unregister()
        clear()
        return 1

    for i in range(2):
        if ret[i] != messages[i]:
            client_sub1.unsubscribe()
            clear()
            return 1

    client_sub1.unsubscribe()
    sleep(0.5)
    push("same key", messages[2], client_push1)

    if not (get_string_from_value(pull(client_pull1)[1]) == messages[2]):
        client_pull1.unregister()
        clear()
        return 1

    client_pull1.unregister()
    clear()
    return 0

@run_test
def Test_Unique_Id() -> int:
    clear()

    client1 = CLI_OBJ()
    client2 = CLI_OBJ()
    client3 = CLI_OBJ()

    register(client1)
    register(client2)
    register(client3)

    if client1.ID == client2.ID:
        clear()
        return 1
    
    if client2.ID == client3.ID:
        clear()
        return 1
    
    if client1.ID == client3.ID:
        clear()
        return 1
    
    clear()
    return 0

@run_test
def Test_Get_Different_Messages() -> int:
    clear()

    client_pull1 = CLI_OBJ()
    client_pull2 = CLI_OBJ()
    client_pull3 = CLI_OBJ()

    client_push1 = CLI_OBJ()

    messages = ["message 1", "message 2", "message 3", "message 4", "message 5"]
    for m in messages:
        push(generate_random_string(), m, client_push1)
    
    ret1 = []
    ret2 = []
    ret3 = []

    register(client_pull1)
    register(client_pull2)
    register(client_pull3)

    for client, arr in zip([client_pull1, client_pull2, client_pull3], [ret1, ret2, ret3]):
        while (temp := get_string_from_value(pull(client)[1])) != END_OF_MESSAGES:
            arr.append(temp)

    if len(ret1) + len(ret2) + len(ret3) != len(messages):
        clear()
        return 1
    
    clear()
    return 0
