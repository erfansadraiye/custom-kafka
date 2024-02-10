from client.client import *
from time import sleep

def run_tests(arr):
    for i in arr:
        temp = i()
        print(f'\t\t{i.__name__}', end='')
        print(' ' * (30 - len(i.__name__)), end='')
        print("\033[91mFAILED\033[0m" if temp != 0 else "\033[92mPASSED\033[0m.")

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

def Test_Subscribe() -> int:
    clear()
    client_sub1 = CLI_OBJ()

    client_push1 = CLI_OBJ()
    client_push2 = CLI_OBJ()

    messages = ["message 1", "message 2", "message 3", "message 4", "message 5"]
    ret = []

    push("same key", messages[0], client_push1)
    push("same key", messages[1], client_push2)
    push("same key", messages[2], client_push1)
    push("same key", messages[3], client_push2)
    push("same key", messages[4], client_push1)

    subscribe(lambda x, y: ret.append(get_string_from_value(y)), client_sub1)

    sleep(2)

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

def Test_Subscribe2() -> int:
    clear()
    client_push1 = CLI_OBJ()
    client_pull1 = CLI_OBJ()
    client_sub1 = CLI_OBJ()

    messages = ["message 1", "message 2", "message 3", "message 4", "message 5"]
    ret = []
    subscribe(lambda x, y: ret.append(get_string_from_value(y)), client_sub1)

    push("same key", messages[0], client_push1)
    push("same key", messages[1], client_push1)
    push("same key", messages[2], client_push1)

    for i in range(3):
        if ret[i] != messages[i]:
            client_sub1.unregister()
            clear()
            return 1
    
    client_sub1.unregister()
    push("same key", messages[3], client_push1)
    push("same key", messages[4], client_push1)

    if not (get_string_from_value(pull(client_pull1)[1]) == messages[3]):
        client_pull1.unregister()
        clear()
        return 1
    
    if not (get_string_from_value(pull(client_pull1)[1]) == messages[4]):
        client_pull1.unregister()
        clear()
        return 1
    
    client_pull1.unregister()
    clear()
    return 0

def Test_Pull_Sanity_Check() -> int:
    clear()
    client_pull1 = CLI_OBJ()

    if not (get_string_from_value(pull(client_pull1)[1]) == END_OF_MESSAGES):
        clear()
        return 1
    
    clear()
    return 0

def Test_Push_Sanity_check() -> int:
    clear()
    client_push1 = CLI_OBJ()

    client_pull1 = CLI_OBJ()

    push("random key 1", "random message", client_push1)

    temp = get_string_from_value(pull(client_pull1)[1])
    if not (temp ==  "random message"):
        clear()
        return 1
    
    clear()
    return 0

run_tests([Test_Pull_Sanity_Check, Test_Push_Sanity_check, Test_Order])