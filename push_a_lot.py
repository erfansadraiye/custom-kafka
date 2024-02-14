from client.client import push
from threading import Thread
import random

NUM_PUSH = 45
THREAD_NUM = 1000

def generate_random_string(l=10):
    ret = ''
    for _ in range(l):
        ret += chr(ord('a') + random.randint(0, 25))
    return ret


def pusher():
    for _ in range(NUM_PUSH):
        push(generate_random_string(), generate_random_string())

for _ in range(THREAD_NUM):
    Thread(target=pusher).start()

print('when done press enter')
input()
