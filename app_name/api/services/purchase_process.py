import random


def purchase_process(msg):
    n = random.randint(0, 100)
    if n <= 75:
        print(f"Purchase is valid: {msg}")
    else:
        print(f"Purchase is not valid: {msg}")
