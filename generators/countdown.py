from config import logger

def countdown(n):
    print(f"Countdown starting from {n}")

    while n > 0:
        yield n
        n -= 1


def main():
    logger.info(f"Countdown using function `{countdown.__name__}`")
    for x in countdown(10):
        print(x, end=" ")

    print()
    logger.info(f"Countdown using generator expressions")
    c = (x - 1 for x in range(11, 0, -1))
    for x in c:
        print(x, end=" ")


if __name__ == "__main__":
    main()
