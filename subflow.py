import time

from prefect import flow, get_run_logger


def get_data():
    time.sleep(5)
    return list(range(1000000))


@flow
def subflow():
    list_ = get_data() # memory leak is `list_`
    raise ValueError


@flow
def pipeline():
    logger = get_run_logger()

    for iteration in range(100):
        subflow(return_state=True)
        logger.info(f'{iteration=}')


if __name__ == '__main__':
    pipeline()

