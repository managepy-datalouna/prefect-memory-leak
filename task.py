import time

from prefect import flow, task, get_run_logger


@task
def get_data():
    time.sleep(5)
    return list(range(1000000)) # memory leak is here


@flow
def pipeline():
    logger = get_run_logger()

    for iteration in range(100):
        get_data.fn()
        logger.info(f'{iteration=}')


if __name__ == '__main__':
    pipeline()
