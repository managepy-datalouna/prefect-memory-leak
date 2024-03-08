import time

from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment


@task
def get_data():
    time.sleep(5)
    return list(range(1000000))


@flow
def pipeline():
    logger = get_run_logger()

    for iteration in range(100):
        get_data.fn()
        logger.info(f'{iteration=}')


if __name__ == '__main__':
    deployment = Deployment.build_from_flow(
        flow=pipeline,
        name="pipeline_deployment",
        work_queue_name="pipeline_queue",
        apply=True,
    )

