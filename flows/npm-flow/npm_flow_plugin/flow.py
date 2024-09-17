from prefect import flow
from prefect.logging import get_run_logger


@flow
def hello(name: str = "world"):
    get_run_logger().info(f"Hello {name}!")


if __name__ == "__main__":
    hello()


# To test after building the image with flow_builder
# if __name__ == "__main__":
#     hello.deploy(
#         name="test-docker-deployment",
#         work_pool_name="docker-pool",
#         image="npm-flow-plugin:0.1.0",
#         job_variables={"stream_output": True, "network_mode": "host"},
#         push=False
#     )
