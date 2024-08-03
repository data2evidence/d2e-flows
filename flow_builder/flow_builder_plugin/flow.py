import sys
from prefect import flow

@flow
def build_flow():
    # install requirements.txt
    my_flow = flow.from_source(
        source="https://github.com/PrefectHQ/prefect.git",
        entrypoint="flows/hello_world.py:hello"
    )
    my_flow.deploy(
        name="hello_world", 
        work_pool_name="test-docker-pool", 
        image="hello_world:develop",
        push=False
    )

if __name__ == '__main__':
    kwargs = {}
    for arg in sys.argv[1:]:
        key, value = arg.split('=')
        kwargs[key] = value

    sourcePath = kwargs.get('sourcePath')
    entrypoint = kwargs.get('entrypoint')
    build_flow.from_source(
        sourcePath,
        entrypoint
    ).deploy(
        name="flow_builder-deployment",
        build=False,
        push=False
    )