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
    build_flow.deploy(
        name="flow_builder-deployment",
        build=False
    )