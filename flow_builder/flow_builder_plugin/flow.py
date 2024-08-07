import json
import os
import subprocess
import sys
import shutil
from pathlib import Path
from prefect import flow, get_run_logger, deploy
from prefect.utilities.dockerutils import build_image
from prefect.deployments.runner import RunnerDeployment

def download_package(package_spec):
    """
    Download a package from a Git URL or npm registry to a specific directory using pnpm.

    Args:
        package_spec (str): The package specification, e.g. "github:user/repo#branch" or "@scope/pkg"
        dest_dir (str): The destination directory to download the package source code
    """
    try:
        subprocess.run(["pnpm", "add", package_spec], check=True)
    except Exception as e:
        get_run_logger(e)

@flow
def build_flow(name, url):
    plugin_path = f'/tmp/node_modules/{name}'
    ori_cwd = os.getcwd()
    logger = get_run_logger()
    os.chdir("/tmp")
    try:
        download_package(url)
        os.chdir(plugin_path)

        package_json_obj = {}
        with open('package.json', 'r') as package_json_file:
            package_json_obj = json.loads(package_json_file.read())
        
        logger.info(msg=json.dumps(package_json_obj))
        config = package_json_obj.get('config', {})
        plugin_version = package_json_obj.get('version', '0.1.0')
        flow_name = config.get("name")
        flow_type = config.get("type")
        flow_params = config.get("params", None)
        flow_entrypoint = config.get("entrypoint")
        platform = package_json_obj.get('platform', 'linux/amd64')

        os.chdir(ori_cwd)
        build_image(Path(plugin_path), platform=platform, tag=f"{flow_name}:{plugin_version}", 
                    stream_progress_to=sys.stdout, pull=True)
        deploy(
            RunnerDeployment.from_entrypoint(
                entrypoint=f"{plugin_path}/{flow_entrypoint}", 
                name=f"{flow_name}",
                parameters=flow_params,
                job_variables={"image_pull_policy": "Never"}
            ),
            work_pool_name="docker-pool", 
            image=f"{flow_name}:{plugin_version}",
            push=False,
            build=False
        )

    finally:
        logger.info(f'removing {plugin_path}')
        shutil.rmtree('/tmp/node_modules')

if __name__ == '__main__':
    kwargs = {}
    for arg in sys.argv[1:]:
        key, value = arg.split('=')
        kwargs[key] = value

    cmd = kwargs.get('cmd', 'deploy')
    match cmd:
        case 'deploy':
            build_flow.from_source(
                source=f"{os.getcwd()}",
                entrypoint="flow.py:build_flow"
            ).deploy(
                name="flow_builder-deployment",
                build=False,
                push=False
            )
        case 'test':
            build_flow('npm-flow-plugin', 'git+https://<PAT_TOKEN>:x-oauth-basic@github.com/alp-os/<REPO_NAME>#path:<DIR_PATH_OF_FLOW>')