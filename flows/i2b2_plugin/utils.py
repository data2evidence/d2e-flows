import os
from prefect_shell import ShellOperation

def get_version_from_tag(tag: str) -> str:
    return tag[1:4].replace(".", "-")

def path_to_ant(tag: str) -> str:
    return f"i2b2-data-{tag[1:]}/edu.harvard.i2b2.data/Release_{get_version_from_tag(tag)}"

async def download_source_code(tag_name: str):
    await ShellOperation(
        commands=[
            f"wget https://github.com/i2b2/i2b2-data/archive/refs/tags/{tag_name}.tar.gz",
        ]).run()
    
async def unzip_source_code(tag_name: str):
    await ShellOperation(
        commands=[         
            f"tar -xzf {tag_name}.tar.gz"
        ]).run()


async def setup_apache_ant(tag_name: str):
    cwd = os.getcwd()
    ant_bin_dir = os.path.join(cwd, f"{path_to_ant(tag_name)}/apache-ant")
    
    # Set ant_home environment variable
    os.environ["ANT_HOME"] = ant_bin_dir
    
    await ShellOperation(
        commands=[         
            f'ln -sfn {ant_bin_dir} /opt/ant',
            f'ln -sfn /opt/ant/bin/ant /usr/bin/ant',
            'ant -version'
        ]).run()