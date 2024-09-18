from setuptools import setup, find_packages

setup(
    name='omop_cdm_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        "prefect-shell==0.2.2", "prefect==2.14.6", 
        "s3fs==2023.1.0", "aiobotocore==2.4.2", 
        "botocore==1.27.59"],
    include_package_data=True,
    package_data={},
    data_files=[('metadata', ['metadata/alp-job.json'])],
)