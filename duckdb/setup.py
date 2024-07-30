from setuptools import setup, find_packages

setup(
    name='create_duckdb_file_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=["prefect==2.14.6", "s3fs==2023.1.0",
                      "aiobotocore==2.4.2", "botocore==1.27.59", "duckdb==1.0"],
    include_package_data=True,
    data_files=[('metadata', ['metadata/alp-job.json'])],
)
