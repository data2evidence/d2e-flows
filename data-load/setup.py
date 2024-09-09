from setuptools import setup, find_packages

setup(
    name='data_load_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=["prefect-shell==0.2.2", "prefect==2.19.9", 
                      "s3fs==2023.1.0", "aiobotocore==2.4.2", "botocore==1.27.59", "chardet", "numpy==1.26.4"],
    include_package_data=True,
    data_files=[('metadata', ['metadata/alp-job.json'])],
)