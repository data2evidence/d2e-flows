from setuptools import setup, find_packages

setup(
    name='strategus',
    version='0.1.0',
    packages=find_packages(),
    install_requires=["prefect-shell==0.2.2", "prefect==2.19.9", "s3fs==2023.1.0", 
                      "aiobotocore==2.4.2", "botocore==1.27.59", "pandas==2.2.2",
                      "pytest==7.4.4", "numpy==2.0.0"],
    include_package_data=True,
    package_data={},
    data_files=[('metadata', ['metadata/alp-job.json'])],
)