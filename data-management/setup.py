from setuptools import setup, find_packages


setup(
    name='data_management_plugin',
    version='0.0.0',
    packages=find_packages(),
    install_requires=["prefect==2.14.6", "s3fs==2023.1.0", "aiobotocore==2.4.2", "botocore==1.27.59", "chardet"],
    include_package_data=True,
    package_data = {
        "": [
            'db/migrations/**/*'
        ]
    },
    data_files=[('metadata', ['metadata/alp-job.json']) ],
)