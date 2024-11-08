from setuptools import setup, find_packages


setup(
    name='data_management_plugin',
    version='0.0.0',
    packages=find_packages(),
    install_requires=[],
    include_package_data=True,
    package_data = {
        "": [
            'db/migrations/**/*'
        ]
    },
    data_files=[('metadata', ['metadata/alp-job.json']) ],
)