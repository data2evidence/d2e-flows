from setuptools import setup, find_packages

setup(
    name='omop_cdm_plugin',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    package_data={},
    data_files=[('metadata', ['metadata/alp-job.json'])],
)