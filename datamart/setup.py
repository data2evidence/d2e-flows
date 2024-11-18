from setuptools import setup, find_packages

setup(
    name='datamart_plugin',
    version='0.0.0',
    packages=find_packages(),
    include_package_data=True,
    package_data={},
    data_files=[('metadata', ['metadata/alp-job.json'])]
)