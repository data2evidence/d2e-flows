from setuptools import setup, find_packages

setup(
    name='add_search_index_plugin',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    data_files=[('metadata', ['metadata/alp-job.json'])],
)