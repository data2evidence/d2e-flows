from setuptools import setup, find_packages

setup(
    name='i2b2_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[],
    include_package_data=True,
    data_files=[('metadata', ['metadata/alp-job.json'])],
)