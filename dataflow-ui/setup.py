from setuptools import setup, find_packages

setup(
    name='dataflow_ui_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[],
    include_package_data=True,
    package_data={},
    data_files=[('metadata', ['metadata/alp-job.json'])],
)