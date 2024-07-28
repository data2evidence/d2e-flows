from setuptools import setup, find_packages

setup(
    name='flow_builder_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=["prefect-shell==0.2.2", "prefect==2.14.6"],
    include_package_data=True,
    data_files=[('metadata', ['metadata/alp-job.json'])],
)