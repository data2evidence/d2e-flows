from setuptools import setup, find_packages

setup(
    name='dqd_plugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[],
    include_package_data=True,
    package_data={
        "": [
            'DataQualityDashboard-2.6.0/**'
        ]
    },
    data_files=[('metadata', ['metadata/alp-job.json'])],
)