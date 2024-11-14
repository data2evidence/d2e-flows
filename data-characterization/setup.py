from setuptools import setup, find_packages

setup(
    name='data_characterization_plugin',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "": [
            'db/migrations/**/*'
        ]
    },
    data_files=[('metadata', ['metadata/alp-job.json'])],
)