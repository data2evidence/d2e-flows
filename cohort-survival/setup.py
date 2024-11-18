from setuptools import setup, find_packages

setup(
    name='cohort_survival_plugin',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "": [
            "lintr"
        ]
        },
    data_files=[('metadata', ['metadata/alp-job.json'])],
)