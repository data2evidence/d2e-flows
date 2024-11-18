from setuptools import setup, find_packages

setup(
    name='create_cachedb_file_plugin',
    version='0.1.0',
    packages=find_packages(),
    include_package_data=True,
    package_data = {
        "": [
            "duckdb_indices.sql"
        ]
    },
    data_files=[('metadata', ['metadata/alp-job.json'])],
)
