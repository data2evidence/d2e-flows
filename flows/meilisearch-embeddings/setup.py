from setuptools import setup, find_packages

setup(
    name='add_search_index_with_embeddings',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        "prefect-shell==0.2.2", "prefect==2.14.6", "s3fs==2023.1.0", 
        "aiobotocore==2.4.2", "botocore==1.27.59", "chardet", "transformers>=4.34.0,<5.0.0"
        ],
    include_package_data=True,
    data_files=[('metadata', ['metadata/alp-job.json'])],
)