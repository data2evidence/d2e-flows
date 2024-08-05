from setuptools import setup, find_packages

setup(
    name="dicom_etl_plugin",
    version='0.1.0',
    packages=find_packages(),
    install_requires=["prefect==2.14.6", "s3fs==2023.1.0", "prefect-shell==0.2.2",
                      "aiobotocore==2.4.2", "botocore==1.27.59", "pydicom==2.4.4", 
                      "pandas==2.2.2", "orthanc-api-client==0.15.3"],
    include_package_data=True,
    package_data = {
        "": ['external/**/*']
    },
    data_files=[('metadata', ['metadata/alp-job.json'])],
)
