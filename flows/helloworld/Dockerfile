FROM prefecthq/prefect:2.20.2-python3.11

RUN pip install httpx
RUN pip install prefect-docker

COPY __init__.py /opt/prefect
COPY flow.py /opt/prefect

