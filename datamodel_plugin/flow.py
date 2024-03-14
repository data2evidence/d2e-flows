from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from datamodel_plugin.config import dataModelType, flowActionType, createDataModelType, updateDataModelType
import importlib


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def create_datamodel(options: createDataModelType):
    dbsvc_module = importlib.import_module('d2e_dbsvc')
    dbsvc_module.create_datamodel(options)


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def update_datamodel(options: updateDataModelType):
    dbsvc_module = importlib.import_module('d2e_dbsvc')
    dbsvc_module.update_datamodel(options)

    
@flow(log_prints=True, task_runner=SequentialTaskRunner)
def create_update_datamodel(options: dataModelType):
    dbsvc_module = importlib.import_module('d2e_dbsvc')
    if options.flow_action_type == flowActionType.CREATE_DATA_MODEL:
        dbsvc_module.create_datamodel(options)
    elif options.flow_action_type == flowActionType.UPDATE_DATA_MODEL:
        dbsvc_module.update_datamodel(options)