from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from data_management_plugin.config import dataModelType, flowActionType
import importlib

    
@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_management_plugin(options: dataModelType):
    dbsvc_module = importlib.import_module('d2e_dbsvc')
    match options.flow_action_type:
        case flowActionType.CREATE_DATA_MODEL:
            dbsvc_module.create_datamodel(options)
        case flowActionType.UPDATE_DATA_MODEL:
            dbsvc_module.update_datamodel(options)
        case flowActionType.ROLLBACK_COUNT:
            dbsvc_module.rollback_count(options)
        case flowActionType.ROLLBACK_TAG:
            dbsvc_module.rollback_tag(options)
        case flowActionType.CREATE_SNAPSHOT:
            dbsvc_module.create_snapshot(options)
        case flowActionType.CREATE_PARQUET_SNAPSHOT:
            dbsvc_module.create_parquet_snapshot(options)
        case flowActionType.FETCH_VERSION_INFO:
            dbsvc_module.fetch_version_info(options)
        case flowActionType.CREATE_QUESTIONNAIRE_DEFINITION:
            dbsvc_module.create_questionnaire_definition(options)
        case flowActionType.GET_QUESTIONNAIRE_RESPONSE:
            dbsvc_module.get_questionnaire_response(options)