from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from data_management_plugin.config import dataModelType, flowActionType
import importlib
import sys

def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    
@flow(log_prints=True, task_runner=SequentialTaskRunner, timeout_seconds=10)
def data_management_plugin(options: dataModelType):
    setup_plugin()
    dbsvc_module = importlib.import_module('flows.alp_db_svc.flow')
    match options.flow_action_type:
        case flowActionType.CREATE_DATA_MODEL:
            dbsvc_module.create_datamodel_flow(options)
        case flowActionType.UPDATE_DATA_MODEL:
            dbsvc_module.update_datamodel_flow(options)
        case flowActionType.ROLLBACK_COUNT:
            dbsvc_module.rollback_count_flow(options)
        case flowActionType.ROLLBACK_TAG:
            dbsvc_module.rollback_tag_flow(options)
        case flowActionType.CREATE_SNAPSHOT:
            dbsvc_module.create_snapshot_flow(options)
        case flowActionType.CREATE_PARQUET_SNAPSHOT:
            dbsvc_module.create_snapshot_flow(options)
        case flowActionType.GET_VERSION_INFO:
            dbsvc_module.get_version_info_flow(options)
        case flowActionType.CREATE_QUESTIONNAIRE_DEFINITION:
            dbsvc_module.create_questionnaire_definition_flow(options)
        case flowActionType.GET_QUESTIONNAIRE_RESPONSE:
            dbsvc_module.get_questionnaire_response_flow(options)
        case flowActionType.CREATE_CDMSCHEMA:
            dbsvc_module.create_cdm_schema(options)
