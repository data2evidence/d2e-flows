from prefect import task, flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.context import TaskRunContext, FlowRunContext

from pydicom import dcmread
from pydicom.dataelem import DataElement

import os
import sys
import json
import importlib
from pathlib import Path
from uuid import uuid4, UUID
from typing import Dict, List
from datetime import datetime
from collections import defaultdict
from orthanc_api_client import OrthancApiClient

from dicom_etl_plugin.types import *
from dicom_etl_plugin.utils import *

#import pandas as pd


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    

@flow(log_prints=True, task_runner=SequentialTaskRunner)
def dicom_etl_plugin(options: ETLOptions):
    logger = get_run_logger()
    flow_action_type = options.flow_action_type
    database_code = options.database_code
    
    root_folder = options.root_folder
    upload_files = options.upload_files
    
    medical_imaging_schema_name = options.medical_imaging_schema_name
    vocab_schema_name = options.vocab_schema_name
    cdm_schema_name = options.cdm_schema_name

    setup_plugin()
    types_module = importlib.import_module('utils.types')
    dbdao_module = importlib.import_module('dao.DBDao')
    dicom_server_api_module = importlib.import_module('api.DicomServerAPI')

    admin_user = types_module.PG_TENANT_USERS.ADMIN_USER
    mi_dbdao = dbdao_module.DBDao(database_code, medical_imaging_schema_name, admin_user)
    vocab_dbdao = dbdao_module.DBDao(database_code, vocab_schema_name, admin_user)
    cdm_dbdao = dbdao_module.DBDao(database_code, cdm_schema_name, admin_user)
    DicomServerAPI = dicom_server_api_module.DicomServerAPI()
    
    match flow_action_type:
        case FlowActionType.INGEST_METADATA:
            # 1. Populate vocabulary and concept tables with DICOM
            # 2. Populate dicom data element reference table
            setup_vocab(vocab_dbdao)
            load_data_elements(mi_dbdao)
            
            for path in Path(root_folder).rglob('*.dcm'):
                if path.is_file() and path.suffix == '.dcm':
                    # 3. Extract data elements and insert into image_occurrence table
                    # 4. Extract data elements and insert into dicom_file_metadata table
                    image_occurrence_id, sop_instance_id = process_file_metadata(path, cdm_dbdao, mi_dbdao, vocab_dbdao)
                    if upload_files:
                        image_occurrence_id, sop_instance_id = upload_file_to_server(dbdao=mi_dbdao, filepath=path, 
                                                                                      image_occurrence_id=image_occurrence_id, 
                                                                                      sop_instance_id=sop_instance_id, 
                                                                                      api=DicomServerAPI)
            
@task(log_prints=True)
def setup_vocab(dbdao):
    # import csv if table is empty
    update_vocabulary_table(dbdao)
    update_concept_class_table(dbdao)
    update_concept_table(dbdao)
    
@task(log_prints=True)
def load_data_elements(dbdao):
    populate_data_elements(dbdao)
    

@task(log_prints=True)
def process_file_metadata(path: str, cdm_dbdao, mi_dbdao, vocab_dbdao):
    '''
    Prefect task that processes the tags of a DICOM file and inserts into medical imaging schema
    '''
    logger = get_run_logger()
    logger.info(f"Processing metadata for '{path.name}'..")
    file_data_to_insert = []
    with dcmread(path) as f:
        try:
            study_instance_uid = f.get("StudyInstanceUID", None) # [0x0020, 0x000D]
            series_instance_uid = f.get("SeriesInstanceUID", None) # [0x0020, 0x000E]
            
            sop_instance_uid = f.get("SOPInstanceUID", None) # [0x0008, 0x0018]
            acquisition_date = f.get("AcquisitionDate", None) # [0x0008, 0x0022]
            patient_id = f.get("PatientID", None) # [0x0010, 0x0020]

            # only process files with valid attributes
            check_none_attributes(study_instance_uid=study_instance_uid,
                                    series_instance_uid=series_instance_uid,
                                    sop_instance_uid=sop_instance_uid,
                                    acquisition_date=acquisition_date,
                                    patient_id=patient_id)
            
            instance_number = f.get("InstanceNumber", None) # [0x0020, 0x0013]
            study_date = f.get("StudyDate", None) # [0x0008, 0x0020] 
            study_description = f.get("StudyDescription", None) # [0x0008, 0x1030] 
            modality_code = f.get("Modality", None)  # [0x0008, 0x0060]
            anatomic_site = f.get("BodyPartExamined", None) # [0x0018, 0x0015]
            
            
            # retrieve person_id from person table
            patient_age = f.get("PatientAge", None) # [0x0010, 0x1010]
            patient_dob = f.get("PatientBirthDate", None) # [0x0010, 0x0030]
            patient_sex = f.get("PatientSex", None) # [0x0010, 0x0040]
            patient_race = f.get("EthnicGroup", None) # [0x0010, 0x2160]
            
            person_id = get_person_id(cdm_dbdao, patient_id, patient_dob, study_date,
                                    patient_sex, patient_age, patient_race)
            
            
            # insert record into procedure_occurrence table
            procedure_occurrence_id = insert_procedure_occurence_table(cdm_dbdao, person_id, study_date, 
                                                                       study_description)

            # ingest into image_occurrence table
            image_occurrence_id = insert_image_occurrence_table(
                vocab_dbdao,
                mi_dbdao,
                modality_code,
                anatomic_site,
                study_instance_uid,
                series_instance_uid,
                acquisition_date,
                person_id,
                procedure_occurrence_id
            )
        
            # ingest into dicom_file_metadata table
            logger.info(f"Processing data elements for ingestion..")
            for data_elem in f:
                if data_elem.keyword == "PixelData":
                    logger.info(f"Excluding Pixel Data with tag {data_elem.tag}")
                else:
                    process_data_element(mi_dbdao, data_elem, image_occurrence_id,
                                            sop_instance_uid, instance_number, 
                                            file_data_to_insert)
                    
            no_of_attributes = len(file_data_to_insert)
            
            if no_of_attributes == 0:
                raise Exception("No metadata to insert")
            else: 
                logger.info(f"Inserting {no_of_attributes} metadata into 'dicom_file_metadata' table..")
                for _data in file_data_to_insert:
                    try:
                        mi_dbdao.insert_values_into_table("dicom_file_metadata", _data)
                    except Exception as e:
                        logger.error(_data)
                        logger.error(e)
                        
        except Exception as e:
            logger.error(e)
            raise(e)
        else:
            return image_occurrence_id, sop_instance_uid
    
            

def process_data_element(dbdao, data_elem: DataElement, image_occurrence_id: int, 
                         sop_instance_id: str, instance_number: int,
                         metadata_list: List, sequence_id: str = None, 
                         dataset_id: str = None) -> bool:
    data_elem_json = data_elem.to_json_dict(bulk_data_element_handler=None, 
                                            bulk_data_threshold=1024) # 1024 is the default used by pydicom for datasets.to_json_dict()
    
    default_factory = lambda: None
    record = defaultdict(default_factory)
    
    metadata_id = str(uuid4())
    
    # Extract from data_elem
    keyword = data_elem.keyword
    value_multiplicity = data_elem.VM
    tag_as_str_tuple = convert_tag_to_tuple(data_elem.tag)
    is_private = data_elem.tag.is_private
    is_private_creator = data_elem.tag.is_private_creator
    private_creator = hex(data_elem.tag.private_creator)if is_private_creator else None
    
    # Extract from data_elem_json
    value_representation = data_elem_json.get("vr", "")
    is_sequence = True if value_representation.upper() == "SQ" else False
    sequence_length = int(value_multiplicity) if is_sequence else None
    data_element_source_value = None if is_sequence else data_elem_json.get("Value", None)
    data_element_id = get_data_element_id(dbdao, tag_as_str_tuple, "", keyword, value_representation, is_private)

    record = {
        "metadata_id": metadata_id,
        "data_element_id": data_element_id,
        "ingested_datetime": datetime.now(),
        "metadata_source_tag": tag_as_str_tuple,
        "metadata_source_group_number": f"{data_elem.tag.group:04X}",
        "metadata_source_keyword": keyword,
        "metadata_source_value_representation": value_representation,
        "metadata_source_value_multiplicity": value_multiplicity,
        "is_sequence": is_sequence,
        "parent_sequence_id": sequence_id,
        "parent_dataset_id": dataset_id,
        "sequence_length": sequence_length,
        "is_private": is_private,
        "is_private_creator": is_private_creator,
        "private_creator": private_creator,
        "image_occurrence_id": image_occurrence_id,
        "sop_instance_id": sop_instance_id,
        "instance_number": instance_number
    }
    
    if is_sequence is False:
        record["metadata_source_value"] = json.dumps(data_element_source_value)
        metadata_list.append(record)
    else:
        # insert sequence as a data element 
        metadata_list.append(record)
        
        # handle nested datasets and data elements
        for dataset in data_elem:
            # a sequence contains >= 0 datasets which contain >= 1 data elements
            dataset_id = str(uuid4())
            for nested_data_elem in dataset:
                process_data_element(dbdao=dbdao,
                                     data_elem=nested_data_elem, 
                                     image_occurrence_id=image_occurrence_id,
                                     sop_instance_id=sop_instance_id,
                                     instance_number=instance_number,
                                     metadata_list=metadata_list, 
                                     sequence_id=metadata_id, dataset_id=dataset_id)


@task(log_prints=True)
def upload_file_to_server(dbdao, filepath: str, image_occurrence_id: int, 
                          sop_instance_id: str, api):     
    logger = get_run_logger()   
    dicom_server_url = os.getenv("DICOM_SERVER__API_BASE_URL")
    
    try:
        filename = filepath.name
        logger.info("Connecting to DICOM server..")
        orthanc_a = OrthancApiClient(dicom_server_url, user='', pwd='')
        
        logger.info(f"Uploading '{filename}' to DICOM server..")
        orthanc_instance_id = orthanc_a.upload_file(filepath)
        logger.info(f"orthanc_instance_id is '{orthanc_instance_id}'")
        
        if orthanc_instance_id:
            # file renamed in storage when uploaded
            uploaded_filename = api.get_uploaded_file_name(orthanc_instance_id[0])
            logger.info(f"'{filename}' was renamed to '{uploaded_filename}' in DICOM server!")
            
            task_run_context = TaskRunContext.get().task_run.dict()
            task_run_id = str(task_run_context.get("id"))
            flow_run_id = str(task_run_context.get("flow_run_id"))
            
            # for traceability
            values_to_insert = {
                "task_run_id": task_run_id,
                "flow_run_id": flow_run_id,
                "original_filename": filename,
                "instance_id": orthanc_instance_id,
                "uploaded_filename": uploaded_filename,
                "uploaded_datetime": datetime.now(),
                "image_occurrence_id": image_occurrence_id,
                "sop_instance_id": sop_instance_id
            }

            logger.info(f"Inserting record to 'file_upload_metadata'..")
            dbdao.insert_values_into_table("file_upload_metadata", values_to_insert)
        else:
            raise Exception(f"Failed to upload file '{filename}'")
    except Exception as e:
        logger.error(e)
        raise e