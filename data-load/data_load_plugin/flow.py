import importlib
import sys
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
import sqlalchemy as sql
from data_load_plugin.utils.types import DataloadOptions
import pandas as pd
import numpy as np

def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    
    
@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_load_plugin(options: DataloadOptions):
    setup_plugin()
    logger = get_run_logger()
    files = options.files
    database_code = options.database_code
    header = 0 if options.header else None
    escape_char = options.escape_character if options.escape_character else None
    schema = options.schema_name
    tables_to_truncate = [f.table_name for f in files if f.truncate]
    chunksize = options.chunksize if options.chunksize else None
    dbutils_module = importlib.import_module('alpconnection.dbutils')
    conn_details = dbutils_module.extract_db_credentials(database_code)
    database_name = conn_details["databaseName"]
    pg_user = conn_details["adminUser"]
    pg_password = conn_details["adminPassword"]
    pg_host = conn_details["host"]
    pg_port = conn_details["port"]

    # TODO: make below dialect agnostic
    engine = sql.create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{database_name}')

    # Truncating
    with engine.connect() as connection:
        for t in tables_to_truncate:
            trans = connection.begin()
            try:
                truncate_sql = sql.text(f"delete from {schema}.{t}")
                connection.execute(truncate_sql)
                trans.commit()
                
            except Exception as e:
                trans.rollback()
                logger.error(e)
                raise e
            else:
                logger.info(f"Table {t} truncated successfully!")

    for file in files:
        try:
            logger.info(f"Reading data from file {file.table_name}")
            # Load data from CSV file
            for i, data in read_csv(file.path, escapechar=escape_char, header=header, delimiter=options.delimiter, encoding=options.encoding, chunksize=chunksize):
                data = format_vocab_synpuf_data(data, file.table_name)
                if header == 0:
                    csv_column_names = data.columns.tolist()
                    sql_query = sql.text("SELECT column_name FROM information_schema.columns WHERE table_name = :x AND table_schema = :y;")
                    with engine.connect() as connection:
                        res = connection.execute(sql_query, {"x": file.table_name, "y": schema}).fetchall()
                        if res is None:
                            raise Exception(f"No columns found for table {file.table_name} in schema {schema}!")
                        else:
                            table_column_names = [column_name[0] for column_name in res]
                            common_columns = list(set(csv_column_names) & set(table_column_names))
                            data[common_columns].to_sql(file.table_name, engine, if_exists='append', index=False, schema=schema, chunksize=chunksize)
                elif header is None:
                    data.to_sql(file.table_name, engine, if_exists="append", index=False, schema=schema, chunksize=chunksize)
        except Exception as e:
            logger.error(f'Data load failed for the table {file.table_name} at the chunk index: {i}  with error: {e}')
            raise e
        else:
            logger.info(f"Data load succeeded for table {file.table_name}!")

def read_csv(filepath, escapechar, header, delimiter, encoding, chunksize):
    i = 1
    if chunksize:
        for chunk in pd.read_csv(filepath, escapechar=escapechar, header=header, delimiter=delimiter, encoding=encoding, chunksize=chunksize, keep_default_na=False):
            yield i, chunk
            i += 1
    else:
        yield i, pd.read_csv(filepath)
        
def format_vocab_synpuf_data(data, table_name):
    match table_name:
        case "concept_relationship" | "concept" | "drug_strength":
            data["valid_start_date"] = pd.to_datetime(data["valid_start_date"].astype(str))
            data["valid_end_date"] =  pd.to_datetime(data["valid_end_date"].astype(str))
            if table_name == "drug_strength":
                # Integer columns use pd.NA
                data["amount_unit_concept_id"].replace('', pd.NA, inplace=True)
                data["box_size"].replace('', pd.NA, inplace=True)
                data["numerator_unit_concept_id"].replace('', pd.NA, inplace=True)
                data["denominator_unit_concept_id"].replace('', pd.NA, inplace=True)
                
                # Float Columns use np.nan
                data["amount_value"].replace('', np.nan, inplace=True)
                data["numerator_value"].replace('', np.nan, inplace=True)
                data["denominator_value"].replace('', np.nan, inplace=True)
                
                # Convert to integer
                data["amount_unit_concept_id"] = pd.to_numeric(data["amount_unit_concept_id"], errors="coerce").astype('Int64')
                data["box_size"] = pd.to_numeric(data["box_size"], errors="coerce").astype('Int64')
                data["numerator_unit_concept_id"] = pd.to_numeric(data["numerator_unit_concept_id"], errors="coerce").astype('Int64')
                data["denominator_unit_concept_id"] = pd.to_numeric(data["denominator_unit_concept_id"], errors="coerce").astype('Int64')
                
                # Convert to float
                data["amount_value"] = pd.to_numeric(data["amount_value"], errors="coerce").astype('float64')
                data["numerator_value"] = pd.to_numeric(data["numerator_value"], errors="coerce").astype('float64')
                data["denominator_value"] = pd.to_numeric(data["denominator_value"], errors="coerce").astype('float64')
        case "location":
            data.drop(['COUNTRY_CONCEPT_ID', 'COUNTRY_SOURCE_VALUE', 'LATITUDE', 'LONGITUDE'], inplace=True, axis=1)
        case "care_site":
            data.drop(['LOCATION_ID'], inplace=True, axis=1)
        case "condition_occurrence":
            data.drop(['CONDITION_END_DATETIME', 'PROVIDER_ID', 'VISIT_DETAIL_ID'], inplace=True, axis=1)
        case "cost":
            data.drop(['TOTAL_CHARGE', 'TOTAL_COST', 'PAID_BY_PAYER', 'PAID_PATIENT_COPAY', 'PAID_PATIENT_DEDUCTIBLE', 'PAID_BY_PRIMARY', 'PAID_INGREDIENT_COST', 'PAID_DISPENSING_FEE', 'PAYER_PLAN_PERIOD_ID', 'AMOUNT_ALLOWED', 'REVENUE_CODE_CONCEPT_ID', 'REVENUE_CODE_SOURCE_VALUE', 'DRG_SOURCE_VALUE'], inplace=True, axis=1)
        case "death":
            data.drop(['CAUSE_CONCEPT_ID', 'DEATH_DATETIME'], inplace=True, axis=1)
        case "device_exposure":
            data.drop(['DEVICE_EXPOSURE_END_DATETIME', 'UNIQUE_DEVICE_ID', 'QUANTITY', 'PROVIDER_ID', 'VISIT_DETAIL_ID', 'UNIT_CONCEPT_ID', 'UNIT_SOURCE_VALUE', 'UNIT_SOURCE_CONCEPT_ID'], inplace=True, axis=1)
        case "drug_exposure":
            data.drop(['DRUG_EXPOSURE_END_DATETIME', 'VERBATIM_END_DATE', 'REFILLS', 'QUANTITY', 'DAYS_SUPPLY', 'ROUTE_CONCEPT_ID', 'LOT_NUMBER', 'PROVIDER_ID', 'VISIT_OCCURRENCE_ID', 'VISIT_DETAIL_ID', 'ROUTE_SOURCE_VALUE', 'DOSE_UNIT_SOURCE_VALUE'], inplace=True, axis=1)
        case "measurement":
            data.drop(['MEASUREMENT_DATETIME', 'MEASUREMENT_TIME', 'OPERATOR_CONCEPT_ID', 'VALUE_AS_NUMBER', 'UNIT_CONCEPT_ID', 'RANGE_LOW', 'RANGE_HIGH', 'PROVIDER_ID', 'VISIT_DETAIL_ID', 'MEASUREMENT_SOURCE_CONCEPT_ID', 'VALUE_SOURCE_VALUE', 'MEASUREMENT_EVENT_ID', 'MEAS_EVENT_FIELD_CONCEPT_ID', 'UNIT_SOURCE_CONCEPT_ID'], inplace=True, axis=1)
        case "observation":
            data.drop(['OBSERVATION_DATETIME', 'OBSERVATION_EVENT_ID', 'VALUE_AS_NUMBER', 'VALUE_AS_STRING', 'QUALIFIER_CONCEPT_ID', 'UNIT_CONCEPT_ID', 'PROVIDER_ID', 'VISIT_DETAIL_ID', 'UNIT_SOURCE_VALUE', 'QUALIFIER_SOURCE_VALUE', 'OBS_EVENT_FIELD_CONCEPT_ID'], inplace=True, axis=1)
        case "payer_plan_period":
            data.drop(['PAYER_CONCEPT_ID', 'PAYER_SOURCE_VALUE', 'PAYER_SOURCE_CONCEPT_ID', 'PLAN_CONCEPT_ID', 'PLAN_SOURCE_CONCEPT_ID', 'SPONSOR_CONCEPT_ID', 'SPONSOR_SOURCE_VALUE', 'SPONSOR_SOURCE_CONCEPT_ID', 'FAMILY_SOURCE_VALUE', 'STOP_REASON_CONCEPT_ID', 'STOP_REASON_SOURCE_VALUE', 'STOP_REASON_SOURCE_CONCEPT_ID'], inplace=True, axis=1)
        case "person":
            data.drop(['BIRTH_DATETIME', 'PROVIDER_ID', 'CARE_SITE_ID', 'GENDER_SOURCE_CONCEPT_ID', 'RACE_SOURCE_CONCEPT_ID', 'ETHNICITY_SOURCE_CONCEPT_ID'], inplace=True, axis=1)
        case "procedure_occurrence":
            data.drop(['PROCEDURE_END_DATE', 'PROCEDURE_END_DATETIME', 'MODIFIER_CONCEPT_ID', 'QUANTITY', 'PROVIDER_ID', 'VISIT_DETAIL_ID', 'MODIFIER_SOURCE_VALUE'], inplace=True, axis=1)
        case "provider":
            data.drop(['YEAR_OF_BIRTH', 'SPECIALTY_SOURCE_VALUE', 'GENDER_SOURCE_VALUE'], inplace=True, axis=1)
        case "visit_occurrence":
            data.drop(['VISIT_START_DATETIME', 'VISIT_END_DATETIME', 'PROVIDER_ID', 'CARE_SITE_ID', 'VISIT_SOURCE_CONCEPT_ID', 'PRECEDING_VISIT_OCCURRENCE_ID'], inplace=True, axis=1)
    data = data.replace("N/A", "N/A")
    data = data.rename(columns=str.lower)
    return data