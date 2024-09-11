import sys
from prefect_shell.commands import ShellOperation, shell_run_command
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from ohdsi_etl_germany_plugin.types import OHDSIEtlGermanyOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')

@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def ohdsi_etl_germany_plugin(options: OHDSIEtlGermanyOptionsType):
    logger = get_run_logger()
    logger.info('Running OHDSI ETL Germany OMOP to FHIR transformation')
    
    setup_plugin()  
    
    batch_chunksize = options.batchChunksize
    fhirGateway_jdbc_curl = options.fhirGatewayJdbcCurl
    fhirGateway_username = options.fhirGatewayUsername
    fhirGateway_password = options.fhirGatewayPassword
    fhirGateway_table = options.fhirGatewayTable
    omop_cdm_jdbc_curl = options.omopCDMJdbcCurl
    omop_cdm_username = options.omopCDMUsername
    omop_cdm_password = options.omopCDMPassword
    omop_cdm_schema = options.omopCDMSchema
    data_begin_date = options.dataBeginDate
    data_end_date = options.dataEndDate
    shell_run_command(
        command = "cd /app/omoptofhir && java org.springframework.boot.loader.JarLauncher",
        env={"JAEGER_SERVICE_NAME": "fhir-to-omop",
            "OPENTRACING_JAEGER_ENABLED": "false",
            "BATCH_CHUNKSIZE": "5000",
            "BATCH_THROTTLELIMIT": "4",
            "BATCH_PAGINGSIZE": "200000",
            "LOGGING_LEVEL_ORG_MIRACUM": "INFO",
            "DATA_FHIRGATEWAY_JDBCURL": "jdbc:postgresql://localhost:15432/fhir",
            "DATA_FHIRGATEWAY_USERNAME": "postgres",
            "DATA_FHIRGATEWAY_PASSWORD": "postgres",
            "DATA_FHIRGATEWAY_TABLENAME": "resources",
            "DATA_FHIRSERVER_CONNECTIONTIMEOUT": "3000",
            "DATA_FHIRSERVER_SOCKETTIMEOUT": "3000",
            "DATA_OMOPCDM_JDBCURL": "jdbc:postgresql://alp-minerva-postgres-1.alp.local:5432/alpdev_pg",
            "DATA_OMOPCDM_USERNAME": "postgres",
            "DATA_OMOPCDM_PASSWORD": "Toor1234",
            "DATA_OMOPCDM_SCHEMA": "cdmdefault",
            "DATA_BEGINDATE": "1800-01-01",
            "DATA_ENDDATE": "2023-01-01",
            "MANAGEMENT_METRICS_EXPORT_PROMETHEUS_PUSHGATEWAY_ENABLED": "false",
            "MANAGEMENT_METRICS_EXPORT_PROMETHEUS_PUSHGATEWAY_BASE_URL": "http://localhost:9091",
            "FHIR_SYSTEMS_DEPARTMENT": "https://www.medizininformatik-initiative.de/fhir/core/modul-fall/CodeSystem/Fachabteilungsschluessel",
            "FHIR_SYSTEMS_INTERPRETATION": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
            "APP_CARESITEIMPORT_ENABLED": "false",
            "APP_BULKLOAD_ENABLED": "false",
            "APP_DICTIONARYLOADINRAM_ENABLED": "true",
            "APP_WRITEMEDICATIONSTATEMENT_ENABLED": "false",
            "APP_STARTSINGLESTEP": "",
            "SPRING_CACHE_CAFFEINE_SPEC_MAXIMUMSIZE": "5000"
        })
    