import sys
import importlib
from ner_extract_plugin.types import NerExtractType
from ner_extract_plugin.nel import EntityExtractorLinker
import pandas as pd

from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def ner_extract_plugin(options: NerExtractType):
    spacy_module = importlib.import_module("spacy")
    logger = get_run_logger()
    # logger.info(f"The following spacy models are available:")
    logger.info(f"The following spacy models are available: {spacy_module.info()['pipelines']}")

    # load transcripts
    docstr = open(options.doc, 'r').read()

    # Two steps of add_pipeline and extract
    medical_ner_nel = EntityExtractorLinker()
    medical_ner_nel.add_pipeline(model_name="en_ner_bc5cdr_md", linker_name="umls")
    df1 = medical_ner_nel.extract_entities(text=docstr, confidence_threshold=0.8)

    medical_ner_nel = EntityExtractorLinker()
    medical_ner_nel.add_pipeline(model_name="en_core_med7_trf", linker_name="rxnorm")
    df2 = medical_ner_nel.extract_entities(text=docstr, confidence_threshold=0.8)
    results_df = pd.concat([df1,df2]).reset_index(drop=True)

    # # One step of add_pipeline and extract
    # medical_ner_nel = EntityExtractorLinker()
    # medical_ner_nel.add_pipeline(model_name="en_ner_bc5cdr_md", linker_name="umls")
    # medical_ner_nel.add_pipeline(model_name="en_core_med7_trf", linker_name="rxnorm")
    # results_df = medical_ner_nel.extract_entities(text=docstr, confidence_threshold=0.8)

    logger.info(f"Results for confidence_threshold=0.8: \n {results_df}")
    # print(results_df)

    