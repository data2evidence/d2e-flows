from flows.ner_extract_plugin.nel import *
from flows.ner_extract_plugin.types import *

import pandas as pd
import spacy
import time

from prefect import flow
from prefect.logging import get_run_logger

@flow(log_prints=True)
def ner_extract_plugin(options: NerExtractOptions):
    logger = get_run_logger()

    # logger.info('start sleep')
    # time.sleep(6000)

    logger.info(f"The following spacy models are available: {spacy.info()['pipelines']}")
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
    