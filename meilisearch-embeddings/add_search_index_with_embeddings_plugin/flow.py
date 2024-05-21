from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from add_search_index_with_embeddings_plugin.config import meilisearchAddIndexWithEmbeddingsType
import sys
import importlib

def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    
@flow(log_prints=True, task_runner=SequentialTaskRunner)
def add_search_index_with_embeddings_plugin(options: meilisearchAddIndexWithEmbeddingsType):
    setup_plugin()
    # Meilisearch flow file has to be imported dynamically as dataflow-mgmt does not have flow source code files and prefect is validating file imports during creation of prefect deployments
    meilisearch_flow_module = importlib.import_module('flows.meilisearch.flow')
    meilisearch_flow_module.execute_add_index_with_embeddings_flow(options)