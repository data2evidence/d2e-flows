import re
import torch
from datetime import date, datetime
from transformers import AutoTokenizer, AutoModel

from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from flows.add_search_index_with_embeddings_plugin.config import MeilisearchAddIndexWithEmbeddingsType

from shared_utils.dao.VocabDao import VocabDao
from shared_utils.api.MeilisearchSvcAPI import MeilisearchSvcAPI
from shared_utils.api.TerminologySvcAPI import TerminologySvcAPI


    
@flow(log_prints=True, task_runner=SequentialTaskRunner)
def add_search_index_with_embeddings_plugin(options: MeilisearchAddIndexWithEmbeddingsType):
    logger = get_run_logger()
    database_code = options.databaseCode
    vocab_schema_name = options.vocabSchemaName
    table_name = options.tableName
    token = options.token
    use_cache_db = options.use_cache_db
    CHUNK_SIZE = options.chunk_size
    MEILISEARCH_INDEX_CONFIG = options.meilisearch_index_config

    # Check if vocab_schema_name is valid
    if not re.match(r"^\w+$", vocab_schema_name):
        error_message = "VocabSchemaName is invalid"
        logger.error(error_message)
        raise ValueError(error_message)

    # Check if table_name is supported for adding as meilisearch index
    # Case insensitive search on MEILISEARCH_INDEX_CONFIG.keys()
    if table_name.lower() not in MEILISEARCH_INDEX_CONFIG.keys():
        errorMessage = f"table_name:{table_name.lower()} has not been configured for adding as a meilisearch index"
        logger.error(errorMessage)
        raise ValueError(errorMessage)

    # Initialize helper classes
    meilisearch_svc_api = MeilisearchSvcAPI()
    terminology_svc_api = TerminologySvcAPI(token)

    config = terminology_svc_api.get_hybridSearchConfig()
    
    hybrid_search_name = f"{config['source'].replace('/', '')}_{config['model'].replace('/', '')}";
    index_name = f"{database_code}_{vocab_schema_name}_{table_name}_{hybrid_search_name}"
    
    vocab_dao = VocabDao(use_cache_db=use_cache_db,
                         database_code=database_code, 
                         schema_name=vocab_schema_name)
    
    logger.info(f"Getting stream connection")
    conn = vocab_dao.get_stream_connection(yield_per=CHUNK_SIZE)
    try:
        meilisearch_primary_key = MEILISEARCH_INDEX_CONFIG[table_name.lower()]["meilisearch_primary_key"]
        index_settings = MEILISEARCH_INDEX_CONFIG[table_name.lower()]["index_settings"]
        # Get table information
        table_length = vocab_dao.get_table_length(table_name)
        column_names = vocab_dao.get_column_names(table_name)

        # Add table column names to meilisearch settings as searchableAttributes
        index_settings["searchableAttributes"] = column_names
        
        # Add embedders settings to enable hybrid search
        index_settings["embedders"] = {
                "default": {
                    "source": f"{config['source']}",
                    "model": f"{config['model']}"
                }
            }
        
        # Create meilisearch index
        logger.info(f"Creating meilisearch index with name: {index_name}")
        meilisearch_svc_api.create_index(index_name, meilisearch_primary_key)

        logger.info(f"Updating settings for index with name: {index_name}")
        meilisearch_svc_api.update_index_settings(index_name, index_settings)
        # Check if meilisearch_primary_key defined in config is in table column_names
        is_meilisearch_primary_key_in_table = meilisearch_primary_key in column_names

        # If meilisearch primary key does not exist for table, add meilisearch_primary_key to column names
        if not is_meilisearch_primary_key_in_table:
            column_names.insert(0, meilisearch_primary_key)
        
        #Add _vectors in the list of column_names to store embeddings
        column_names.append("_vectors")
        
        stream_result_set = vocab_dao.get_stream_result_set(conn, table_name)
        chunk_iteration = 0
        # Iterate through stream_result_set, parse and post data to meilisearch index
        logger.info(f"Posting {table_name} data to meilisearch index in chunks of {CHUNK_SIZE}...")
        for data in stream_result_set.partitions():
            logger.info(
                f"Progress: {(CHUNK_SIZE*chunk_iteration)+len(data)}/{table_length}")
            # Convert each element in data from sqlalchemy.engine.row.Row to list
            data = [list(row) for row in data]

            # If meilisearch primary key does not exist for table, add running index as primary key to data
            if not is_meilisearch_primary_key_in_table:
                [row.insert(0, idx+(CHUNK_SIZE*chunk_iteration))
                for idx, row in enumerate(data)]

            # Parse date/datetime values into formatted string
            data = [parse_dates(row) for row in data]

            # Calculate embeddings and append them to the row
            data = calculate_embeddings(data, config=config, logger=logger)
            
            # Map column names to data for insertion into meilisearch index
            mappedData = [dict(zip(column_names, row))
                        for row in data]

            # Add documents to index
            meilisearch_svc_api.add_documents_to_index(
                index_name, meilisearch_primary_key, mappedData)
            chunk_iteration += 1
    
    except Exception as err:
        logger.error(err)
        raise err
    finally:
        conn.close()
        
        
def calculate_embeddings(rows:list[list], config, logger):
    # Sentences we want sentence embeddings for    
    for row in rows:
        sentence = " ".join(str(r) for r in row);
        row.append(sentence)
        
    # Mean Pooling - Take attention mask into account for correct averaging
    def meanpooling(output, mask):
        
        
        embeddings = output[0] # First element of model_output contains all token embeddings
        mask = mask.unsqueeze(-1).expand(embeddings.size()).float()
        return torch.sum(embeddings * mask, 1) / torch.clamp(mask.sum(1), min=1e-9)

    # Load model from HuggingFace Hub
    tokenizer = AutoTokenizer.from_pretrained(config['model'])
    model = AutoModel.from_pretrained(config['model'])

    for row in rows:
        inputs = tokenizer(row[len(row)-1], padding=True, truncation=True, return_tensors='pt')
        # Compute token embeddings
        with torch.no_grad():
            output = model(**inputs)

        # Perform pooling. In this case, mean pooling.
        embeddings = meanpooling(output, inputs['attention_mask'])
        row.pop() # Remove sentences
        row.append(embeddings.tolist()[0]) # Append calculated embeddings
     
    return rows

def parse_dates(row):
    result = []
    for element in row:
        if isinstance(element, date):
            datetime_element = datetime.combine(element, datetime.min.time())
            result.append(int(datetime_element.timestamp()))
        elif isinstance(element, datetime):
            result.append(int(element.timestamp()))
        else:
            result.append(element)
    return result