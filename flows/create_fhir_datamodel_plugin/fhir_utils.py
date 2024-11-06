import sqlalchemy as sql
from shared_utils.dao.DBDao import DBDao

from prefect.logging import get_run_logger
from prefect.variables import Variable

def get_fhir_dataModel_for_object(list_of_object_columns, is_array):
    return f"struct({', '.join(list_of_object_columns)}){'[]' if is_array else ''}"

def get_property_for_table(duckdb_data_types, data_structure, property, property_type = None):
    if property_type:
        return f"\"{property}\" {property_type}"
    elif data_structure[property] in duckdb_data_types:
        return f"\"{property}\" {duckdb_data_types[data_structure[property]]}"
    elif data_structure[property] == "json":
        return f"\"{property}\" {data_structure[property]}"
    else:
        raise f"{property} has undefined property type"

def get_fhir_dataModel(duckdb_data_types, data_structure, property):
    if (type(data_structure[property]) == str or type(data_structure[property]) == bool or type(data_structure[property]) == int):
        return get_property_for_table(duckdb_data_types, data_structure, property)
    else:
        list_of_table_columns = []
        is_array = False
        #Nested extension objects are set as {}
        if type(data_structure[property]) == dict and len(data_structure[property].keys()) == 0:
            return get_property_for_table(duckdb_data_types, data_structure, property, duckdb_data_types['string'])
        else:
            current_property = data_structure[property]
            # Is array?
            if type(data_structure[property]) == list:
                is_array = True
                current_property = data_structure[property][0]
            #For properties with array of strings
            if type(data_structure[property]) == list and (type(current_property) == str or type(current_property) == bool or type(current_property) == int):
                return get_property_for_table(duckdb_data_types, data_structure, property, duckdb_data_types[current_property])+'[]'
            else:
                for child_property in current_property:
                    list_of_table_columns.append(get_fhir_dataModel(duckdb_data_types, current_property, child_property))
                return get_property_for_table(duckdb_data_types, data_structure, property, get_fhir_dataModel_for_object(list_of_table_columns, is_array))

def get_duckdb_column_string(duckdb_data_types, data_structure, concat_columns:bool):
    list_of_table_columns = []
    for property in data_structure:
        if property.find('_') == -1:
            list_of_table_columns.append(get_fhir_dataModel(duckdb_data_types, data_structure, property))
    return ', '.join(list_of_table_columns) if concat_columns else list_of_table_columns

def get_nested_property(json_schema, property_path, property_details, heirarchy):
    if '$ref' in property_details:
        if property_path != None:
            sub_properties = json_schema[5][property_path]
            if 'properties' in sub_properties:
                sub_properties['parsedProperties'] = dict()
                for sub_property in sub_properties['properties']:
                    if sub_property[0:1] != "_":
                        sub_property_details = sub_properties['properties'][sub_property]
                        sub_property_path =  get_property_path(sub_property_details)
                        if is_custom_type(sub_property_path):
                            sub_properties['parsedProperties'][sub_property] = ['string']
                        elif sub_property_path == 'Meta' or sub_property_path == 'Extension':
                            sub_properties['parsedProperties'][sub_property] = 'json'
                        else:
                            #Check if the property is already covered previously
                            if sub_property_path != None and heirarchy.find(sub_property_path) > -1:
                                sub_properties['parsedProperties'][sub_property] = dict()
                            else:
                                newHeirarchy = heirarchy + ("/" + sub_property_path if sub_property_path != None else "")
                                sub_properties['parsedProperties'][sub_property] = get_nested_property(json_schema, sub_property_path, sub_property_details, newHeirarchy)
                return sub_properties['parsedProperties']
            else:
                return sub_properties['type'] if 'type' in sub_properties else "string"
    elif 'type' in property_details and  property_details['type'] == 'array': 
        if 'enum' in property_details['items']:
            return ['string']
        elif property_path != None:
            sub_properties = json_schema[5][property_path]
            if 'properties' in sub_properties:
                sub_properties['parsedProperties'] = dict()
                for sub_property in sub_properties['properties']:
                    if sub_property[0:1] != "_":
                        sub_property_details = sub_properties['properties'][sub_property]
                        sub_property_path =  get_property_path(sub_property_details)
                        if is_custom_type(sub_property_path):
                            sub_properties['parsedProperties'][sub_property] = ['string']
                        elif sub_property_path == 'Meta' or sub_property_path == 'Extension':
                            sub_properties['parsedProperties'][sub_property] = 'json'
                        else:
                            #Check if the property is already covered previously
                            if sub_property_path != None and heirarchy.find(sub_property_path) > -1:
                                sub_properties['parsedProperties'][sub_property] = dict()
                            else:
                                newHeirarchy = heirarchy + ("/" + sub_property_path if sub_property_path != None else "")
                                sub_properties['parsedProperties'][sub_property] = get_nested_property(json_schema, sub_property_path, sub_property_details, newHeirarchy)
                return [sub_properties['parsedProperties']]
            else:
                return [sub_properties['type']] if 'type' in sub_properties else ["string"]
    elif 'enum' in property_details:
        return "string"
    elif 'const' in property_details:
        return "string"
    elif 'type' in property_details == None:
        return 'string'
    else:
        return property_details['type']

def is_custom_type(propery_path):
    if propery_path == 'ResourceList' or propery_path == 'Resource' or propery_path == 'ProjectSetting' or propery_path == 'ProjectSite' or propery_path == 'ProjectLink' or propery_path == 'ProjectMembershipAccess' or propery_path == 'AccessPolicyResource' or propery_path == 'AccessPolicyIpAccessRule' or propery_path == 'UserConfigurationMenu' or propery_path == 'UserConfigurationSearch'or propery_path == 'UserConfigurationOption' or propery_path == 'BulkDataExportOutput' or propery_path == 'BulkDataExportDeleted' or propery_path == 'BulkDataExportError' or propery_path == 'AgentSetting' or propery_path == 'AgentChannel' or propery_path == 'ViewDefinitionConstant' or propery_path == 'ViewDefinitionSelect' or propery_path == 'ViewDefinitionWhere':
        return True

def get_property_path(fhir_definition_properties):
    if '$ref' in fhir_definition_properties:
        return fhir_definition_properties['$ref'][fhir_definition_properties['$ref'].rindex('/') + 1: len(fhir_definition_properties['$ref'])]
    elif 'items' in fhir_definition_properties and '$ref' in fhir_definition_properties['items']:
        return fhir_definition_properties['items']['$ref'][fhir_definition_properties['items']['$ref'].rindex('/') + 1: len(fhir_definition_properties['items']['$ref'])]
    else:
        return None

def is_resource(schema, resource_definition: any):
    return schema[3]['mapping'][resource_definition] != None

def get_fhir_table_structure(json_schema, fhir_definition_name):
    try:
        fhir_definition = json_schema[5][fhir_definition_name]
        fhir_definition['parsedProperties'] = dict()
        if is_resource(json_schema, fhir_definition_name):
            if 'properties' in fhir_definition:
                for property in fhir_definition['properties']:
                    propery_path = get_property_path(fhir_definition['properties'][property])
                    if is_custom_type(propery_path):
                        fhir_definition['parsedProperties'][property] = ['string']
                    elif propery_path == 'Meta' or propery_path == 'Extension':
                        fhir_definition['parsedProperties'][property] = 'json'
                    else:
                        fhir_definition['parsedProperties'][property] = get_nested_property(json_schema, propery_path, fhir_definition['properties'][property], propery_path)
                return fhir_definition['parsedProperties']
            else:
                return f"The input FHIR resource {fhir_definition} has no properties defined"
        else:
            return f"The input resource {fhir_definition} is not a FHIR resource"
    except Exception as err:
        print(err)
        print(f"Error while creating duckdb table for resource : {fhir_definition_name}")
        raise err

def get_fhir_data_types(json_schema):
    data_types = {}
    for definition in json_schema[0][5]:
        if "type" in json_schema[0][5][definition]:
            data_types[json_schema[0][5][definition]['type']] = json_schema[0][5][definition]['type']
    return data_types

def convert_fhir_data_types_to_duckdb(json_schema):
    data_types = get_fhir_data_types(json_schema)
    for fhir_data_type in data_types:
        match data_types[fhir_data_type]:
            case "string": 
                data_types[fhir_data_type] = "varchar"
            case "number":
                data_types[fhir_data_type] = "integer"
            case "boolean":
                data_types[fhir_data_type] = "boolean"
            case "json": 
                data_types[fhir_data_type] = "json"
    return data_types

def create_fhir_table(fhir_definition, fhir_table_definition, dbdao: DBDao):
    logger = get_run_logger()
    engine = dbdao.engine
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            create_fhir_datamodel_table = sql.text(f"create or replace table {fhir_definition}Fhir ({fhir_table_definition})")
            connection.execute(create_fhir_datamodel_table)
            trans.commit()
        except Exception as e:
            trans.rollback()
            logger.error(f"Failed to create table: {fhir_definition}: {e}")
            raise e

def parse_fhir_schema_json_file(fhir_schema, dbdao: DBDao):
    logger = get_run_logger()
    #Get Discriminator from the list 
    fhir_resources = fhir_schema[0][3]['mapping']
    duckdb_data_types = convert_fhir_data_types_to_duckdb(fhir_schema)
    for resource in fhir_resources:
        parsed_fhir_definitions = get_fhir_table_structure(fhir_schema[0], resource)
        duckdb_table_structure = get_duckdb_column_string(duckdb_data_types, parsed_fhir_definitions, True)
        create_fhir_table(resource, duckdb_table_structure, dbdao)
        logger.info(f'Successfully created table: {resource}')
    return True

def read_json_file_and_create_duckdb_tables(database_code: str, schema_name: str, vocab_schema: str):
        logger = get_run_logger()
        schema_path = Variable.get("fhir_schema_file") + '/fhir.schema.json'
        try:
            dbdao = DBDao(use_cache_db=True,
                      database_code=database_code, 
                      schema_name=schema_name,
                      connect_to_duckdb=True,
                      vocab_schema=vocab_schema,
                      duckdb_connection_type='write')
            engine = dbdao.engine
            with engine.connect() as connection:
                try:
                    logger.info('Read fhir.schema.json file to get fhir definitions')
                    get_schema_json = sql.text(f"select * from '{schema_path}'")
                    fhir_schema = connection.execute(get_schema_json).fetchall()
                except Exception as e:
                    logger.error(f"Failed to get fhir schema json file': {e}")
                    raise e
            parse_fhir_schema_json_file(fhir_schema, dbdao)
            logger.info('FHIR DataModel created successfuly!')
            return True
        except Exception as err:
            logger.info(err)