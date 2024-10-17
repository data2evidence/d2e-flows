from shared_utils.dao.DBDao import DBDao
import sqlalchemy as sql
from prefect.logging import get_run_logger
from prefect.variables import Variable

def getFhirDataModelForObject(listOfObjectColumns, isArray):
    return f"struct({', '.join(listOfObjectColumns)}){'[]' if isArray else ''}"

def getPropertyForTable(duckdbDataTypes, dataStructure, property, propertyType = None):
    if propertyType:
        return f"\"{property}\" {propertyType}"
    elif dataStructure[property] in duckdbDataTypes:
        return f"\"{property}\" {duckdbDataTypes[dataStructure[property]]}"
    elif dataStructure[property] == "json":
        return f"\"{property}\" {dataStructure[property]}"
    else:
        raise f"{property} has undefined property type"

def getFhirDataModel(duckdbDataTypes, dataStructure, property):
    if (type(dataStructure[property]) == str or type(dataStructure[property]) == bool or type(dataStructure[property]) == int):
        return getPropertyForTable(duckdbDataTypes, dataStructure, property)
    else:
        listOfTableColumns = []
        isArray = False
        #Nested extension objects are set as {}
        if type(dataStructure[property]) == dict and len(dataStructure[property].keys()) == 0:
            return getPropertyForTable(duckdbDataTypes, dataStructure, property, duckdbDataTypes['string'])
        else:
            currentProperty = dataStructure[property]
            # Is array?
            if type(dataStructure[property]) == list:
                isArray = True
                currentProperty = dataStructure[property][0]
            #For properties with array of strings
            if type(dataStructure[property]) == list and (type(currentProperty) == str or type(currentProperty) == bool or type(currentProperty) == int):
                return getPropertyForTable(duckdbDataTypes, dataStructure, property, duckdbDataTypes[currentProperty])+'[]'
            else:
                for childProperty in currentProperty:
                    listOfTableColumns.append(getFhirDataModel(duckdbDataTypes, currentProperty, childProperty))
                return getPropertyForTable(duckdbDataTypes, dataStructure, property, getFhirDataModelForObject(listOfTableColumns, isArray))

def getDuckdbColumnString(duckdbDataTypes, dataStructure, concatColumns:bool):
    listOfTableColumns = []
    for property in dataStructure:
        if property.find('_') == -1:
            listOfTableColumns.append(getFhirDataModel(duckdbDataTypes, dataStructure, property))
    return ', '.join(listOfTableColumns) if concatColumns else listOfTableColumns

def getNestedProperty(jsonschema, propertyPath, propertyDetails, heirarchy):
    if '$ref' in propertyDetails:
        if propertyPath != None:
            subProperties = jsonschema[5][propertyPath]
            if 'properties' in subProperties:
                subProperties['parsedProperties'] = dict()
                for subProperty in subProperties['properties']:
                    if subProperty[0:1] != "_":
                        subPropertyDetails = subProperties['properties'][subProperty]
                        subPropertyPath =  getPropertyPath(subPropertyDetails)
                        if isCustomType(subPropertyPath):
                            subProperties['parsedProperties'][subProperty] = ['string']
                        elif subPropertyPath == 'Meta' or subPropertyPath == 'Extension':
                            subProperties['parsedProperties'][subProperty] = 'json'
                        else:
                            #Check if the property is already covered previously
                            if subPropertyPath != None and heirarchy.find(subPropertyPath) > -1:
                                subProperties['parsedProperties'][subProperty] = dict()
                            else:
                                newHeirarchy = heirarchy + ("/" + subPropertyPath if subPropertyPath != None else "")
                                subProperties['parsedProperties'][subProperty] = getNestedProperty(jsonschema, subPropertyPath, subPropertyDetails, newHeirarchy)
                return subProperties['parsedProperties']
            else:
                return subProperties['type'] if 'type' in subProperties else "string"
    elif 'type' in propertyDetails and  propertyDetails['type'] == 'array': 
        if 'enum' in propertyDetails['items']:
            return ['string']
        elif propertyPath != None:
            subProperties = jsonschema[5][propertyPath]
            if 'properties' in subProperties:
                subProperties['parsedProperties'] = dict()
                for subProperty in subProperties['properties']:
                    if subProperty[0:1] != "_":
                        subPropertyDetails = subProperties['properties'][subProperty]
                        subPropertyPath =  getPropertyPath(subPropertyDetails)
                        if isCustomType(subPropertyPath):
                            subProperties['parsedProperties'][subProperty] = ['string']
                        elif subPropertyPath == 'Meta' or subPropertyPath == 'Extension':
                            subProperties['parsedProperties'][subProperty] = 'json'
                        else:
                            #Check if the property is already covered previously
                            if subPropertyPath != None and heirarchy.find(subPropertyPath) > -1:
                                subProperties['parsedProperties'][subProperty] = dict()
                            else:
                                newHeirarchy = heirarchy + ("/" + subPropertyPath if subPropertyPath != None else "")
                                subProperties['parsedProperties'][subProperty] = getNestedProperty(jsonschema, subPropertyPath, subPropertyDetails, newHeirarchy)
                return [subProperties['parsedProperties']]
            else:
                return [subProperties['type']] if 'type' in subProperties else ["string"]
    elif 'enum' in propertyDetails:
        return "string"
    elif 'const' in propertyDetails:
        return "string"
    elif 'type' in propertyDetails == None:
        return 'string'
    else:
        return propertyDetails['type']

def isCustomType(properyPath):
    if properyPath == 'ResourceList' or properyPath == 'Resource' or properyPath == 'ProjectSetting' or properyPath == 'ProjectSite' or properyPath == 'ProjectLink' or properyPath == 'ProjectMembershipAccess' or properyPath == 'AccessPolicyResource' or properyPath == 'AccessPolicyIpAccessRule' or properyPath == 'UserConfigurationMenu' or properyPath == 'UserConfigurationSearch'or properyPath == 'UserConfigurationOption' or properyPath == 'BulkDataExportOutput' or properyPath == 'BulkDataExportDeleted' or properyPath == 'BulkDataExportError' or properyPath == 'AgentSetting' or properyPath == 'AgentChannel' or properyPath == 'ViewDefinitionConstant' or properyPath == 'ViewDefinitionSelect' or properyPath == 'ViewDefinitionWhere':
        return True

def getPropertyPath(fhirDefinitionProperties):
    if '$ref' in fhirDefinitionProperties:
        return fhirDefinitionProperties['$ref'][fhirDefinitionProperties['$ref'].rindex('/') + 1: len(fhirDefinitionProperties['$ref'])]
    elif 'items' in fhirDefinitionProperties and '$ref' in fhirDefinitionProperties['items']:
        return fhirDefinitionProperties['items']['$ref'][fhirDefinitionProperties['items']['$ref'].rindex('/') + 1: len(fhirDefinitionProperties['items']['$ref'])]
    else:
        return None

def isResource(schema, resourceDefinition: any):
    return schema[3]['mapping'][resourceDefinition] != None

def getFhirTableStructure(jsonSchema, fhirDefinitionName):
    try:
        fhirDefinition = jsonSchema[5][fhirDefinitionName]
        fhirDefinition['parsedProperties'] = dict()
        if isResource(jsonSchema, fhirDefinitionName):
            if 'properties' in fhirDefinition:
                for property in fhirDefinition['properties']:
                    properyPath = getPropertyPath(fhirDefinition['properties'][property])
                    if isCustomType(properyPath):
                        fhirDefinition['parsedProperties'][property] = ['string']
                    elif properyPath == 'Meta' or properyPath == 'Extension':
                        fhirDefinition['parsedProperties'][property] = 'json'
                    else:
                        fhirDefinition['parsedProperties'][property] = getNestedProperty(jsonSchema, properyPath, fhirDefinition['properties'][property], properyPath)
                return fhirDefinition['parsedProperties']
            else:
                return f"The input FHIR resource {fhirDefinition} has no properties defined"
        else:
            return f"The input resource {fhirDefinition} is not a FHIR resource"
    except Exception as err:
        print(err)
        print(f"Error while creating duckdb table for resource : {fhirDefinitionName}")
        raise err

def getFhirDataTypes(jsonSchema):
    dataTypes = {}
    for definition in jsonSchema[0][5]:
        if "type" in jsonSchema[0][5][definition]:
            dataTypes[jsonSchema[0][5][definition]['type']] = jsonSchema[0][5][definition]['type']
    return dataTypes

def convertFhirDataTypesToDuckdb(jsonSchema):
    dataTypes = getFhirDataTypes(jsonSchema)
    for fhirDataType in dataTypes:
        match dataTypes[fhirDataType]:
            case "string": 
                dataTypes[fhirDataType] = "varchar"
            case "number":
                dataTypes[fhirDataType] = "integer"
            case "boolean":
                dataTypes[fhirDataType] = "boolean"
            case "json": 
                dataTypes[fhirDataType] = "json"
    return dataTypes

def createFhirTable(fhirDefinition, fhirTableDefinition, dbdao: DBDao):
    logger = get_run_logger()
    engine = dbdao.engine
    with engine.connect() as connection:
        trans = connection.begin()
        try:
            create_fhir_datamodel_table = sql.text(f"create or replace table {fhirDefinition}Fhir ({fhirTableDefinition})")
            connection.execute(create_fhir_datamodel_table)
            trans.commit()
        except Exception as e:
            trans.rollback()
            logger.error(f"Failed to create table: {fhirDefinition}: {e}")
            raise e

def parseFhirSchemaJsonFile(fhirSchema, dbdao: DBDao):
    logger = get_run_logger()
    #Get Discriminator from the list 
    fhirResources = fhirSchema[0][3]['mapping']
    duckdbDataTypes = convertFhirDataTypesToDuckdb(fhirSchema)
    for resource in fhirResources:
        parsedFhirDefinitions = getFhirTableStructure(fhirSchema[0], resource)
        duckdbTableStructure = getDuckdbColumnString(duckdbDataTypes, parsedFhirDefinitions, True)
        createFhirTable(resource, duckdbTableStructure, dbdao)
        logger.info(f'Successfully created table: {resource}')
    return True

def readJsonFileAndCreateDuckdbTables(database_code: str, schema_name: str, vocab_schema: str):
        logger = get_run_logger()
        schemaPath = Variable.get("fhir_schema_file") + '/fhir.schema.json'
        try:
            dbdao = DBDao(use_cache_db=True,
                      database_code=database_code, 
                      schema_name=schema_name,
                      connectToDuckdb=True,
                      vocab_schema=vocab_schema)
            engine = dbdao.engine
            with engine.connect() as connection:
                try:
                    logger.info('Read fhir.schema.json file to get fhir definitions')
                    get_schema_json = sql.text(f"select * from '{schemaPath}'")
                    fhir_schema = connection.execute(get_schema_json).fetchall()
                except Exception as e:
                    logger.error(f"Failed to get fhir schema json file': {e}")
                    raise e
            parseFhirSchemaJsonFile(fhir_schema, dbdao)
            logger.info('FHIR DataModel created successfuly!')
            return True
        except Exception as err:
            logger.info(err)