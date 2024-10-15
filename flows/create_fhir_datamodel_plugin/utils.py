from typing import Callable
from shared_utils.dao.DBDao import DBDao
import sqlalchemy as sql
from prefect import get_run_logger
logger = get_run_logger()

class fhirUtils():
    def _getFhirDataModelForObject(self, listOfObjectColumns, isArray):
        return f"struct({', '.join(listOfObjectColumns)}){'[]' if isArray else ''}"

    def _getPropertyForTable(self, duckdbDataTypes, dataStructure, property, propertyType = None):
        if propertyType:
            return f"\"{property}\" {propertyType}"
        elif dataStructure[property] in duckdbDataTypes:
            return f"\"{property}\" {duckdbDataTypes[dataStructure[property]]}"
        elif dataStructure[property] == "json":
            return f"\"{property}\" {dataStructure[property]}"
        else:
            raise f"{property} has undefined property type"

    def _getFhirDataModel(self, duckdbDataTypes, dataStructure, property):
        if (type(dataStructure[property]) == str or type(dataStructure[property]) == bool or type(dataStructure[property]) == int):
            return self._getPropertyForTable(duckdbDataTypes, dataStructure, property)
        else:
            listOfTableColumns = []
            isArray = False
            #Nested extension objects are set as {}
            if type(dataStructure[property]) == dict and len(dataStructure[property].keys()) == 0:
                return self._getPropertyForTable(duckdbDataTypes, dataStructure, property, duckdbDataTypes['string'])
            else:
                currentProperty = dataStructure[property]
                # Is array?
                if type(dataStructure[property]) == list:
                    isArray = True
                    currentProperty = dataStructure[property][0]
                #For properties with array of strings
                if type(dataStructure[property]) == list and (type(currentProperty) == str or type(currentProperty) == bool or type(currentProperty) == int):
                    return self._getPropertyForTable(duckdbDataTypes, dataStructure, property, duckdbDataTypes[currentProperty])+'[]'
                else:
                    for childProperty in currentProperty:
                        listOfTableColumns.append(self._getFhirDataModel(duckdbDataTypes, currentProperty, childProperty))
                    return self._getPropertyForTable(duckdbDataTypes, dataStructure, property, self._getFhirDataModelForObject(listOfTableColumns, isArray))

    def _getDuckdbColumnString(self, duckdbDataTypes, dataStructure, concatColumns:bool):
        listOfTableColumns = []
        for property in dataStructure:
            if property.find('_') == -1:
                listOfTableColumns.append(self._getFhirDataModel(duckdbDataTypes, dataStructure, property))
        return ', '.join(listOfTableColumns) if concatColumns else listOfTableColumns

    def _getNestedProperty(self, jsonschema, propertyPath, propertyDetails, heirarchy):
        if '$ref' in propertyDetails:
            if propertyPath != None:
                subProperties = jsonschema['definitions'][0][propertyPath]
                if 'properties' in subProperties:
                    subProperties['parsedProperties'] = dict()
                    for subProperty in subProperties['properties']:
                        if subProperty[0:1] != "_":
                            subPropertyDetails = subProperties['properties'][subProperty]
                            subPropertyPath =  self._getPropertyPath(subPropertyDetails)
                            if self._isCustomType(subPropertyPath):
                                subProperties['parsedProperties'][subProperty] = ['string']
                            elif subPropertyPath == 'Meta' or subPropertyPath == 'Extension':
                                subProperties['parsedProperties'][subProperty] = 'json'
                            else:
                                #Check if the property is already covered previously
                                if subPropertyPath != None and heirarchy.find(subPropertyPath) > -1:
                                    subProperties['parsedProperties'][subProperty] = dict()
                                else:
                                    newHeirarchy = heirarchy + ("/" + subPropertyPath if subPropertyPath != None else "")
                                    subProperties['parsedProperties'][subProperty] = self._getNestedProperty(jsonschema, subPropertyPath, subPropertyDetails, newHeirarchy)
                    return subProperties['parsedProperties']
                else:
                    return subProperties['type'] if 'type' in subProperties else "string"
        elif 'type' in propertyDetails and  propertyDetails['type'] == 'array': 
            if 'enum' in propertyDetails['items']:
                return ['string']
            elif propertyPath != None:
                subProperties = jsonschema['definitions'][0][propertyPath]
                if 'properties' in subProperties:
                    subProperties['parsedProperties'] = dict()
                    for subProperty in subProperties['properties']:
                        if subProperty[0:1] != "_":
                            subPropertyDetails = subProperties['properties'][subProperty]
                            subPropertyPath =  self._getPropertyPath(subPropertyDetails)
                            if self._isCustomType(subPropertyPath):
                                subProperties['parsedProperties'][subProperty] = ['string']
                            elif subPropertyPath == 'Meta' or subPropertyPath == 'Extension':
                                subProperties['parsedProperties'][subProperty] = 'json'
                            else:
                                #Check if the property is already covered previously
                                if subPropertyPath != None and heirarchy.find(subPropertyPath) > -1:
                                    subProperties['parsedProperties'][subProperty] = dict()
                                else:
                                    newHeirarchy = heirarchy + ("/" + subPropertyPath if subPropertyPath != None else "")
                                    subProperties['parsedProperties'][subProperty] = self._getNestedProperty(jsonschema, subPropertyPath, subPropertyDetails, newHeirarchy)
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

    def _isCustomType(self, properyPath):
        if properyPath == 'ResourceList' or properyPath == 'Resource' or properyPath == 'ProjectSetting' or properyPath == 'ProjectSite' or properyPath == 'ProjectLink' or properyPath == 'ProjectMembershipAccess' or properyPath == 'AccessPolicyResource' or properyPath == 'AccessPolicyIpAccessRule' or properyPath == 'UserConfigurationMenu' or properyPath == 'UserConfigurationSearch'or properyPath == 'UserConfigurationOption' or properyPath == 'BulkDataExportOutput' or properyPath == 'BulkDataExportDeleted' or properyPath == 'BulkDataExportError' or properyPath == 'AgentSetting' or properyPath == 'AgentChannel' or properyPath == 'ViewDefinitionConstant' or properyPath == 'ViewDefinitionSelect' or properyPath == 'ViewDefinitionWhere':
            return True

    def _getPropertyPath(self, fhirDefinitionProperties):
        if '$ref' in fhirDefinitionProperties:
            return fhirDefinitionProperties['$ref'][fhirDefinitionProperties['$ref'].rindex('/') + 1: len(fhirDefinitionProperties['$ref'])]
        elif 'items' in fhirDefinitionProperties and '$ref' in fhirDefinitionProperties['items']:
            return fhirDefinitionProperties['items']['$ref'][fhirDefinitionProperties['items']['$ref'].rindex('/') + 1: len(fhirDefinitionProperties['items']['$ref'])]
        else:
            return None

    def _isResource(self, schema, resourceDefinition: any):
        return schema['discriminator'][0]['mapping'][resourceDefinition] != None

    def _getFhirTableStructure(self, jsonSchema, fhirDefinitionName):
        try:
            fhirDefinition = jsonSchema['definitions'][0][fhirDefinitionName]
            fhirDefinition['parsedProperties'] = dict()
            if self._isResource(jsonSchema, fhirDefinitionName):
                if 'properties' in fhirDefinition:
                    for property in fhirDefinition['properties']:
                        properyPath = self._getPropertyPath(fhirDefinition['properties'][property])
                        if self._isCustomType(properyPath):
                            fhirDefinition['parsedProperties'][property] = ['string']
                        elif properyPath == 'Meta' or properyPath == 'Extension':
                            fhirDefinition['parsedProperties'][property] = 'json'
                        else:
                            fhirDefinition['parsedProperties'][property] = self._getNestedProperty(jsonSchema, properyPath, fhirDefinition['properties'][property], properyPath)
                    return fhirDefinition['parsedProperties']
                else:
                    return f"The input FHIR resource {fhirDefinition} has no properties defined"
            else:
                return f"The input resource {fhirDefinition} is not a FHIR resource"
        except Exception as err:
            print(err)
            print(f"Error while creating duckdb table for resource : {fhirDefinitionName}")
            raise err

    def _getFhirDataTypes(self, jsonSchema):
        dataTypes = {}
        for definition in jsonSchema['definitions'][0]:
            if "type" in jsonSchema['definitions'][0][definition]:
                dataTypes[jsonSchema['definitions'][0][definition]['type']] = jsonSchema['definitions'][0][definition]['type']
        return dataTypes

    def _convertFhirDataTypesToDuckdb(self, jsonSchema):
        dataTypes = self._getFhirDataTypes(jsonSchema)
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

    def _createFhirTable(self, fhirDefinition, fhirTableDefinition, dbdao: DBDao):
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
    
    def _parseFhirSchemaJsonFile(self, fhirSchema, dbdao: DBDao):
        fhirResources = fhirSchema['discriminator'][0]['mapping']
        duckdbDataTypes = self._convertFhirDataTypesToDuckdb(fhirSchema)
        for resource in fhirResources:
            parsedFhirDefinitions = self._getFhirTableStructure(fhirSchema, resource)
            duckdbTableStructure = self._getDuckdbColumnString(duckdbDataTypes, parsedFhirDefinitions, True)
            self._createFhirTable(resource, duckdbTableStructure, dbdao)
            print(f'Successfully created table: {resource}')
        return True

    def readJsonFileAndCreateDuckdbTables(self, database_code: str, schema_name: str):
        logger = get_run_logger()
        schemaPath = '/app/flows/create_fhir_datamodel_plugin/fhir.schema.json'
        try:
            dbdao = DBDao(use_cache_db=True,
                      database_code=database_code, 
                      schema_name=schema_name,
                      connectToDuckdb=True)
            engine = dbdao.engine
            with engine.connect() as connection:
                try:
                    logger.info('Read fhir.schema.json file to get fhir definitions')
                    get_schema_json = sql.text(f"select * from '{schemaPath}'")
                    fhir_schema = connection.execute(get_schema_json).fetchall()
                    logger.info(fhir_schema)
                except Exception as e:
                    logger.error(f"Failed to get fhir schema json file': {e}")
                    raise e
            self._parseFhirSchemaJsonFile(fhir_schema, dbdao=dbdao)
            logger.info('FHIR DataModel created successfuly!')
            return True
        except Exception as err:
            logger.info(err)