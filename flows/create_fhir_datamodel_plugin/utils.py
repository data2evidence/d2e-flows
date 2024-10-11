from typing import Callable
from shared_utils.dao.DBDao import DBDao
from prefect import get_run_logger
logger = get_run_logger()

class fhirUtils():
    def __init__(self, database_code: str, schema_name: str):
        self.dbdao = DBDao(use_cache_db=True,
                      database_code=database_code, 
                      schema_name=schema_name,
                      connectToDuckdb=True)

    def _getFhirDataModelForObject(self, listOfObjectColumns, isArray):
        return f"struct({listOfObjectColumns.join(', ')}){'[]' if isArray else ''}"

    def _getPropertyForTable(self, duckdbDataTypes, dataStructure, property, propertyType = None):
        if propertyType:
            return f"\"{property}\" {propertyType}"
        elif duckdbDataTypes[dataStructure[property]] != None:
            return f"\"{property}\" {duckdbDataTypes[dataStructure[property]]}"
        elif duckdbDataTypes[dataStructure[property]] == None and dataStructure[property] == "json":
            return f"\"{property}\" {dataStructure[property]}"
        else:
            raise f"{property} has undefined property type"

    def _getFhirDataModel(self, duckdbDataTypes, dataStructure, property):
        if type(dataStructure[property]) != "object":
            return self._getPropertyForTable(duckdbDataTypes, dataStructure, property)
        elif type(dataStructure[property]) == "object":
            listOfTableColumns = []
            isArray = False
            #Nested extension objects are set as {}
            if dataStructure[property].keys().length == 0:
                return self._getPropertyForTable(duckdbDataTypes, dataStructure, property, duckdbDataTypes['string'])
            else:
                currentProperty = dataStructure[property]
                if currentProperty.length != None and dataStructure[property].length > 0:
                    isArray = True
                    currentProperty =  dataStructure[property][0]
                #For properties with array of strings
                if dataStructure[property].length > 0 and duckdbDataTypes[currentProperty] != None:
                    return self._getPropertyForTable(duckdbDataTypes, dataStructure, property, duckdbDataTypes[currentProperty])+'[]'
                else:
                    for childProperty in currentProperty:
                        listOfTableColumns.push(self._getFhirDataModel(duckdbDataTypes, currentProperty, childProperty))
                    return self._getPropertyForTable(duckdbDataTypes, dataStructure, property, self._getFhirDataModelForObject(listOfTableColumns, isArray))

    def _getDuckdbColumnString(self, duckdbDataTypes, dataStructure, concatColumns:bool):
        listOfTableColumns = []
        for property in dataStructure:
            if property.indexOf('_') == -1:
                listOfTableColumns.push(self._getFhirDataModel(duckdbDataTypes, dataStructure, property))
        return listOfTableColumns.join(', ') if concatColumns else listOfTableColumns

    def _getNestedProperty(self, jsonschema, propertyPath, propertyDetails, heirarchy):
        if propertyDetails["$ref"] != None:
            if propertyPath != None:
                subProperties = jsonschema.definitions[propertyPath]
                if subProperties.properties != None:
                    subProperties.parsedProperties = {}
                    for subProperty in subProperties.properties:
                        if subProperty.substring(0, 1) != "_":
                            subPropertyDetails = subProperties.properties[subProperty]
                            subPropertyPath =  self._getPropertyPath(subPropertyDetails)
                            if self._isCustomType(subPropertyPath):
                                subProperties.parsedProperties[subProperty] = ['string']
                            elif subPropertyPath == 'Meta' or subPropertyPath == 'Extension':
                                subProperties.parsedProperties[subProperty] = 'json'
                            else:
                                #Check if the property is already covered previously
                                if heirarchy.indexOf(subPropertyPath) > -1:
                                    subProperties.parsedProperties[subProperty] = {}
                                else:
                                    newHeirarchy = heirarchy + "/" + subPropertyPath
                                    subProperties.parsedProperties[subProperty] = self._getNestedProperty(jsonschema, subPropertyPath, subPropertyDetails, newHeirarchy)
                    return subProperties.parsedProperties
                else:
                    return "string" if subProperties.type == None else subProperties.type
        elif propertyDetails.type == "array": 
            if propertyDetails.items.enum != None:
                return ['string']
            elif propertyPath != None:
                subProperties = jsonschema.definitions[propertyPath]
                if subProperties.properties != None:
                    subProperties.parsedProperties = {}
                    for subProperty in subProperties.properties:
                        if subProperty.substring(0, 1) != "_":
                            subPropertyDetails = subProperties.properties[subProperty]
                            subPropertyPath =  self._getPropertyPath(subPropertyDetails)
                            if self._isCustomType(subPropertyPath):
                                subProperties.parsedProperties[subProperty] = ['string']
                            elif subPropertyPath == 'Meta' or subPropertyPath == 'Extension':
                                subProperties.parsedProperties[subProperty] = 'json'
                            else:
                                #Check if the property is already covered previously
                                if heirarchy.indexOf(subPropertyPath) > -1:
                                    subProperties.parsedProperties[subProperty] = {}
                                else:
                                    newHeirarchy = heirarchy + "/" + subPropertyPath
                                    subProperties.parsedProperties[subProperty] = self._getNestedProperty(jsonschema, subPropertyPath, subPropertyDetails, newHeirarchy)
                    return [subProperties.parsedProperties]
                else:
                    return ["string"] if subProperties.type == None else [subProperties.type]
        elif propertyDetails.enum != None:
            return "string"
        elif propertyDetails.const != None:
            return "string"
        elif propertyDetails.type == None:
            return 'string'
        else:
            return propertyDetails.type

    def _isCustomType(self, properyPath):
        if properyPath == 'ResourceList' or properyPath == 'Resource' or properyPath == 'ProjectSetting' or properyPath == 'ProjectSite' or properyPath == 'ProjectLink' or properyPath == 'ProjectMembershipAccess' or properyPath == 'AccessPolicyResource' or properyPath == 'AccessPolicyIpAccessRule' or properyPath == 'UserConfigurationMenu' or properyPath == 'UserConfigurationSearch'or properyPath == 'UserConfigurationOption' or properyPath == 'BulkDataExportOutput' or properyPath == 'BulkDataExportDeleted' or properyPath == 'BulkDataExportError' or properyPath == 'AgentSetting' or properyPath == 'AgentChannel' or properyPath == 'ViewDefinitionConstant' or properyPath == 'ViewDefinitionSelect' or properyPath == 'ViewDefinitionWhere':
            return True

    def _getPropertyPath(self, fhirDefinitionProperties):
        if fhirDefinitionProperties["$ref"] != None:
            return fhirDefinitionProperties["$ref"].substring(fhirDefinitionProperties["$ref"].lastIndexOf("/") + 1, fhirDefinitionProperties["$ref"].length)
        elif fhirDefinitionProperties.items != None and fhirDefinitionProperties.items["$ref"] != None:
            return fhirDefinitionProperties.items["$ref"].substring(fhirDefinitionProperties.items["$ref"].lastIndexOf("/") + 1, fhirDefinitionProperties.items["$ref"].length)
        else:
            return None

    def _isResource(self, schema, resourceDefinition: any):
        return schema.discriminator.mapping[resourceDefinition] != None

    def _getFhirTableStructure(self, jsonSchema, fhirDefinitionName):
        try:
            fhirDefinition = jsonSchema.definitions[fhirDefinitionName]
            fhirDefinition.parsedProperties = {}
            if self._isResource(jsonSchema, fhirDefinitionName):
                if fhirDefinition.properties != None:
                    for property in fhirDefinition.properties:
                        properyPath = self._getPropertyPath(fhirDefinition.properties[property])
                        if self._isCustomType(properyPath):
                            fhirDefinition.parsedProperties[property] = ['string']
                        elif properyPath == 'Meta' or properyPath == 'Extension':
                            fhirDefinition.parsedProperties[property] = 'json'
                        else:
                            fhirDefinition.parsedProperties[property] = self._getNestedProperty(jsonSchema, properyPath, fhirDefinition.properties[property], properyPath)
                    return fhirDefinition.parsedProperties
                else:
                    return f"The input FHIR resource {fhirDefinition} has no properties defined"
            else:
                return f"The input resource {fhirDefinition} is not a FHIR resource"
        except Exception as err:
            logger.error(err)
            logger.error(f"Error while creating duckdb table for resource : {fhirDefinitionName}")
            raise err

    def _getFhirDataTypes(self, jsonSchema):
        dataTypes = {}
        for definition in jsonSchema.definitions:
            if jsonSchema.definitions[definition].type != None:
                dataTypes[jsonSchema.definitions[definition].type] = jsonSchema.definitions[definition].type
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

    def _createFhirTable(self, dbdao: DBDao, fhirDefinition, fhirTableDefinition, callback: Callable):
        try:
            dbdao.execute_query(f"create or replace table {fhirDefinition}Fhir ({fhirTableDefinition})", lambda res: callback(res, fhirDefinition))
        except Exception as err:
            logger.info(f"Error creating table {fhirDefinition}")
            raise err
    
    def _parseFhirSchemaJsonFile(self, fhirSchema):
        fhirResources = fhirSchema[0].discriminator.mapping
        duckdbDataTypes = self.convertFhirDataTypesToDuckdb(fhirSchema[0])
        for resource in fhirResources:
            parsedFhirDefinitions = self._getFhirTableStructure(fhirSchema[0], resource)
            duckdbTableStructure = self._getDuckdbColumnString(duckdbDataTypes, parsedFhirDefinitions, True)
            self._createFhirTable(self.dbdao, resource, duckdbTableStructure, lambda res,fhirResource: logger.info(f"Fhir table created for resource {fhirResource}"))
        return True

    def readJsonFileAndCreateDuckdbTables(self):
        logger = get_run_logger()
        schemaPath = ''
        try:
            logger.info('Read fhir.schema.json file to get fhir definitions')
            self.dbdao.execute_fetch_query(f"select * from read_json('{schemaPath}')", self._parseFhirSchemaJsonFile)
            logger.info('FHIR DataModel created successfuly!')
            return True
        except Exception as err:
            logger.info(err)