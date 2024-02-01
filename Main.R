# Copyright 2024 Observational Health Data Sciences and Informatics
#
# This file is part of AtlasModule
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Adding library references that are required for Strategus
library(CohortGenerator)
library(DatabaseConnector)
library(keyring)
library(ParallelLogger)
library(SqlRender)

# Adding RSQLite so that we can test modules with Eunomia
library(RSQLite)

# Adding ROhdsiWebApi to make connections to WebAPI
library(ROhdsiWebApi)

# Module methods -------------------------
getModuleInfo <- function() {
  checkmate::assert_file_exists("MetaData.json")
  return(ParallelLogger::loadSettingsFromJson("MetaData.json"))
}

execute <- function(jobContext) {
  
}

executeOld <- function(jobContext) {
  rlang::inform("Validating inputs")
  inherits(jobContext, "list")
  
  if (is.null(jobContext$settings)) {
    stop("Analysis settings not found in job context")
  }
  if (is.null(jobContext$moduleExecutionSettings)) {
    stop("Execution settings not found in job context")
  }
  
  moduleInfo <- getModuleInfo()
  
  baseUrl <- jobContext$moduleExecutionSettings$baseUrl
  if (jobContext$moduleExecutionSettings$authMethod != "none") {
    rlang::inform("Execution Authentication to WebAPI")
    
    ROhdsiWebApi::authorizeWebApi(baseUrl = baseUrl,
                                  authMethod = jobContext$moduleExecutionSettings$authMethod, 
                                  webApiUsername = jobContext$moduleExecutionSettings$webApiUsername,
                                  webApiPassword = jobContext$moduleExecutionSettings$webApiPassword)
  }

  rlang::inform("Bringing Cohort Definitions into Package")
  for (atlasCohortId in atlasCohortIds) {
    ROhdsiWebApi::insertCohortDefinitionInPackage(cohortId = atlasCohortId, 
                                                  jsonFolder = jsonFolder,
                                                  sqlFolder = sqlFolder, 
                                                  baseUrl = baseUrl, 
                                                  generateStats = FALSE)
  }
  
  if (jobContext$settings$copyCohortResults) {
    rlang::inform("Copying Cohort Results into Package")
    
    connectionDetails = jobContext$moduleExecutionSettings$connectionDetails
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection = connection))
    
    resultsDatabaseSchema = jobContext$moduleExecutionSettings$resultsDatabaseSchema
    targetDatabaseSchema = jobContext$moduleExecutionSettings$workDatabaseSchema
    targetTable = jobContext$moduleExecutionSettings$cohortTableNames$cohortTable
    
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "copyCohortResults.sql", 
                                             packageName = "AtlasModule", 
                                             dbms = connectionDetails$dbms,
                                             resultsDatabaseSchema = resultsDatabaseSchema,
                                             targetDatabaseSchema = targetDatabaseSchema,
                                             targetTable = targetTable,
                                             cohortIds = atlasCohortIds)
    
    DatabaseConnector::executeSql(connection = connection, sql = sql)
  }
  
  
 
}

createDataModelSchema <- function(jobContext) {
  checkmate::assert_class(jobContext$moduleExecutionSettings$resultsConnectionDetails, "ConnectionDetails")
  checkmate::assert_string(jobContext$moduleExecutionSettings$resultsDatabaseSchema)
  connectionDetails <- jobContext$moduleExecutionSettings$resultsConnectionDetails
  moduleInfo <- getModuleInfo()
  tablePrefix <- moduleInfo$TablePrefix
  resultsDatabaseSchema <- jobContext$moduleExecutionSettings$resultsDatabaseSchema
  # Workaround for issue https://github.com/tidyverse/vroom/issues/519:
  readr::local_edition(1)
  resultsDataModel <- ResultModelManager::loadResultsDataModelSpecifications(
    filePath = system.file(
      "settings/resultsDataModelSpecification.csv",
      package = "Characterization"
    )
  )
  resultsDataModel$tableName <- paste0(tablePrefix, resultsDataModel$tableName)
  sql <- ResultModelManager::generateSqlSchema(
    schemaDefinition = resultsDataModel
  )
  sql <- SqlRender::render(
    sql = sql,
    database_schema = resultsDatabaseSchema
  )
  connection <- DatabaseConnector::connect(
    connectionDetails = connectionDetails
  )
  on.exit(DatabaseConnector::disconnect(connection))
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql
  )
}