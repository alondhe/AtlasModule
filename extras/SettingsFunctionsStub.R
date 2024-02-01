createAtlasModuleSpecifications <- function(atlasCohortIds,
                                            copyCohortResults = FALSE,
                                            jsonFolder = "cohorts",
                                            sqlFolder = "sql") {

  settings <- list(
    atlasCohortIds = atlasCohortIds,
    copyCohortResults = copyCohortResults,
    jsonFolder = jsonFolder,
    sqlFolder = sqlFolder
  )
  
  specifications <- list(
    module = "%module%",
    version = "%version%",
    remoteRepo = "github.com",
    remoteUsername = "ohdsi",
    settings = settings
  )
  class(specifications) <- c("AtlasModuleSpecifications", "ModuleSpecifications")
  return(specifications)
}