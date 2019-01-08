#include <utility>

#ifndef PARSER_H
#define PARSER_H
/*! \file */
#include "savime.h"
#include "metadata.h"
#include "storage_manager.h"
#include "query_data_manager.h"


/**Parser is the module responsible for parsing the query text and generating
 * a query plan.*/
class Parser : public SavimeModule {

public:
  /**
  * Constructor.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  */
  Parser(ConfigurationManagerPtr configurationManager,
         SystemLoggerPtr systemLogger)
      : SavimeModule("Parser", std::move(configurationManager), std::move(systemLogger)) {}

  /**
  * Sets the standard Metadata Manager for the Parser.
  * @param metadaManager is the standard system MetadataManager.
  */
  virtual void SetMetadataManager(MetadataManagerPtr metadaManager) = 0;

  /**
  * Sets the standard Storage Manager for the Parser.
  * @param metadaManager is the standard system StorageManager.
  */
  virtual void SetStorageManager(StorageManagerPtr storageManager) = 0;

  /**
  * Infers the schema for the output TAR for the operation.
  * @param operation is the operation for which the resulting TAR schema must be inferred;
  * @return the TARPtr with the resulting TAR schema.
  */
  virtual TARPtr InferOutputTARSchema(OperationPtr operation) = 0;

  /**
  * Parses the query string and creates a QueryPlan in the QueryDataManager.
  * @param queryDataManager is the QueryDataManager reference containing the
  * query to be parsed and where the parser is going to store the newly created
  * QueryPlan.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Parse(QueryDataManagerPtr queryDataManager) = 0;
};
typedef std::shared_ptr<Parser> ParserPtr;

inline bool validadeIdentifier(string identifier) {
  const char *cIdentifier = identifier.c_str();
  for (int32_t i = 0; i < identifier.length(); ++i) {
    if (!isalpha(cIdentifier[i]) && !isdigit(cIdentifier[i]) &&
        cIdentifier[i] != '_')
      return false;

    if (isdigit(cIdentifier[i]) && i == 0)
      return false;
  }

  return true;
}

#endif /* PARSER_H */

