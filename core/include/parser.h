#ifndef PARSER_H
#define PARSER_H
/*! \file */
#include "savime.h"
#include "metadata.h"
#include "storage_manager.h"
#include "query_data_manager.h"

#define DEFAULT_TEMP_MEMBER "_aux"
#define DEFAULT_SYNTHETIC_DIMENSION "_i"
#define SYNTHETIC_DIMENSION "i"
#define DEFAULT_MASK_ATTRIBUTE "mask"
#define DEFAULT_OFFSET_ATTRIBUTE "offset"
#define NUM_TARS_FOR_JOIN 2
#define LEFT_DATAELEMENT_PREFIX "left_"
#define RIGHT_DATAELEMENT_PREFIX "right_"
#define INPUT_TAR "input_tar"
#define OPERATOR_NAME "operator_name"
#define NEW_MEMBER "new_member"
#define AUX_TAR "aux_tar"
#define OP "op"
#define LITERAL "literal"
#define IDENTIFIER "identifier"
#define COMMAND "command_str"
#define NO_INTERSECTION_JOIN "no_intersection_join"
#define EMPTY_SUBSET "empty_subset"
#define OPERAND(x) "operand"+std::to_string(x)
#define DIM(x) "dim"+std::to_string(x)
#define LB(x) "lb"+std::to_string(x)
#define UP(x) "up"+std::to_string(x)
#define LITERAL_FILLER_MARK string("___LITERAL___")

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
      : SavimeModule("Parser", configurationManager, systemLogger) {}

  /**
  * Sets the standard Metadata Manager for the Parser.
  * @param metadaManager is the standard system MetadataManager.
  */
  virtual void SetMetadaManager(MetadataManagerPtr metadaManager) = 0;

  /**
  * Sets the standard Storage Manager for the Parser.
  * @param metadaManager is the standard system StorageManager.
  */
  virtual void SetStorageManager(StorageManagerPtr storageManager) = 0;

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

    if (identifier == DEFAULT_SYNTHETIC_DIMENSION)
      return false;
  }

  return true;
}

#endif /* PARSER_H */

