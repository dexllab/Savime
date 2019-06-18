#include <utility>

#ifndef QUERY_DATA_MANAGER_H
#define QUERY_DATA_MANAGER_H
/*! \file */
#include <list>
#include "metadata.h"
#include "savime.h"

#define DEFAULT_TEMP_MEMBER "_aux"
#define DEFAULT_SYNTHETIC_DIMENSION "ID"
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

#define _CREATE_TARS "create_tars"
#define _CREATE_TAR "create_tar"
#define _CREATE_TYPE "create_type"
#define _CREATE_DATASET "create_dataset"
#define _LOAD_SUBTAR "load_subtar"
#define _DELETE "delete"
#define _DROP_TARS "drop_tars"
#define _DROP_TAR "drop_tar"
#define _DROP_TYPE "drop_type"
#define _DROP_DATASET "drop_dataset"
#define _SHOW "show"
#define _SAVE "save"
#define _RESTORE "restore"
#define _BATCH "batch"

#define _SCAN "scan"
#define _SELECT "select"
#define _WHERE "where"
#define _SUBSET "subset"
#define _DERIVE "derive"
#define _CROSS_PRODUCT "cross"
#define _EQUIJOIN "equijoin"
#define _DIMJOIN "dimjoin"
#define _SLICE "slice"
#define _UNION "union"
#define _SPLIT "split"
#define _REORIENT "reorient"
#define _ATT2DIM "att2dim"
#define _TRANSLATE "translate"
#define _AGGREGATE "aggregate"
#define _SPLIT "split"

using namespace std;

/*Forward declarations*/
class QueryDataManager;
typedef std::shared_ptr<QueryDataManager> QueryDataManagerPtr;

/**Enum with codes for Savime Internal Operations. */
enum OperationCode {
  TAL_CREATE_TARS,    /*!<DML operation that creates a TARS. */
  TAL_CREATE_TAR,     /*!<DML operation that creates a TAR. */
  TAL_CREATE_TYPE,    /*!<DML operation that creates a Type. */
  TAL_CREATE_DATASET, /*!<DML operation that creates a Dataset. */
  TAL_DROP_TARS,      /*!<DML operation that removes a TARS. */
  TAL_DROP_TAR,       /*!<DML operation that removes a TAR. */
  TAL_DROP_TYPE,      /*!<DML operation that removes a Type. */
  TAL_DROP_DATASET,   /*!<DML operation that removes a Dataset. */
  TAL_LOAD_SUBTAR, /*!<DML operation that creates a Subtar and attaches Datasets
                      to it. */
  TAL_DELETE,      /*!<DML operation that removes all Subtars intersecting a n-dimensional slice.*/
  TAL_SAVE,        /*!<DML operation saves a logical backup. */
  TAL_SHOW,        /*!<DML operation that lists TARs and Subtars. */
  TAL_SCAN,        /*!<DDL operation to allow fully TAR retrieval as is. */
  TAL_SELECT, /*!<DDL operation that projects dimensions and attributes from the
                 TAR. */
  TAL_FILTER, /*!<DDL operation that applies a bitmask aux TAR and removes TAR
                 cells. */
  TAL_SUBSET, /*!<DDL operation that cuts the TAR creating a TAR subset. */
  TAL_LOGICAL, /*!<DDL operation that combines bitmasks aux TARs using lofical
                  operations. */
  TAL_COMPARISON, /*!<DDL operation that creates bitmasks aux TARs using
                     comparison operations. */
  TAL_ARITHMETIC, /*!<DDL operation that creates derived attributes by executing
                     an arithmetic expression. */
  TAL_CROSS,    /*!<DDL operation that creates a TAR as the carterian product of
                   two other TARS. */
  TAL_EQUIJOIN, /*!<DDL operation that creates a cartersin product of two TARs
                   and them filters the result.*/
  TAL_DIMJOIN,  /*!<DDL operation that creates a TAR intersecting cells with_NEW_MEMBER
                   matching indexes for a set of paierd dimensions. */
  TAL_SLICE,

  TAL_SPLIT, /*!<DDL operation that divides each subTAR in a TAR in smaller subTARS. */

  TAL_REORIENT, /*!<DDL operation that resorts sorted subTARs. */

  TAL_ATT2DIM ,  /*!DDL that transform an attribute into a dimension.*/

  TAL_AGGREGATE, /*!<DDL operation that calculate aggregation functions with
                    TARS. */
  TAL_UNION, /*!<DDL operation that merges equivalent TARs. */

  TAL_TRANSLATE, /*!<DDL operation that shifts dimension indexes according to an offset. */

  TAL_USER_DEFINED /*!<DDL operation code for UDFs. */
};

enum TALParameter {
  _INPUT_TAR, _OPERAND, _OPERATOR, _LOWER_BOUND, _UPPER_BOUND,
  _DIMENSION, _IDENTIFIER, _LITERAL, _COMMAND, _AUX_TAR, _NEW_MEMBER,
  _EMPTY_SUBSET, _NO_INTERSECTION_JOIN
};

string PARAM(OperationCode operation, TALParameter parameterType, int32_t index = -1);


/**Enum with codes for Operation Parameters Types. */
enum ParameterType {
  TAR_PARAM,
  IDENTIFIER_PARAM,
  LITERAL_FLOAT_PARAM,
  LITERAL_DOUBLE_PARAM,
  LITERAL_INT_PARAM,
  LITERAL_LONG_PARAM,
  LITERAL_STRING_PARAM,
  LITERAL_BOOLEAN_PARAM
};

/**Enum with codes for Query Types. */
enum QueryType {
  DDL, /*!<DDL operations are the ones that create and/or remove the structures
          in the database. */
  DML  /*!<DML operations are the ones that manipulate/transform the data in the
          database. */
};

/**Class to stores data for an operator parameter. */
class Parameter {

public:
  string name;            /*!<Parameter name. */
  ParameterType type;     /*!<Parameter type. */
  TARPtr tar;             /*!<TAR reference value. */
  DataElementPtr element; /*!<DataElement reference value. */
  string literal_str;     /*!<String reference value. */
  int32_t literal_int;    /*!<32-bit integer reference value. */
  int64_t literal_lng;    /*!<64-bit integer reference value. */
  float literal_flt;      /*!<32-bit floating point reference value. */
  double literal_dbl;     /*!<64-bit floating point reference value. */
  bool literal_bool;      /*!<boolean reference value. */

  /**
  * Creates a parameter containing a TAR reference.
  * @param paramName is a string with the parameter name.
  * @param param is a TAR reference.
  */
  Parameter(string paramName, TARPtr param);

  /**
  * Creates a parameter containing an identifier reference or a literal string.
  * @param paramName is a string with the parameter name.
  * @param param is a string with the literal name or the identifier name.
  * @param isIdentifier is a flag determining whether it is a literal string
  * or identifier parameter.
  */
  Parameter(string paramName, string param, bool isIdentifier);

  /**
  * Creates a parameter containing a string reference.
  * @param paramName is a string with the parameter name.
  * @param param is a string reference.
  */
  Parameter(string paramName, string param);

  /**
  * Creates a parameter containing a 32-bit integer reference.
  * @param paramName is a string with the parameter name.
  * @param param is a 32-bit integer reference.
  */
  Parameter(string paramName, int32_t param);

  /**
  * Creates a parameter containing a 64-bit integer reference.
  * @param paramName is a string with the parameter name.
  * @param param is a 64-bit integer reference.
  */
  Parameter(string paramName, int64_t param);

  /**
  * Creates a parameter containing a 32-bit float reference.
  * @param paramName is a string with the parameter name.
  * @param param is a 32-bit float reference.
  */
  Parameter(string paramName, float param);

  /**
  * Creates a parameter containing a 64-float integer reference.
  * @param paramName is a string with the parameter name.
  * @param param is a 64-bit float reference.
  */
  Parameter(string paramName, double param);

  /**
  * Creates a parameter containing a boolean value reference.
  * @param paramName is a string with the parameter name.
  * @param param is a boolean value reference.
  */
  Parameter(string paramName, bool param);
};
typedef std::shared_ptr<Parameter> ParameterPtr;

/**Class represents an operation in a query plan. */
class Operation {
  string _name;         /*!<Operation name. */
  OperationCode _type;  /*!<Operation type. */
  TARPtr _resultingTAR; /*!<Reference to TAR generated by the operation. */
  list<ParameterPtr> _parameters; /*!<Parameters list. */

  /**
  * Creates a string representation of the operation.
  * @param op is the type of the operation.
  * @return A string containing the textual representation of the operation.
  */
  string OpToString(OperationCode op);

  /**
  * Creates a string representation of a parameter.
  * @param param is the parameter for which the textual representation must be
  * created.
  * @return A string containing the textual representation of the param.
  */
  string ParamToString(ParameterPtr param);

public:
  /**
  * Constructor.
  * @param type is the type of operation.
  */
  explicit Operation(OperationCode type);

  /**
  * Gets operation parameters.
  * @return A list with references for all parameters of the operation.
  */
  list<ParameterPtr>& GetParameters();

  /**
  * Gets a parameter with a given name.
  * @param name is a string containing the name of the parameter to be
  * retrieved..
  * @return The parameter with the given name or NULL if it was not found.
  */
  ParameterPtr GetParametersByName(string name);

  /**
  * Sets the operation type.
  * @param type is a reference to an operation type to be set.
  */
  void SetOperation(OperationCode type);

  /**
  * Get the operation type.
  * @return A OperationCode with the operation type.
  */
  OperationCode GetOperation();

  /**
  * Gets a parameter with a given name.
  * @param name is a string containing the name of the parameter to be
  * retrieved.
  * @return The parameter with the given name or NULL if it was not found.
  */
  void SetResultingTAR(TARPtr tar);

  /**
  * Gets the resulting TAR generated by the operator.
  * @return A TAR reference to the resulting TAR.
  */
  TARPtr GetResultingTAR();

  /**
  * Add a new identifier parameter to the operation.
  * @param paramName is a string containing the name.
  * @param identifier is a string containing the name of the identifier.
  */
  void AddIdentifierParam(string paramName, string identifier);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name.
  * @param paramData is TAR reference to be added as a parameter.
  */
  void AddParam(string paramName, TARPtr paramData);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name .
  * @param paramData is a float value to be added as a parameter.
  */
  void AddParam(string paramName, float paramData);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name .
  * @param paramData is a double value to be added as a parameter.
  */
  void AddParam(string paramName, double paramData);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name .
  * @param paramData is a 32-bit int value to be added as a parameter.
  */
  void AddParam(string paramName, int32_t paramData);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name .
  * @param paramData is a 64-bit int value to be added as a parameter.
  */
  void AddParam(string paramName, int64_t paramData);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name .
  * @param paramData is a boolean value to be added as a parameter.
  */
  void AddParam(string paramName, bool paramData);

  /**
  * Add a new parameter to the operation.
  * @param paramName is a string containing the name .
  * @param paramData is a string value to be added as a parameter.
  */
  void AddParam(string paramName, string paramData);


  /**
  * Add a new parameter to the operation.
  * @param parameter is the parameter to be added;
  */
  void AddParam(ParameterPtr parameter);

  /**
  * Sets the operations name.
  * @param name is a string with the name to be set.
  */
  void SetName(string name);

  /**
  * Gets the operations name.
  * @return A string containing the operation name.
  */
  string GetName();

  /**
  * Creates a textual representation of the operation.
  * @return A string containing the textual representation of the operation.
  */
  string toString();

  /**
  * Destructor.
  */
  //~Operation();
};
typedef std::shared_ptr<Operation> OperationPtr;

/**A QueryPlan encompasses the operations that need to be executed in order to
 * answer the user query */
class QueryPlan {
  QueryType _type; /*!<Query type: DML or DDL. */
  list<OperationPtr>
      _operations; /*!<List of operation references in the QueryPlan. */

public:
  /**
  * Adds a new operation in the query plan.
  * @param operation reference to be added in the query plan.
  */
  void AddOperation(OperationPtr operation);

  /**
  * Adds a new operation in the query plan.
  * @param operation reference to be added in the query plan.
  * @param idCounter is a integer counter reference to assign sequential TAR
  * names.
  */
  void AddOperation(OperationPtr operation, int &idCounter);

  /**
  * Gets a list of operations in the query plan.
  * @return A list with the operations in the query plan.
  */
  list<OperationPtr> &GetOperations();

  /**
  * Sets the type of query in query plan.
  * @param type is a QueryType indicating the query in the query plan.
  */
  void SetType(QueryType type);

  /**
  * Sets the type of query in query plan.
  * @return type is a QueryType indicating the query in the query plan.
  */
  QueryType GetType();

  /**
  * Sorts the operations by the resulting TAR name.
  */
  void SortOperations();

  /**
   *Creates a string representation of the query plan.
   * @return a string representation of the query plan.
   */
   string toString();

  /**
   *Destructor
   */
  ~QueryPlan();
};
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

/**A QueryDataManager is the module responsible for keeping all query related
 * data , such
 * as the query text string, query data blocks and the query plan.*/
class QueryDataManager : public SavimeModule {
public:
  /**
  * Constructor.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  */
  QueryDataManager(ConfigurationManagerPtr configurationManager,
                   SystemLoggerPtr systemLogger)
      : SavimeModule("Query Data Manager", std::move(configurationManager), systemLogger) {
  }

  /**
  * Gets a common instance of the QueryDataManager.
  * @return A reference to a common QueryDataManager object.
  */
  virtual QueryDataManagerPtr GetInstance() = 0;

  /**
  * Sets the query id.
  * @param queryId is a 32-bit integer containing the query id.
  */
  virtual void SetQueryId(int32_t queryId) = 0;

  /**
  * Gets the query id.
  * @return A 32-bit integer containing the query id.
  */
  virtual int32_t GetQueryId() = 0;

  /**
  * Concatenates a text string to the current query string text.
  * @param queryPart is a string containing a part of a query string to be
  * concatenated.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult AddQueryTextPart(string queryPart) = 0;

  /**
  * Returns the current saved query text string.
  * @return A string containing the current query text string.
  */
  virtual string GetQueryText() = 0;

  /**
  * Sets the error text response string.
  * @param error is a string containing the error message to be sent to the
  * client.
  */
  virtual void SetErrorResponseText(string error) = 0;

  /**
  * Gets the error text response string.
  * @return A string cotaining the current erro message to be sent to the
  * client.
  */
  virtual string GetErrorResponse() = 0;

  /**
  * Sets the text response string.
  * @return A string cotaining the current message to be sent to the client.
  */
  virtual void SetQueryResponseText(string text) = 0;

  /**
  * Gets the text response string.
  * @return A string cotaining the current message to be sent to the client.
  */
  virtual string GetQueryResponseText() = 0;

  /**
  * Gets the list containing the query params. A query param is a name block of
  * data used to load data into SAVIME.
  * @return A list of strings with params name.
  */
  virtual list<string> GetParamsList() = 0;

  /**
  * Register the usage of memory for a transfer buffer, it succeds if
  * the size of transfer buffer is less than the specified in the configuration.
  * @param size the size of the next block of memory to be transferred.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult RegisterTransferBuffer(int64_t size) = 0;

  /**
  * Gets the file descriptor associated to a query data param. If the file
  * does not exists for a given param, it is created.
  * @return A int32_t containing the file descriptor of the file for that param.
  */
  virtual int32_t GetParamFile(string paramName) = 0;

  /**
  * Deletes a param file.
  * @param paramName is the name of the parameter whose file is to be deleted.
  */
  virtual void RemoveParamFile(string paramName) = 0;

  /**
  * Gets the absolute file path to the given param file.
  * @param paramName is the name of the parameter whose file path is to be
  * retrieved.
  * @return A string containing the file path for the param.
  */
  virtual string GetParamFilePath(string paramName) = 0;

  /**
  * Sets the QueryPlan for the QueryDataManager.
  * @param queryPlan is the query plan.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult SetQueryPlan(QueryPlanPtr queryPlan) = 0;

  /**
  * Gets the QueryPlan for the QueryDataManager.
  * @return A reference to the QueryPlan held in the QueryDataManager.
  */
  virtual QueryPlanPtr GetQueryPlan() = 0;

  /**
  * Releases all the resources used by the query plan except the created param
  * files.
  * Empties the parameters list, the query, message and error strings.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Release() = 0;
};

#endif /* QUERY_DATA_MANAGER_H */