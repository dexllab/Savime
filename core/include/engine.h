#include <utility>

#ifndef ENGINE_H
#define ENGINE_H
/*! \file */
#include "savime.h"
#include "parser.h"
#include "connection_manager.h"
#include "storage_manager.h"

using namespace std;


/**
 * Interface for interacting with the Engine. While the engine is running,
 * it notifies an EngineListener as data is ready to be sent to the client.
 */
class EngineListener {
public:
  /**
  * Notifies the listener that the text response for the client is ready.
  * @param text is a string to be sent for the client.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual int NotifyTextResponse(string text) = 0;

  /**
  * Notifies the listener that a new block of data is read to be sent.
  * @param paramName is a string with the name of param to which the block
  * belongs to.
  * @param file_descriptor is the descriptor for the file containing the data of
  * the block.
  * @param size is the size in bytes of block to be sent to the client.
  * @param isFirst is flag indicating that it is the first block for the param.
  * @param isLast is flag indicating that it is the last block for the param.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual int NotifyNewBlockReady(string paramName, int32_t file_descriptor,
                                  int64_t size, bool isFirst, bool isLast) = 0;

  /**
  * Notifies the listener that the engine has finished its work.
  */
  virtual void NotifyWorkDone() = 0;
};
typedef EngineListener *EngineListenerPtr;

/**
 * The Engine is the module responsible for executing the query plan for a
 * query,
 */
class Engine : public SavimeModule {

public:
  /**
  * Constructor.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  * @param metadataManager is the standard MetadataManager.
  * @param storageManager is the standard StorageManager.
  */
  Engine(ConfigurationManagerPtr configurationManager,
         SystemLoggerPtr systemLogger, MetadataManagerPtr metadataManager,
         const StorageManagerPtr &storageManager)
      : SavimeModule("Engine", std::move(configurationManager), systemLogger) {}

  /**
  * Executes the query in the QueryPlan of the QueryDataManager, and notifies
  * the EngineListener as
  * the data for the query result and text response is ready.
  * @param queryDataManager is a QueryDataManager storing the information of the
  * query to be executed.
  * @param caller is EngineListener to be notified as the result of the
  * execution becomes available.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Run(QueryDataManagerPtr queryDataManager,
                           EngineListenerPtr caller) = 0;
};
typedef std::shared_ptr<Engine> EnginePtr; //!< Engine pointer.

/**
 * Signature for a engine operator function.
*/
typedef int (*OperatorFunction)(SubTARIndex subtarIndex, OperationPtr operation,
                                ConfigurationManagerPtr configurationManager,
                                QueryDataManagerPtr queryDataManager,
                                MetadataManagerPtr metadataManager,
                                StorageManagerPtr storageManager,
                                EnginePtr engine);
/**
 * The EngineOperator is implemented by DDL and DML operators. The EngineOperators implement the logic for each
 * operator supported.
 */
class EngineOperator {

protected:

  OperationPtr _operation;
  ConfigurationManagerPtr _configurationManager;
  QueryDataManagerPtr _queryDataManager;
  MetadataManagerPtr _metadataManager;
  StorageManagerPtr _storageManager;
  EnginePtr _engine;

public:

  EngineOperator(const OperationPtr& operation,
                 const ConfigurationManagerPtr& configurationManager,
                 const QueryDataManagerPtr& queryDataManager,
                 const MetadataManagerPtr& metadataManager,
                 const StorageManagerPtr& storageManager,
                 const EnginePtr& engine) {

    _operation = operation;
    _configurationManager = configurationManager;
    _queryDataManager = queryDataManager;
    _metadataManager = metadataManager;
    _storageManager = storageManager;
    _engine = engine;
  }

  /**
   * Generate the desired subtar in the query plan pipeline.
   * @param subtarIndex is the number of the subtar to be generated.
   * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
   */
  virtual SavimeResult GenerateSubtar(SubTARIndex subtarIndex) = 0;

  /**
  * Run the DDL operator.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Run() = 0;


  /**
  * Returns the operation which defines the EngineOperator.
  * @return the operation which defines the EngineOperator.
  */
  virtual OperationPtr GetOperation() {
    return _operation;
  }

  /**
  * Returns a string representation of the operator.
  * @return a string representing the operator.
  */
  virtual string toString() { return "";}
};
typedef std::shared_ptr<EngineOperator> EngineOperatorPtr;


/**
 * EngineOperatorFactory creates instances of EngineOperators
 */
class EngineOperatorFactory {

public:

    ConfigurationManagerPtr _configurationManager;
    QueryDataManagerPtr _queryDataManager;
    MetadataManagerPtr _metadataManager;
    StorageManagerPtr _storageManager;
    EnginePtr _engine;

    EngineOperatorFactory(ConfigurationManagerPtr configurationManager,
                          QueryDataManagerPtr queryDataManager,
                          MetadataManagerPtr metadataManager,
                          StorageManagerPtr storageManager,
                          EnginePtr engine){
      _configurationManager = configurationManager;
      _queryDataManager = queryDataManager;
      _metadataManager = metadataManager;
      _storageManager = storageManager;
      _engine = engine;
    }

    /**
     * Creates an instance of an EngineOperator.
     * @param operation is the query plan operator for which the engine operator instance must be created.
     * @return a instance of an operator as specified by the operation.
     */
    EngineOperatorPtr Make(OperationPtr operation);

};
typedef std::shared_ptr<EngineOperatorFactory> EngineOperatorFactoryPtr;
#endif
