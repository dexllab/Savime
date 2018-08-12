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
         StorageManagerPtr storageManager)
      : SavimeModule("Engine", configurationManager, systemLogger) {}

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
  virtual SavimeResult run(QueryDataManagerPtr queryDataManager,
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


#endif
