#ifndef MODULESBUILDER_H
#define MODULESBUILDER_H

#include <memory>
#include "metadata.h"
#include "connection_manager.h"
#include "engine.h"
#include "config_manager.h"
#include "system_logger.h"
#include "session.h"

class ModulesBuilder : EngineListener {

  void Daemonize();
  
public:
  ConfigurationManagerPtr _configurationManager = 0;
  SystemLoggerPtr _systemLogger = 0;
  SessionManagerPtr _sessionManager = 0;
  EnginePtr _engine = 0;
  ParserPtr _parser = 0;
  OptimizerPtr _optmizier = 0;
  MetadataManagerPtr _metadataManager = 0;
  ConnectionManagerPtr _connectionManager = 0;
  StorageManagerPtr _storageManager = 0;
  QueryDataManagerPtr _queryDataManager = 0;

  ModulesBuilder(int args, char **argc);
  ConfigurationManagerPtr BuildConfigurationManager();
  SystemLoggerPtr BuildSystemLogger();
  SessionManagerPtr BuildSessionManager();
  EnginePtr BuildEngine();
  ParserPtr BuildParser();
  OptimizerPtr BuildOptimizer();
  MetadataManagerPtr BuildMetadaManager();
  ConnectionManagerPtr BuildConnectionManager();
  StorageManagerPtr BuildStorageManager();
  QueryDataManagerPtr BuildQueryDataManager();
  void RunBootQueryFile(string queryFile);

  int NotifyTextResponse(string text) { return SAVIME_SUCCESS; }
  int NotifyNewBlockReady(string paramName, int32_t file_descriptor,
                          int64_t size, bool isFirst, bool isLast) {
    return SAVIME_SUCCESS;
  }
  void NotifyWorkDone() {}
};


#endif /* MODULESBUILDER_H */

