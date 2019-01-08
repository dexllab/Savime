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
  ConfigurationManagerPtr _configurationManager = nullptr;
  SystemLoggerPtr _systemLogger = nullptr;
  SessionManagerPtr _sessionManager = nullptr;
  EnginePtr _engine = nullptr;
  ParserPtr _parser = nullptr;
  OptimizerPtr _optimizer = nullptr;
  MetadataManagerPtr _metadataManager = nullptr;
  ConnectionManagerPtr _connectionManager = nullptr;
  StorageManagerPtr _storageManager = nullptr;
  QueryDataManagerPtr _queryDataManager = nullptr;

  ModulesBuilder(int args, char **argc);
  ConfigurationManagerPtr BuildConfigurationManager();
  SystemLoggerPtr BuildSystemLogger();
  SessionManagerPtr BuildSessionManager();
  EnginePtr BuildEngine();
  ParserPtr BuildParser();
  OptimizerPtr BuildOptimizer();
  MetadataManagerPtr BuildMetadataManager();
  ConnectionManagerPtr BuildConnectionManager();
  StorageManagerPtr BuildStorageManager();
  QueryDataManagerPtr BuildQueryDataManager();
  void RunBootQueryFile(string queryFile);

  int NotifyTextResponse(string text) override { return SAVIME_SUCCESS; }
  int NotifyNewBlockReady(string paramName, int32_t file_descriptor,
                          int64_t size, bool isFirst, bool isLast) override {
    return SAVIME_SUCCESS;
  }
  void NotifyWorkDone() override {}
};


#endif /* MODULESBUILDER_H */

