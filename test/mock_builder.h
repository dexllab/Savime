/*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/

#ifndef SAVIME_MOCK_BUILDER_H
#define SAVIME_MOCK_BUILDER_H

#include "../core/include/builder.h"
#include "../core/include/config_manager.h"
#include "../core/include/system_logger.h"
#include "../core/include/session.h"
#include "../core/include/engine.h"
#include "../core/include/parser.h"
#include "../core/include/optimizer.h"
#include "../core/include/metadata.h"

class MockModulesBuilder : EngineListener {

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
  void RunBootQueries(list<string> queries);


  int NotifyTextResponse(string text) { return SAVIME_SUCCESS; }
  int NotifyNewBlockReady(string paramName, int32_t file_descriptor,
                          int64_t size, bool isFirst, bool isLast) {
    return SAVIME_SUCCESS;
  }
  void NotifyWorkDone() {}
};


#endif //SAVIME_MOCK_BUILDER_H
