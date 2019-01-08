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
#include "mock_builder.h"
#include "mock_logger.h"
#include "storage/mock_default_storage_manager.h"
#include "engine/mock_engine.h"
#include "parser/mock_parser.h"
#include "optimizer/mock_optimizer.h"
#include "test/metadata/metadata_test.h"
#include "query/mock_query_data.h"
#include "../configuration/default_config_manager.h"
#include "../core/include/system_logger.h"
#include "catch2/catch.hpp"

ConfigurationManagerPtr MockModulesBuilder::BuildConfigurationManager() {
  if (_configurationManager == nullptr) {
    _configurationManager =
      ConfigurationManagerPtr(new DefaultConfigurationManager());
  }

  return _configurationManager;
}

SystemLoggerPtr MockModulesBuilder::BuildSystemLogger() {
  if (_systemLogger == nullptr) {
    _systemLogger =
      SystemLoggerPtr(new MockLogger(BuildConfigurationManager()));
      //SystemLoggerPtr(new DefaultSystemLogger(BuildConfigurationManager()));
  }

  return _systemLogger;
}

EnginePtr MockModulesBuilder::BuildEngine() {
  if (_engine == nullptr) {
    _engine = EnginePtr(
      new MockDefaultEngine(BuildConfigurationManager(), BuildSystemLogger(),
                        BuildMetadaManager(), BuildStorageManager()));

    ((DefaultEngine *) _engine.get())->SetThisPtr(_engine);
  }

  return _engine;
}

ParserPtr MockModulesBuilder::BuildParser() {
  if (_parser == nullptr) {
    _parser = ParserPtr(
      new MockDefaultParser(BuildConfigurationManager(), BuildSystemLogger()));
    _parser->SetMetadataManager(BuildMetadaManager());
    _parser->SetStorageManager(BuildStorageManager());
  }

  return _parser;
}

OptimizerPtr MockModulesBuilder::BuildOptimizer() {
  if (_optmizier == nullptr) {
    _optmizier = OptimizerPtr(
      new MockDefaultOptimizer(BuildConfigurationManager(), BuildSystemLogger(), BuildMetadaManager()));
    _optmizier->SetParser(BuildParser());
  }

  return _optmizier;
}

MetadataManagerPtr MockModulesBuilder::BuildMetadaManager() {
  if (_metadataManager == nullptr) {
    _metadataManager = MetadataManagerPtr(new MockDefaultMetadataManager(
      BuildConfigurationManager(), BuildSystemLogger()));
  }

  return _metadataManager;
}

ConnectionManagerPtr MockModulesBuilder::BuildConnectionManager() {
  return nullptr;
}

StorageManagerPtr MockModulesBuilder::BuildStorageManager() {
  if (_storageManager == nullptr) {
    _storageManager = StorageManagerPtr(new MockDefaultStorageManager(
      BuildConfigurationManager(), BuildSystemLogger()));
    ((MockDefaultStorageManager *) (_storageManager.get()))
      ->SetThisPtr(
        std::dynamic_pointer_cast<MockDefaultStorageManager>(_storageManager));
  }

  return _storageManager;
}

QueryDataManagerPtr MockModulesBuilder::BuildQueryDataManager() {
  if (_queryDataManager == nullptr) {
    _queryDataManager = QueryDataManagerPtr(new MockDefaultQueryDataManager(
      BuildConfigurationManager(), BuildSystemLogger()));
  }

  return _queryDataManager;
}

SessionManagerPtr MockModulesBuilder::BuildSessionManager() {
  return nullptr;
}

void MockModulesBuilder::RunBootQueries(list<string> queries) {

  int queryId = 0;
  auto configurationManager = BuildConfigurationManager();
  auto systemLogger = BuildSystemLogger();
  auto parser = BuildParser();
  auto engine = BuildEngine();

  for(const auto &query : queries){
    auto queryDataManager = QueryDataManagerPtr(
        new DefaultQueryDataManager(configurationManager, systemLogger));
    queryDataManager->SetQueryId(queryId++);
    queryDataManager->AddQueryTextPart(query);

    if (parser->Parse(queryDataManager) != SAVIME_SUCCESS) {
      continue;
    }

    if (engine->Run(queryDataManager, this) != SAVIME_SUCCESS) {
      systemLogger->LogEvent("Builder",
                             "Error during boot query execution: " + query +
                                 " " + queryDataManager->GetErrorResponse());
    }
  }
}