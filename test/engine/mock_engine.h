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
#ifndef SAVIME_MOCK_ENGINE_H
#define SAVIME_MOCK_ENGINE_H

#include "../core/include/config_manager.h"
#include "../core/include/system_logger.h"
#include "../core/include/session.h"
#include "../core/include/engine.h"
#include "../core/include/parser.h"
#include "../core/include/optimizer.h"
#include "../core/include/metadata.h"
#include "../../engine/include/default_engine.h"

class MockDefaultEngine : public DefaultEngine {

  void SendResultingTAR(EngineListener *caller, TARPtr tar);

public:
  MockDefaultEngine(ConfigurationManagerPtr configurationManager,
  SystemLoggerPtr systemLogger,
    MetadataManagerPtr metadataManager,
  StorageManagerPtr storageManager)
  : DefaultEngine(configurationManager, systemLogger, metadataManager,
    storageManager) {
  }
};


class TestCaseMatcher : public EngineListener {

  string _textResponse;
  std::map<std::string, vector<vector<uint8_t>>> _binData;
  std::map<std::string, vector<string>> _binStrData;
  StorageManagerPtr _storageManager;

public:

  int NotifyTextResponse(string text){
    _textResponse = text;
    return SAVIME_SUCCESS;
  }

  int NotifyNewBlockReady(string paramName, int32_t file_descriptor,
                          int64_t size, bool isFirst, bool isLast) {
    return SAVIME_SUCCESS;
  }

  void NotifyWorkDone() {
    return;
  }

  TestCaseMatcher(StorageManagerPtr storageManager) {
    _storageManager = storageManager;
  }

  void StartNewQuery();
  void AddDataset(string blockName, DatasetPtr dataset);
  string GetBinaryString();
  string GetTextResponse() { return _textResponse; }
  std::map<std::string, vector<vector<uint8_t>>>  GetBinaryData() { return _binData; }
};

bool Compare(std::map<std::string, vector<vector<uint8_t>>> binData1,
             std::map<std::string, vector<vector<uint8_t>>> binData2,
             string& dataElement, int64_t& i_error , int64_t& j_error);

#endif //SAVIME_MOCK_ENGINE_H
