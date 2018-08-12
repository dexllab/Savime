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
#ifndef DEFAULT_ENGINE_H
#define DEFAULT_ENGINE_H

#include <mutex>
#include <vector>
#include <thread>
#include <unordered_map>
#include <condition_variable>
#include "../core/include/engine.h"
#include "../core/include/parser.h"
#include "../core/include/storage_manager.h"


#define UNDEFINED -1
using namespace std;

struct SubtarControler {
  int32_t accessCount;
  SubtarPtr subtar;
};
typedef std::shared_ptr<SubtarControler> SubtarControlerPtr;

#define MAX_MAPS 5
#define DEFAULT_MAP 0

class TARGenerator {
  mutex _mutex;
  int32_t _maxAccesses = 1;
  unordered_map<SubTARIndex, SubTARIndex> _subtarIndexMap[MAX_MAPS];
  unordered_map<SubTARIndex, SubtarControlerPtr> _subtarMap;

  TARPtr _tar;
  vector<SubtarPtr> _subtarsVector;

  OperationPtr _operation;
  ConfigurationManagerPtr _configurationManager;
  QueryDataManagerPtr _queryDataManager;
  MetadataManagerPtr _metadataManager;
  StorageManagerPtr _storageManager;
  EnginePtr _engine;
  OperatorFunction _producer;

public:
  TARGenerator(TARPtr tar) {
    _tar = tar;
    for (auto& subtar : tar->GetSubtars())
      _subtarsVector.push_back(subtar);
  }

  TARGenerator(OperatorFunction producer, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) {
    _producer = producer;
    _operation = operation;
    _configurationManager = configurationManager;
    _queryDataManager = queryDataManager;
    _metadataManager = metadataManager;
    _storageManager = storageManager;
    _engine = engine;
    _tar = nullptr;
  }

  SubtarPtr GetSubtar(SubTARIndex subtarIndex);
  void AddSubtar(SubTARIndex subtarIndex, SubtarPtr subtar);
  void TestAndDisposeSubtar(SubTARIndex subtarIndex);

  // unordered_map<int32_t, int32_t>& GetSubtarsIndexMap();
  int32_t GetSubtarsIndexMap(SubTARIndex index);
  void SetSubtarsIndexMap(SubTARIndex index, SubTARIndex value);

  int32_t GetSubtarsIndexMap(SubTARIndex mapIndex, SubTARIndex index);
  void SetSubtarsIndexMap(int32_t mapIndex, SubTARIndex index,
    SubTARIndex value);

  // unordered_map<int32_t, SubtarControlerPtr> & GetSubtarsMap();
  SubtarControlerPtr GetSubtarsMap(SubTARIndex index);
  void SetSubtarsMap(SubTARIndex index, SubtarControlerPtr value);

  int32_t getMaxAccesses();
  void SetMaxAccesses(int32_t maxAccesses);
};
typedef std::shared_ptr<TARGenerator> TARGeneratorPtr;

struct BlockToDispatch {
  EngineListener *caller;
  DatasetPtr dataset;
  string param_name;
  string file_location;
  int64_t size;
  bool is_first;
  bool is_last;
};
typedef std::shared_ptr<BlockToDispatch> BlockToDispatchPtr;

class DefaultEngine : public Engine {

  mutex _mutex;
  mutex _dispatchMutex;
  condition_variable _conditionVar;
  SavimeResult _sendResult;
  bool _runningDispatcher = false;
  shared_ptr<thread> _thread;
  list<BlockToDispatchPtr> _blocksToDispatch;
  list<TARPtr> tempTARs;
  MetadataManagerPtr _metadataManager;
  StorageManagerPtr _storageManager;
  EnginePtr _this;
  unordered_map<std::string, TARGeneratorPtr> _generators;

  void CleanTempTARs();
  void SendResultingTAR(EngineListener *caller, TARPtr tar);
  SavimeResult WaitSendBlocksCompletion();
  void AddBlockToDispatchList(EngineListener *caller, const DatasetPtr& dataset,
                              const string& paramName, const string& fileLocation,
                              int64_t size, bool isFirst, bool isLast);
  void WakeDispatcher();
  void DispatchBlocks();

public:
  DefaultEngine(ConfigurationManagerPtr configurationManager,
                SystemLoggerPtr systemLogger,
                MetadataManagerPtr metadataManager,
                StorageManagerPtr storageManager)
      : Engine(configurationManager, systemLogger, metadataManager,
               storageManager) {
    _metadataManager = metadataManager;
    _storageManager = storageManager;
  }

  void SetThisPtr(EnginePtr thisPtr) { _this = thisPtr; }
  unordered_map<std::string, TARGeneratorPtr> &GetGenerators();
  void SetMetadaManager(MetadataManagerPtr metadaManager);
  SavimeResult run(QueryDataManagerPtr queryDataManager,
                   EngineListenerPtr caller);
};
typedef std::shared_ptr<DefaultEngine> DefaultEnginePtr;
#define DEFAULT_ENGINE(X) (std::dynamic_pointer_cast<DefaultEngine>(X));

#endif /* DEFAULT_ENGINE_H */
