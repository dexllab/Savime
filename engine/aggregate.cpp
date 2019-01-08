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
*    HERMANO L. S. LUSTOSA				JANUARY 2019
*/
#include "include/dml_operators.h"
#include "include/aggregate_buffer.h"
#include "include/aggregate_config.h"
#include "include/aggregate_engine.h"

Aggregate::Aggregate(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                     QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                     StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
  SET_INT_CONFIG_VAL(_numThreads, MAX_THREADS);
  SET_INT_CONFIG_VAL(_workPerThread, WORK_PER_THREAD);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
}

SavimeResult Aggregate::GenerateSubtar(SubTARIndex subtarIndex) {

  OMP_EXCEPTION_ENV()

  SubtarPtr newSubtar = std::make_shared<Subtar>();
  newSubtar->SetTAR(_outputTAR);
  unordered_map<string, AggregateBufferPtr> globalAggBuffer, globalAuxBuffer;
  vector<AggregateConfigurationPtr> aggConfigs(static_cast<unsigned long>(_numSubtars));

  AggregateConfigurationPtr aggConfig =
    createConfiguration(_inputTAR, _operation);
  setAggregateMode(aggConfig, _configurationManager);
  setSavimeDatasets(aggConfig, _storageManager, _numThreads, _workPerThread);
  setGlobalAggregatorBuffers(aggConfig, globalAggBuffer, globalAuxBuffer);

  for (int32_t i = 0; i < _numSubtars; i++) {
    aggConfigs[i] = aggConfig->Clone();
  }
  auto outputLen = aggConfig->GetTotalLength();

  vector<SubtarPtr> subtar(static_cast<unsigned long>(_numSubtars));
  vector<int64_t> currentSubtars(static_cast<unsigned long>(_numSubtars));
  unordered_map<string, vector<AggregateBufferPtr>> aggregateBuffers(_numSubtars);
  unordered_map<string, vector<AggregateBufferPtr>> auxAggregateBuffers(_numSubtars);

  for (const auto &func : aggConfig->functions) {
    aggregateBuffers[func->attribName] = vector<AggregateBufferPtr>(static_cast<unsigned long>(_numSubtars));
    auxAggregateBuffers[func->attribName] =
      vector<AggregateBufferPtr>(static_cast<unsigned long>(_numSubtars));
  }

  if (subtarIndex != FIRST_SUBTAR) {
    return SAVIME_SUCCESS;
  }

  SubTARIndex currentSubtar = subtarIndex;
  bool hasSubtars = true;

  while (true) {

    for (int32_t i = 0; i < _numSubtars; i++) {
      subtar[i] = nullptr;
    }

    for (int32_t i = 0; i < _numSubtars; i++) {
      currentSubtars[i] = currentSubtar;
      subtar[i] = _generator->GetSubtar(currentSubtar);
      if(subtar[i] == nullptr)
        break;
      currentSubtar++;
    }

    SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
    for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

      TRY()
        if (subtar[i] == nullptr) {
          hasSubtars = false;
          for (const auto &func : aggConfigs[i]->functions) {
            aggregateBuffers[func->attribName][i] = nullptr;
            auxAggregateBuffers[func->attribName][i] = nullptr;
          }
          break;
        }

        AggregateBufferPtr localAggBuffer, localAuxBuffer;
        for (const auto &func : aggConfigs[i]->functions) {
          setLocalAggregatorBuffers(aggConfigs[i], func, localAggBuffer,
                                    localAuxBuffer, outputLen);
          aggregateBuffers[func->attribName][i] = localAggBuffer;
          auxAggregateBuffers[func->attribName][i] = localAuxBuffer;
        }

        int64_t subtarLen = subtar[i]->GetFilledLength();
        createLogicalIndexesBuffer(aggConfigs[i], _storageManager, subtar[i]);
        setInputBuffers(aggConfigs[i], _storageManager, subtar[i]);

        for (const auto &func : aggConfigs[i]->functions) {
          auto type = _inputTAR->GetDataElement(func->paramName)->GetDataType();
          AbstractAggregateEnginePtr engine = buildAggregateEngine(
            aggConfigs[i], func, type, subtarLen, _numThreads, _workPerThread);

          engine->Run(localAggBuffer, localAuxBuffer, outputLen);
        }

        _generator->TestAndDisposeSubtar(currentSubtars[i]);
      CATCH()
    }
    RETHROW()

    /*Reducing aggregations. */
    for (const auto &func : aggConfigs[0]->functions) {

      auto type = _inputTAR->GetDataElement(func->paramName)->GetDataType();
      AbstractAggregateEnginePtr engine = buildAggregateEngine(
        aggConfigs[0], func, type, 0, _numThreads, _workPerThread);

      engine->Reduce(globalAggBuffer[func->attribName],
                     globalAuxBuffer[func->attribName],
                     aggregateBuffers[func->attribName],
                     auxAggregateBuffers[func->attribName], outputLen);
    }

    if (!hasSubtars)
      break;
  }

  for (const auto &func : aggConfig->functions) {

    auto type = _inputTAR->GetDataElement(func->paramName)->GetDataType();
    AbstractAggregateEnginePtr engine = buildAggregateEngine(
      aggConfigs[0], func, type, 0, _numThreads, _workPerThread);

    engine->Finalize(globalAggBuffer[func->attribName],
                     globalAuxBuffer[func->attribName], outputLen);
  }

  if (aggConfig->mode == BUFFERED) {

    for (auto &dim : _outputTAR->GetDimensions()) {
      DimSpecPtr newDimSpec = std::make_shared<DimensionSpecification>(
        UNSAVED_ID, dim, 0, dim->CurrentUpperBound(),
        aggConfig->stride[aggConfig->name2index[dim->GetName()]],
        aggConfig->adj[aggConfig->name2index[dim->GetName()]]);

      newSubtar->AddDimensionsSpecification(newDimSpec);
    }

    for (auto entry : aggConfig->datasets) {
      newSubtar->AddDataSet(entry.first, entry.second);
    }

    newSubtar = filterAggregatedSubtar(
      newSubtar, globalAggBuffer.begin()->second->Bitmask(), _numThreads,
      _workPerThread, _storageManager);
  } else {

    auto aggBuffer = globalAggBuffer.begin()->second;
    outputLen = aggBuffer->getIndexesMap()->size(); int64_t i = 0;
    unordered_map<int32_t, DatasetPtr> dimensionDatasets;
    unordered_map<int32_t, DatasetHandlerPtr> dimensionHandlers;
    unordered_map<int32_t, RealIndex *> dimensionBuffers;
    unordered_map<string, DatasetPtr> attributeDatasets;
    unordered_map<string, DatasetHandlerPtr> attributeHandlers;
    unordered_map<string, double *> attributeBuffers;

    /*Creating datasets for new total dimensions*/
    for (const DimensionPtr &dim : _outputTAR->GetDimensions()) {
      auto ds = _storageManager->Create(REAL_INDEX, outputLen);
      dimensionDatasets[aggConfig->name2index[dim->GetName()]] = ds;
      auto handler = _storageManager->GetHandler(ds);
      dimensionHandlers[aggConfig->name2index[dim->GetName()]] = handler;
      auto *buffer = (RealIndex *)handler->GetBuffer();
      dimensionBuffers[aggConfig->name2index[dim->GetName()]] = buffer;
    }

    /*Copying data from hashmap to datasets*/
    for (auto entry : (*aggBuffer->getIndexesMap())) {
      for (int32_t d = 0; d < entry.second.size(); d++) {
        dimensionBuffers[d][i] = entry.second[d];
      }
      i++;
    }

    /*Setting up dimensions. */
    for (DimensionPtr dim : _outputTAR->GetDimensions()) {

      DimSpecPtr newDimSpec = make_shared<DimensionSpecification>(
        UNSAVED_ID, dim,
        dimensionDatasets[aggConfig->name2index[dim->GetName()]], 0,
        dim->CurrentUpperBound());

      if (dim->GetDimensionType() == IMPLICIT) {
        DatasetPtr logicalIndexesDataset;
        _storageManager->Real2Logical(
          dim, newDimSpec,
          dimensionDatasets[aggConfig->name2index[dim->GetName()]],
          logicalIndexesDataset);
        dimensionDatasets[aggConfig->name2index[dim->GetName()]] =
          logicalIndexesDataset;
      }
      newDimSpec->AlterDataset(
        dimensionDatasets[aggConfig->name2index[dim->GetName()]]);

      dimensionHandlers[aggConfig->name2index[dim->GetName()]]->Close();
      newSubtar->AddDimensionsSpecification(newDimSpec);
    }

    /*Creating datasets for attributes*/
    for (const auto &func : aggConfig->functions) {
      auto ds = _storageManager->Create(DOUBLE, outputLen);
      attributeDatasets[func->attribName] = ds;
      auto handler = _storageManager->GetHandler(ds);
      attributeHandlers[func->attribName] = handler;
      auto *buffer = (double *)handler->GetBuffer();
      attributeBuffers[func->attribName] = buffer;
    }

    i = 0;
    for (auto entry : aggBuffer->getMap()) {
      for (const auto &func : aggConfig->functions) {
        attributeBuffers[func->attribName][i] = entry.second;
      }
      i++;
    }

    for (const auto &func : aggConfig->functions) {
      newSubtar->AddDataSet(func->attribName,
                            attributeDatasets[func->attribName]);
      attributeHandlers[func->attribName]->Close();
    }
  }

  _outputGenerator->AddSubtar(0, newSubtar);

  for (const auto &h : aggConfig->handlersToClose)
    h->Close();

  return SAVIME_SUCCESS;
}