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
#ifndef AGGREGATE_CONFIG_H
#define AGGREGATE_CONFIG_H

#include "../../core/include/util.h"
#include "../../core/include/metadata.h"
#include "../../core/include/storage_manager.h"
#include "dml_operators.h"
#include "filter.h"
#include "aggregate_buffer.h"

#define SINGLE_DIMENSION "i"
#define AVG_FUNCTION "avg"
#define COUNT_FUNCTION "count"
#define MAX_FUNCTION "max"
#define MIN_FUNCTION "min"
#define SUM_FUNCTION "sum"

using namespace std;

struct AggregateFunction {
  string function;
  string paramName;
  string attribName;

  AggregateFunction(string f, string p, string a) {
    function = f;
    paramName = p;
    attribName = a;
  }

  double GetStartValue() {
    if (!function.compare(AVG_FUNCTION)) {
      return 0.0;
    } else if (!function.compare(SUM_FUNCTION)) {
      return 0.0;
    } else if (!function.compare(MIN_FUNCTION)) {
      return std::numeric_limits<double>::max();
    } else if (!function.compare(MAX_FUNCTION)) {
      return std::numeric_limits<double>::min();
    } else if (!function.compare(COUNT_FUNCTION)) {
      return 0.0;
    }
  }

  bool RequiresAuxDataset() { return !function.compare(AVG_FUNCTION); }
};
typedef shared_ptr<AggregateFunction> AggregateFunctionPtr;

typedef enum { HASHED, BUFFERED } AggregationMode;

struct AggregateConfiguration {
public:
  vector<int64_t> multipliers;
  vector<int64_t> adj;
  vector<int64_t> skew;
  vector<DimensionPtr> dimensions;
  vector<AggregateFunctionPtr> functions;
  unordered_map<string, DatasetPtr> datasets;
  unordered_map<string, DatasetHandlerPtr> handlers;
  unordered_map<string, DatasetHandlerPtr> auxHandlers;
  unordered_map<string, DatasetHandlerPtr> inputHandlers;
  vector<DatasetHandlerPtr> positionHandlers;
  list<DatasetHandlerPtr> handlersToClose;
  vector<SubTARPosition *> positionHandlersBuffers;
  unordered_map<string, int32_t> name2index;
  AggregationMode mode;

  void Configure() {
    int32_t numDims = dimensions.size();

    if (numDims == 0) {
      // dimensions.push_back(SINGLE_DIMENSION);
      multipliers.push_back(0.0);
      skew.push_back(1.0);
      adj.push_back(1.0);
    } else {
      multipliers.resize(numDims);
      skew.resize(numDims);
      adj.resize(numDims);
    }

    for (int32_t i = 0; i < numDims; i++) {
      auto dimName = dimensions[i]->GetName();

      multipliers[name2index[dimName]] = 1;
      skew[name2index[dimName]] = dimensions[i]->GetCurrentLength();
      adj[name2index[dimName]] = 1;

      for (int32_t j = i + 1; j < numDims; j++) {
        int64_t preamble = multipliers[name2index[dimName]];
        multipliers[name2index[dimName]] =
            preamble * dimensions[j]->GetCurrentLength();
        skew[name2index[dimName]] *= dimensions[j]->GetCurrentLength();
        adj[name2index[dimName]] *= dimensions[j]->GetCurrentLength();
      }
    }
  }

  inline uint64_t GetTotalLength() {
    uint64_t totalLen = 1;

    for (DimensionPtr dim : dimensions) {
      uint64_t beforeMultiplication = totalLen;
      // totalLen *= dim->GetLength();
      totalLen *= dim->GetCurrentLength();
      /*Checking overflow*/
      if (totalLen < beforeMultiplication) {
        throw runtime_error("Impossible to run aggregate operator. Input TAR "
                            "grouping dimensions are too long.");
      }
    }

    return totalLen;
  }

  inline SubTARPosition GetLinearPosition(AggregateBufferPtr buffer,
                                          int64_t pos) {

    if (positionHandlersBuffers.size() == 0 && mode == BUFFERED)
      return 0.0;

    vector<SubTARPosition> indexes(positionHandlersBuffers.size());

    for (int32_t i = 0; i < positionHandlersBuffers.size(); i++)
      indexes[i] = positionHandlersBuffers[i][pos];

    return buffer->GetHash(indexes);
  }

  shared_ptr<AggregateConfiguration> Clone() {
    shared_ptr<AggregateConfiguration> cloned =
        make_shared<AggregateConfiguration>();
    cloned->multipliers = multipliers;
    cloned->adj = adj;
    cloned->skew = skew;
    cloned->dimensions = dimensions;
    cloned->functions = functions;
    cloned->datasets = datasets;
    cloned->handlers = handlers;
    cloned->auxHandlers = auxHandlers;
    cloned->inputHandlers = inputHandlers;
    cloned->positionHandlers = positionHandlers;
    cloned->handlersToClose = handlersToClose;
    cloned->positionHandlersBuffers = positionHandlersBuffers;
    cloned->name2index = name2index;
    cloned->mode = mode;
    return cloned;
  }
};
typedef shared_ptr<AggregateConfiguration> AggregateConfigurationPtr;

inline AggregateConfigurationPtr createConfiguration(TARPtr inputTAR,
                                                     OperationPtr operation) {

  AggregateConfigurationPtr aggConfig =
      AggregateConfigurationPtr(new AggregateConfiguration());

  /*Get length of output*/
  int32_t dimCount = 0;
  while (true) {
    auto param = operation->GetParametersByName(DIM(dimCount));
    if (param == nullptr)
      break;
    auto dataElement = inputTAR->GetDataElement(param->literal_str);
    aggConfig->dimensions.push_back(dataElement->GetDimension());
    aggConfig->name2index[dataElement->GetName()] = dimCount;
    dimCount++;
  }
  
  /*Mapping for full aggregations.*/
  aggConfig->name2index[SINGLE_DIMENSION] = 0;

  /*Get functions*/
  int32_t opCount = 0;
  while (true) {
    auto param1 = operation->GetParametersByName(OPERAND(opCount++));
    if (param1 == nullptr)
      break;
    auto param2 = operation->GetParametersByName(OPERAND(opCount++));
    auto param3 = operation->GetParametersByName(OPERAND(opCount++));

    AggregateFunctionPtr func = AggregateFunctionPtr(new AggregateFunction(
        param1->literal_str, param2->literal_str, param3->literal_str));
    aggConfig->functions.push_back(func);
  }

  aggConfig->Configure();
  return aggConfig;
}

inline void setAggregateMode(AggregateConfigurationPtr aggConfig,
                             ConfigurationManagerPtr configurationManager) {

  int64_t totalLen = aggConfig->GetTotalLength();
  int64_t maxAggregateBuffer =
      configurationManager->GetLongValue(MAX_AGGREGATE_BUFFER);

  if (totalLen <= maxAggregateBuffer) {
    aggConfig->mode = BUFFERED;
  } else {
    aggConfig->mode = HASHED;
  }
}

inline void setSavimeDatasets(AggregateConfigurationPtr aggConfig,
                              StorageManagerPtr storageManager,
                              int32_t numCores, int64_t minWork) {

  if (aggConfig->mode == BUFFERED) {
    int64_t totalLen = aggConfig->GetTotalLength();
    SET_THREADS(totalLen, minWork, numCores);

    for (auto func : aggConfig->functions) {

      DatasetPtr aggregateDs = storageManager->Create(DOUBLE, totalLen);
      if (aggregateDs == nullptr)
        throw std::runtime_error("Could not create dataset.");

      DatasetHandlerPtr aggregateHandler =
          storageManager->GetHandler(aggregateDs);
      aggConfig->handlersToClose.push_back(aggregateHandler);

      aggConfig->datasets[func->attribName] = aggregateDs;
      aggConfig->handlers[func->attribName] = aggregateHandler;
      double *buffer = (double *)aggregateHandler->GetBuffer();
      double initVal = func->GetStartValue();

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        buffer[i] = initVal;
      }

      if (func->RequiresAuxDataset()) {
        DatasetPtr aggregateDsAux = storageManager->Create(DOUBLE, totalLen);
        if (aggregateDsAux == nullptr)
          throw std::runtime_error("Could not create dataset.");

        DatasetHandlerPtr auxAggregateHandler =
            storageManager->GetHandler(aggregateDsAux);
        aggConfig->handlersToClose.push_back(auxAggregateHandler);
        aggConfig->auxHandlers[func->attribName] = auxAggregateHandler;
        buffer =
            (double *)aggConfig->auxHandlers[func->attribName]->GetBuffer();

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          buffer[i] = initVal;
        }
      }
    }
  }
}

inline void setGlobalAggregatorBuffers(
    AggregateConfigurationPtr aggConfig,
    unordered_map<string, AggregateBufferPtr> &globalAggBuffer,
    unordered_map<string, AggregateBufferPtr> &globalAuxBuffer) {
  if (aggConfig->mode == BUFFERED) {

    for (auto func : aggConfig->functions) {

      double *buffer =
          (double *)aggConfig->handlers[func->attribName]->GetBuffer();
      int64_t size = aggConfig->datasets[func->attribName]->GetEntryCount();
      AggregateBufferPtr outputBuffer = AggregateBufferPtr(
          new VectorAggregateBuffer(aggConfig->dimensions, buffer, size));
      globalAggBuffer[func->attribName] = outputBuffer;

      if (func->RequiresAuxDataset()) {
        buffer =
            (double *)aggConfig->auxHandlers[func->attribName]->GetBuffer();
        outputBuffer = AggregateBufferPtr(
            new VectorAggregateBuffer(aggConfig->dimensions, buffer, size));
        globalAuxBuffer[func->attribName] = outputBuffer;
      }
    }

  } else {

    for (auto func : aggConfig->functions) {

      AggregateBufferPtr outputBuffer = AggregateBufferPtr(
          new MapAggregateBuffer(aggConfig->dimensions, func->GetStartValue()));
      globalAggBuffer[func->attribName] = outputBuffer;

      if (func->RequiresAuxDataset()) {
        outputBuffer = AggregateBufferPtr(new MapAggregateBuffer(
            aggConfig->dimensions, func->GetStartValue()));
        globalAuxBuffer[func->attribName] = outputBuffer;
      }
    }
  }
}

inline void setLocalAggregatorBuffers(AggregateConfigurationPtr aggConfig,
                                      AggregateFunctionPtr func,
                                      AggregateBufferPtr &localAggBuffer,
                                      AggregateBufferPtr &localAuxBuffer,
                                      int64_t outputLen) {
  if (aggConfig->mode == BUFFERED) {
    localAggBuffer = AggregateBufferPtr(new VectorAggregateBuffer(
        aggConfig->dimensions, outputLen, func->GetStartValue()));
    if (func->RequiresAuxDataset())
      localAuxBuffer = AggregateBufferPtr(
          new VectorAggregateBuffer(aggConfig->dimensions, outputLen, 0));
  } else {
    localAggBuffer = AggregateBufferPtr(
        new MapAggregateBuffer(aggConfig->dimensions, func->GetStartValue()));
    if (func->RequiresAuxDataset())
      localAuxBuffer = AggregateBufferPtr(
          new MapAggregateBuffer(aggConfig->dimensions, func->GetStartValue()));
  }
}

inline void createLogicalIndexesBuffer(AggregateConfigurationPtr aggConfig,
                                       StorageManagerPtr storageManager,
                                       SubtarPtr subtar) {

  int64_t subtarLen = subtar->GetFilledLength();
  aggConfig->positionHandlers.resize(aggConfig->dimensions.size());
  aggConfig->positionHandlersBuffers.resize(aggConfig->dimensions.size());

  for (DimensionPtr dim : aggConfig->dimensions) {
    DatasetPtr auxDataset, indexes;
    auto dimSpecs = subtar->GetDimensionSpecificationFor(dim->GetName());

    if (storageManager->MaterializeDim(dimSpecs, subtarLen, auxDataset) !=
        SAVIME_SUCCESS)
      throw std::runtime_error(ERROR_MSG("MaterializeDim", "AGGREGATE"));

    if (storageManager->Logical2Real(dim, dimSpecs, auxDataset, indexes) !=
        SAVIME_SUCCESS)
      throw std::runtime_error(ERROR_MSG("Logical2Real", "AGGREGATE"));

    auto indexHandler = storageManager->GetHandler(indexes);
    aggConfig->positionHandlers[aggConfig->name2index[dim->GetName()]] = indexHandler;
    aggConfig->handlersToClose.push_back(indexHandler);
    aggConfig->positionHandlersBuffers[aggConfig->name2index[dim->GetName()]] =
        (int64_t *)aggConfig->positionHandlers[aggConfig->name2index[dim->GetName()]]
            ->GetBuffer();
  }
}

inline void setInputBuffers(AggregateConfigurationPtr aggConfig,
                            StorageManagerPtr storageManager,
                            SubtarPtr subtar) {
  int64_t subtarLen = subtar->GetFilledLength();

  aggConfig->inputHandlers.clear();
  for (auto func : aggConfig->functions) {
    DatasetPtr dataset = subtar->GetDataSetFor(func->paramName);

    if (dataset == nullptr)
      storageManager->MaterializeDim(
          subtar->GetDimensionSpecificationFor(func->paramName), subtarLen,
          dataset);

    auto dsHandler = storageManager->GetHandler(dataset);
    aggConfig->inputHandlers[func->paramName] = dsHandler;
    aggConfig->handlersToClose.push_back(dsHandler);
  }
}

inline SubtarPtr filterAggregatedSubtar(SubtarPtr subtar, BitsetPtr bitMask,
                                        int32_t numCores, int32_t minWork,
                                        StorageManagerPtr storageManager) {

  if (bitMask->all_parallel(numCores, minWork)) {
    return subtar;
  }

  if (bitMask->none_parallel(numCores, minWork)) {
    return nullptr;
  }

  SubtarPtr newSubtar = SubtarPtr(new Subtar());
  DatasetPtr filterDs = make_shared<Dataset>(bitMask);
  filterDs->HasIndexes() = false;
  filterDs->Sorted() = false;

  applyFilter(subtar, newSubtar, filterDs, storageManager);
  newSubtar->SetTAR(subtar->GetTAR());
  return newSubtar;
}

#endif /* AGGREGATE_CONFIG_H */
