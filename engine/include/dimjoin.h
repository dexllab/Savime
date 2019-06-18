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
#ifndef DIMJOIN_H
#define DIMJOIN_H

#include "../../core/include/metadata.h"
#include "../../core/include/storage_manager.h"
#include "../../core/include/parser.h"
#include "dml_operators.h"
#include <unordered_map>
#include <memory>

using namespace std;

#define SET_DIMJOIN_TARS()                                                     \
  _leftTAR = operation->GetParametersByName(OPERAND(0))->tar;                  \
  _rightTAR = operation->GetParametersByName(OPERAND(1))->tar;                 \
  assert(_leftTAR != nullptr && _rightTAR != nullptr);                         \
  _outputTAR = operation->GetResultingTAR();                                   \

#define CHECK_INTERSECTION2()                                                  \
  bool hasNoIntersection = false;                                              \
  auto interParam = _operation->GetParametersByName(NO_INTERSECTION_JOIN);     \
  if(interParam) {                                                             \
    hasNoIntersection = interParam->literal_bool;                              \
    if(hasNoIntersection)                                                      \
       return SAVIME_SUCCESS;                                                  \
  }


struct JoinedRange {
  int64_t lower_bound;
  int64_t upper_bound;
  int64_t GetEstimatedLength() { return upper_bound - lower_bound + 1; }
};
typedef shared_ptr<JoinedRange> JoinedRangePtr;

inline bool compareDimSpecsByAdj(const DimSpecPtr &a, const DimSpecPtr &b) {
  return a->GetAdjacency() < b->GetAdjacency();
}

inline void createDimensionMappings(const OperationPtr& operation,
                             map<string, string> &leftDims,
                             map<string, string> &rightDims) {
  int32_t count = 0;

  while (true) {
    auto param1 = operation->GetParametersByName(DIM(count++));
    auto param2 = operation->GetParametersByName(DIM(count++));
    if (param1 == nullptr)
      break;
    leftDims[param1->literal_str] = param2->literal_str;
    rightDims[param2->literal_str] = param1->literal_str;
  }
}

#define GET_NEXT_SUBTAR_PAIR(INDEX)\
  mutex.lock();\
  INDEX = nextUntested;\
  nextUntested++;\
  rightSubtarIndex =\
      _outputGenerator->GetSubtarsIndexMap(1, INDEX - 1) + 1;\
  rightSubtar = _rightGenerator->GetSubtar(rightSubtarIndex);\
  if (rightSubtar == nullptr) {\
    leftSubtarIndex =\
        _outputGenerator->GetSubtarsIndexMap(0, INDEX - 1) + 1;\
    _leftGenerator->TestAndDisposeSubtar(leftSubtarIndex - 1);\
    rightSubtarIndex = 0;\
    rightSubtar = _rightGenerator->GetSubtar(rightSubtarIndex);\
  } else if (INDEX > 0) {\
    leftSubtarIndex =\
        _outputGenerator->GetSubtarsIndexMap(0, INDEX - 1);\
  } else {\
    leftSubtarIndex = 0;\
  }\
  if(leftSubtarIndex >= 0)\
    leftSubtar = _leftGenerator->GetSubtar(leftSubtarIndex);\
  else\
    leftSubtar =  nullptr;\
  if (leftSubtar == nullptr || rightSubtar == nullptr) {\
    int32_t lastSubtar =\
        _outputGenerator->GetSubtarsIndexMap(1, INDEX - 2);\
    if (!_freeBufferedSubtars) {\
      for (int32_t i = 0; i <= lastSubtar; i++) {\
        _rightGenerator->TestAndDisposeSubtar(i);\
      }\
    }\
    finalSubtar = true;\
  }\
  if(!finalSubtar) { \
    _outputGenerator->SetSubtarsIndexMap(0, INDEX, leftSubtarIndex);\
    _outputGenerator->SetSubtarsIndexMap(1, INDEX, rightSubtarIndex);\
  }\
  mutex.unlock();

inline bool checkIntersection(const TARPtr &tar, SubtarPtr subtar1, SubtarPtr subtar2,
                       map<string, string> joinedDims,
                       map<string, JoinedRangePtr> &intersection,
                       const StorageManagerPtr& storageManager) {

  for (auto entry : joinedDims) {
    DimSpecPtr dimSpecs1 = subtar1->GetDimensionSpecificationFor(entry.first);
    DimSpecPtr dimSpecs2 = subtar2->GetDimensionSpecificationFor(entry.second);
    DimensionPtr dim =
        tar->GetDataElement(LEFT_DATAELEMENT_PREFIX + entry.first)
            ->GetDimension();
    DimSpecPtr dimSpecs[2] = {dimSpecs1, dimSpecs2};
    Literal lower[2], upper[2];
    int64_t realLower[2], realUpper[2];

    for (int32_t i = 0; i < 2; i++) {
      lower[i] = storageManager->Real2Logical(
          dimSpecs[i]->GetDimension(), dimSpecs[i]->GetLowerBound());
      upper[i] = storageManager->Real2Logical(
          dimSpecs[i]->GetDimension(), dimSpecs[i]->GetUpperBound());
      //realLower[i] = storageManager->Logical2Real(dim, lower[i]);

      /*Logical2real should give the 2 closesst values*/
      realLower[i] = storageManager->Logical2ApproxReal(dim, lower[i]).sup;
      realUpper[i] = storageManager->Logical2ApproxReal(dim, upper[i]).inf;
    }

    if (IN_RANGE(realLower[0], realLower[1], realUpper[1]) ||
        IN_RANGE(realLower[1], realLower[0], realUpper[0])) {
      JoinedRangePtr joinedRange = std::make_shared<JoinedRange>();
      joinedRange->lower_bound =
          (realLower[0] > realLower[1]) ? realLower[0] : realLower[1];
      joinedRange->upper_bound =
          (realUpper[0] < realUpper[1]) ? realUpper[0] : realUpper[1];
      intersection[dim->GetName()] = joinedRange;
    } else {
      return false;
    }
  }

  return true;
}

inline void calculateMultipliers(unordered_map<string, int64_t> &multipliers,
                          vector<DimensionPtr> dimensions) {

  auto numDims = dimensions.size();
  for (int32_t i = 0; i < numDims; i++) {
    auto name = dimensions[i]->GetName();
    multipliers[name] = (1);
    for (int32_t j = i + 1; j < numDims; j++) {
      int64_t preamble = multipliers[name];
      multipliers[name] = preamble * dimensions[j]->GetCurrentLength();
    }
  }
}

inline DatasetPtr calculateSingleDim(vector<DatasetPtr> matDims,
                                     vector<int64_t> multipliers,
                                     int32_t invalidFlag,
                                     const StorageManagerPtr& storageManager,
                                     int64_t numCores, int64_t minWork) {

  auto numDims = matDims.size();

  if (numDims == 1) {
   // return matDims[0];
  }

  vector<DatasetHandlerPtr> handlers(numDims);
  vector<int64_t *> buffers(numDims);
  auto dimCount = 0;
  auto size = matDims[0]->GetEntryCount();

  for (const auto &matdimDs : matDims) {
    handlers[dimCount] = storageManager->GetHandler(matdimDs);
    buffers[dimCount] = (int64_t *) handlers[dimCount]->GetBuffer();
    dimCount++;
  }

  DatasetPtr singleDs = storageManager->Create(TAR_POSITION, size);
  if (singleDs == nullptr)
    throw std::runtime_error(ERROR_MSG("MatchDim", "DIMJOIN"));

  DatasetHandlerPtr singleHandler = storageManager->GetHandler(singleDs);
  TARPosition *singleBuffer = (TARPosition *) singleHandler->GetBuffer();
  bool isSorted = true;

  SET_THREADS_ALIGNED(size, minWork, numCores, singleDs->GetBitsPerBlock());
#pragma omp parallel
  for (int64_t i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
    singleBuffer[i] = 0;
    for (int32_t d = 0; d < numDims; d++) {
      if (buffers[d][i] == INVALID_EXACT_REAL_INDEX) {

        if(singleDs->BitMask() == nullptr){
          singleDs->InflateBitMask();
        }

        singleBuffer[i] = static_cast<TARPosition>(invalidFlag);
        (*singleDs->BitMask())[i] = true;
        //isSorted = false;
        break;
      }
      singleBuffer[i] += multipliers[d] * buffers[d][i];
    }

    if(i > 0 && isSorted){
      if(singleBuffer[i] < singleBuffer[i-1])
        isSorted = false;
    }
  }

  for (auto handler : handlers) {
    handler->Close();
  }
  singleHandler->Close();
  singleDs->Sorted() = isSorted;

  return singleDs;
}

inline DatasetPtr materializeForJoinedDim(DimSpecPtr specs, DimensionPtr dim,
                                          int64_t size,
                                          StorageManagerPtr storageManager) {
  DatasetPtr materialized;
  if (storageManager->MaterializeDim(specs, size, materialized) ==
      SAVIME_FAILURE) {
    throw std::runtime_error(ERROR_MSG("MatchDim", "DIMJOIN"));
  }

  if (storageManager->UnsafeLogical2Real(dim, specs, materialized, materialized)
      == SAVIME_FAILURE) {
    throw std::runtime_error(ERROR_MSG("MatchDim", "DIMJOIN"));
  }

  return materialized;
}

inline void createSingularJoinDims(const TARPtr& tar, const SubtarPtr& subtar1,
                                   const SubtarPtr& subtar2,
                                   map<string, string> joinedDims,
                                   const StorageManagerPtr& storageManager,
                                   int32_t numCores, int64_t minWork,
                                   DatasetPtr &leftSingDim,
                                   DatasetPtr &rightSingDim) {
#define INVALID_LEFT_FLAG -1
#define INVALID_RIGHT_FLAG -2

  int32_t i = 0;
  unordered_map<string, int64_t> multipliers;
  vector<DimensionPtr> dimensions;
  vector<DatasetPtr> leftDims(joinedDims.size());
  vector<int64_t> leftDimsMultipliers(joinedDims.size());
  vector<DatasetPtr> rightDims(joinedDims.size());
  vector<int64_t> rightDimsMultipliers(joinedDims.size());

  /*Listing dimensions resulting from JOIN.*/
  for (auto entry : joinedDims) {
    dimensions.push_back(tar->GetDataElement(LEFT_DATAELEMENT_PREFIX
                                                 + entry.first)->GetDimension());
  }

  /*Calculating multipliers for linearization.*/
  calculateMultipliers(multipliers, dimensions);

  /*For each dimension pair.*/
  for (auto entry : joinedDims) {

    auto outputTarDimName = LEFT_DATAELEMENT_PREFIX + entry.first;
    auto dim = tar->GetDataElement(outputTarDimName)->GetDimension();
    auto multiplier = multipliers[outputTarDimName];

    /*Create a set of real indexes with respect to the joined dimension,*/
    auto dimSpecs1 = subtar1->GetDimensionSpecificationFor(entry.first);
    auto len1 = subtar1->GetFilledLength();
    leftDims[i] = materializeForJoinedDim(dimSpecs1, dim, len1, storageManager);
    leftDimsMultipliers[i] = multiplier;

    /*Same as before, but now for the other TAR's dimension,*/
    auto dimSpecs2 = subtar2->GetDimensionSpecificationFor(entry.second);
    auto len2 = subtar2->GetFilledLength();
    rightDims[i] =
        materializeForJoinedDim(dimSpecs2, dim, len2, storageManager);
    rightDimsMultipliers[i] = multiplier;
    i++;
  }

  /*Getting single dimensions values.*/
  leftSingDim = calculateSingleDim(leftDims, leftDimsMultipliers,
                                   INVALID_LEFT_FLAG,
                                   storageManager, numCores, minWork);

  rightSingDim = calculateSingleDim(rightDims, rightDimsMultipliers,
                                    INVALID_RIGHT_FLAG,
                                    storageManager, numCores, minWork);
}

#endif /* DIMJOIN_H */
