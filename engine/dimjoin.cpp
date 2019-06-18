#include <memory>

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
#include "engine/include/dml_operators.h"
#include "include/dimjoin.h"

DimJoin::DimJoin(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                 QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                 StorageManagerPtr storageManager, EnginePtr engine)  :
            EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){


  SET_INT_CONFIG_VAL(_numThreads, MAX_THREADS);
  SET_INT_CONFIG_VAL(_workPerThread, WORK_PER_THREAD);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  SET_BOOL_CONFIG_VAL(_freeBufferedSubtars, FREE_BUFFERED_SUBTARS);
  SET_DIMJOIN_TARS();

  // Obtaining subtar generator
  SET_GENERATOR(_leftGenerator, _leftTAR->GetName());
  SET_GENERATOR(_rightGenerator, _rightTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());
}

SavimeResult DimJoin::GenerateSubtar(SubTARIndex subtarIndex) {

  CHECK_INTERSECTION2();

  DMLMutex mutex;
  SubtarMap subtarMap;
  SubtarIndexList foundSubtarsIndexes;
  auto nextUntested =
    static_cast<int32_t>(_outputGenerator->GetSubtarsIndexMap(2, subtarIndex - 1) + 1);
  map<string, string> leftDims, rightDims;

  SET_SINGLE_THREAD_MULTIPLE_SUBTARS(_configurationManager);
  createDimensionMappings(_operation, leftDims, rightDims);
  UNSET_SINGLE_THREAD_MULTIPLE_SUBTARS(_configurationManager);

  OMP_EXCEPTION_ENV()
  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()

      SubtarPtr newSubtar = std::make_shared<Subtar>();
      SubtarPtr rightSubtar, leftSubtar;
      DatasetPtr leftMapDs, rightMapDs;
      DatasetPtr filterDs = nullptr;
      DatasetPtr finalLeftMap, finalRightMap;
      map<string, JoinedRangePtr> ranges;
      int64_t leftSubtarLen, rightSubtarLen, leftSubtarIndex, rightSubtarIndex;
      int32_t currentSubtar;
      bool hasTotalDims = false;
      bool hasPartialJoinDims = false;
      bool finalSubtar = false;

      while (true) {

        GET_NEXT_SUBTAR_PAIR(currentSubtar);

        if (finalSubtar)
          break;

        if (!checkIntersection(_outputTAR, leftSubtar, rightSubtar, leftDims,
                               ranges, _storageManager)) {
          continue;
        } else {

          DatasetPtr matchLeftDim, matchRightDim;
          leftSubtarLen = leftSubtar->GetFilledLength();
          rightSubtarLen = rightSubtar->GetFilledLength();

          createSingularJoinDims(_outputTAR, leftSubtar, rightSubtar, leftDims,
                                 _storageManager, _numThreads, _workPerThread,
                                 matchLeftDim, matchRightDim);

          if (_storageManager->Match(matchLeftDim, matchRightDim, finalLeftMap,
                                    finalRightMap) != SAVIME_SUCCESS) {
            throw std::runtime_error(ERROR_MSG("MatchDim", "DIMJOIN"));
          }

          /*If there are matches between subtars, process. Otherwise, skip.*/
          if (finalLeftMap == nullptr || finalRightMap == nullptr) {
            continue;
          } else {
            finalLeftMap->HasIndexes() = true;
            finalRightMap->HasIndexes() = true;
            break;
          }
        }
      }

      if (finalSubtar) {
        newSubtar = nullptr;
        break;
      }

      for (auto entry : leftSubtar->GetDataSets()) {
        string attName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (_storageManager->Filter(ds, finalLeftMap, ds) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));

        newSubtar->AddDataSet(attName, ds);
      }

      for (auto entry : rightSubtar->GetDataSets()) {
        string attName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (_storageManager->Filter(ds, finalRightMap, ds) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));

        newSubtar->AddDataSet(attName, ds);
      }

      list<DimSpecPtr> leftDimSpecs, rightDimSpecs;
      for (auto entry : leftSubtar->GetDimSpecs()) {
        string dimName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DimSpecPtr dimspec = entry.second;
        DimSpecPtr newDimspec = dimspec->Clone();

        if (leftDims.find(entry.first) != leftDims.end()) {
          newDimspec->AlterBoundaries(ranges[dimName]->lower_bound,
                                      ranges[dimName]->upper_bound);
        }

        newDimspec->AlterAdjacency(1);
        newDimspec->AlterDimension(
          _outputTAR->GetDataElement(dimName)->GetDimension());

        hasPartialJoinDims =
          hasPartialJoinDims || (dimspec->GetSpecsType() == PARTIAL);
        hasTotalDims = hasTotalDims || (dimspec->GetSpecsType() == TOTAL);
        leftDimSpecs.push_back(newDimspec);
      }

      for (auto entry : rightSubtar->GetDimSpecs()) {
        string dimName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DimSpecPtr dimspec = entry.second;
        DimSpecPtr newDimspec;

        hasPartialJoinDims =
          hasPartialJoinDims || (dimspec->GetSpecsType() == PARTIAL);
        hasTotalDims = hasTotalDims || (dimspec->GetSpecsType() == TOTAL);

        if (rightDims.find(entry.first) == rightDims.end()) {
          newDimspec = dimspec->Clone();
          newDimspec->AlterDimension(
            _outputTAR->GetDataElement(dimName)->GetDimension());
          hasTotalDims = hasTotalDims || (dimspec->GetSpecsType() == TOTAL);
          rightDimSpecs.push_back(newDimspec);
        }
      }

      if (hasTotalDims || hasPartialJoinDims) {
        for (const DimSpecPtr &dimSpecs : leftDimSpecs) {
          string originalDimName =
            dimSpecs->GetDimension()->GetName().substr(5, string::npos);
          DimensionType dimType = dimSpecs->GetDimension()->GetDimensionType();
          DatasetPtr ds, joinedDs, joinedDsReal;
          DimSpecPtr originalDimspecs =
            leftSubtar->GetDimensionSpecificationFor(originalDimName);

          if (_storageManager->PartiatMaterializeDim(
            finalLeftMap, originalDimspecs, leftSubtarLen, joinedDs,
            joinedDsReal) != SAVIME_SUCCESS)
            throw std::runtime_error(
              ERROR_MSG("PartiatMaterializeDim", "DIMJOIN"));

          if (dimType == EXPLICIT) {

            if (_storageManager->UnsafeLogical2Real(dimSpecs->GetDimension(),
                                                   originalDimspecs, joinedDs,
                                                   joinedDs) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("Logical2Real", "DIMJOIN"));

          }

          DimSpecPtr newDimSpecs = make_shared<DimensionSpecification>(
            UNSAVED_ID, dimSpecs->GetDimension(), joinedDs,
            dimSpecs->GetLowerBound(), dimSpecs->GetUpperBound());

          newSubtar->AddDimensionsSpecification(newDimSpecs);
        }

        for (const DimSpecPtr &dimSpecs : rightDimSpecs) {
          string originalDimName =
            dimSpecs->GetDimension()->GetName().substr(6, string::npos);
          DimensionType dimType = dimSpecs->GetDimension()->GetDimensionType();
          DatasetPtr ds, strechedDimDs, joinedDs, joinedDsReal;
          DimSpecPtr originalDimspecs =
            rightSubtar->GetDimensionSpecificationFor(originalDimName);

          if (_storageManager->PartiatMaterializeDim(
            finalRightMap, originalDimspecs, rightSubtarLen, joinedDs,
            joinedDsReal) != SAVIME_SUCCESS)
            throw std::runtime_error(
              ERROR_MSG("PartiatMaterializeDim", "DIMJOIN"));

          if (dimType == EXPLICIT) {
            if (_storageManager->UnsafeLogical2Real(dimSpecs->GetDimension(),
                                                   originalDimspecs, joinedDs,
                                                   joinedDs) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("Logical2Real", "DIMJOIN"));
          }

          DimSpecPtr newDimSpecs = make_shared<DimensionSpecification>(
            UNSAVED_ID, dimSpecs->GetDimension(), joinedDs,
            dimSpecs->GetLowerBound(), dimSpecs->GetUpperBound());

          newSubtar->AddDimensionsSpecification(newDimSpecs);
        }

      } else {

        leftDimSpecs.sort(compareDimSpecsByAdj);
        rightDimSpecs.sort(compareDimSpecsByAdj);
        while (!rightDimSpecs.empty()) {
          leftDimSpecs.push_back(rightDimSpecs.front());
          rightDimSpecs.pop_front();
        }

        ADJUST_SPECS(leftDimSpecs);
        for(const DimSpecPtr &spec : leftDimSpecs){
          newSubtar->AddDimensionsSpecification(spec);
        }

      }

      if (_freeBufferedSubtars) {
        _rightGenerator->TestAndDisposeSubtar(rightSubtarIndex);
      }

      if (newSubtar != nullptr) {
        mutex.lock();
        subtarMap[currentSubtar] = newSubtar;
        foundSubtarsIndexes.push_back(currentSubtar);
        mutex.unlock();
      }

    CATCH()
  } // end for

  RETHROW()

  std::sort(foundSubtarsIndexes.begin(), foundSubtarsIndexes.end());
  for (int32_t i = 0; i < foundSubtarsIndexes.size(); i++) {
    auto newSubtar = subtarMap[foundSubtarsIndexes[i]];
    _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
    _outputGenerator->SetSubtarsIndexMap(2, subtarIndex + i,
                                        foundSubtarsIndexes[i]);
  }

  return SAVIME_SUCCESS;
}