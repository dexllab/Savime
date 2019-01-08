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
#include "include/dml_operators.h"
#include "include/subset.h"

Subset::Subset(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){

  SET_TARS(_inputTAR, _outputTAR);
  SET_INT_CONFIG_VAL(_numThreads, MAX_THREADS);
  SET_INT_CONFIG_VAL(_workPerThread, WORK_PER_THREAD);
  SET_INT_CONFIG_VAL(_numSubtars, MAX_PARA_SUBTARS);
  SET_GENERATOR(_generator, _inputTAR->GetName());
  SET_GENERATOR(_outputGenerator, _outputTAR->GetName());

  subset_get_boundaries(_operation, _filteredDim, _lower_bounds, _upper_bounds);
}

SavimeResult Subset::GenerateSubtar(SubTARIndex subtarIndex) {

  bool isEmptySubset;
  auto interParam = _operation->GetParametersByName(EMPTY_SUBSET);
  if (interParam) {
    isEmptySubset = interParam->literal_bool;
    if (isEmptySubset)
      return SAVIME_SUCCESS;
  }

  OMP_EXCEPTION_ENV()

  DMLMutex mutex;
  SubtarMap subtarMap;
  SubtarIndexList foundSubtarsIndexes;

  SubTARIndex nextUntested = subtarIndex;
  auto previousMapping = _outputGenerator->GetSubtarsIndexMap(subtarIndex - 1);
  if (previousMapping != -1) {
    nextUntested = previousMapping + 1;
  }

  SET_SUBTARS_THREADS(_numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      SubtarPtr subtar, newSubtar = std::make_shared<Subtar>();
      SubTARIndex currentSubtar = 0;

      while (true) {

        mutex.lock();
        currentSubtar = nextUntested++;
        subtar = _generator->GetSubtar(currentSubtar);
        mutex.unlock();

        if (subtar == nullptr)
          break;

        SubsetIntersectionType currentSubtarIntersection = SUBSET_TOTAL;
        bool isAllOrdered = true;

        check_intersection(_storageManager, subtar, _outputTAR, _inputTAR,
                           _filteredDim, _lower_bounds, _upper_bounds,
                           currentSubtarIntersection, isAllOrdered);

        if (currentSubtarIntersection == SUBSET_NONE) {
          _generator->TestAndDisposeSubtar(currentSubtar);
          continue;
        } else {
          savime_size_t totalLength = subtar->GetFilledLength();
          newSubtar->SetTAR(_outputTAR);

          if (currentSubtarIntersection == SUBSET_PARTIAL) {
            DatasetPtr filter, comparisonResult, matDim, realDim;
            vector<DimSpecPtr> dimensionsSpecs;


            for (auto entry : subtar->GetDimSpecs()) {

              Literal logLower, logUpper;
              int64_t originalRealLowerBound, originalRealUpperBound;
              double subsetLowerBound, subsetUpperBound;
              auto dimName = entry.first;
              auto specs = entry.second;
              auto dim = _outputTAR->GetDataElement(dimName)->GetDimension();
              auto originalDim =
                _inputTAR->GetDataElement(dimName)->GetDimension();

              SET_LITERAL(logLower, originalDim->GetType(), dim->GetLowerBound());
              SET_LITERAL(logUpper, originalDim->GetType(), dim->GetUpperBound());
              originalRealLowerBound =
                _storageManager->Logical2Real(originalDim, logLower);
              originalRealUpperBound =
                _storageManager->Logical2Real(originalDim, logUpper);

              int64_t offset =
                originalRealLowerBound; //- originalDim->real_lower_bound;
              subsetLowerBound = dim->GetLowerBound();
              subsetUpperBound = dim->GetUpperBound();

              auto newDimSpecsLowerBound =
                std::max(specs->GetLowerBound(), originalRealLowerBound) -
                offset;
              auto newDimSpecsUpperBound =
                std::min(specs->GetUpperBound(), originalRealUpperBound) -
                offset;

              DimSpecPtr newDimSpec;
              if (specs->GetSpecsType() == ORDERED) {
                newDimSpec = make_shared<DimensionSpecification>(
                  UNSAVED_ID,
                  _outputTAR->GetDataElement(dimName)->GetDimension(),
                  newDimSpecsLowerBound, newDimSpecsUpperBound,
                  specs->GetStride(), specs->GetAdjacency());
              } else if (specs->GetSpecsType() == PARTIAL) {
                newDimSpec = make_shared<DimensionSpecification>(
                  UNSAVED_ID,
                  _outputTAR->GetDataElement(dimName)->GetDimension(),
                  specs->GetDataset(), newDimSpecsLowerBound,
                  newDimSpecsUpperBound, specs->GetStride(),
                  specs->GetAdjacency());

              } else if (specs->GetSpecsType() == TOTAL) {
                newDimSpec = make_shared<DimensionSpecification>(
                  UNSAVED_ID,
                  _outputTAR->GetDataElement(dimName)->GetDimension(),
                  specs->GetDataset(), newDimSpecsLowerBound,
                  newDimSpecsUpperBound);
              }

              if (!isAllOrdered) {
                if (newDimSpec->GetLowerBound() + offset !=
                    specs->GetLowerBound()) {
                  if (_storageManager->ComparisonDim(
                    string(_GEQ), specs, totalLength,
                    Literal(subsetLowerBound),
                    comparisonResult) != SAVIME_SUCCESS)
                    throw std::runtime_error(
                      ERROR_MSG("ComparisonDim", "SUBSET"));

                  if (filter != nullptr) {
                    if (_storageManager->And(filter, comparisonResult, filter) !=
                        SAVIME_SUCCESS)
                      throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                  } else {
                    filter = comparisonResult;
                  }
                }

                if (newDimSpec->GetUpperBound() + offset !=
                    specs->GetUpperBound()) {
                  if (_storageManager->ComparisonDim(
                    _LEQ, specs, totalLength, subsetUpperBound,
                    comparisonResult) != SAVIME_SUCCESS)
                    throw std::runtime_error(
                      ERROR_MSG("ComparisonDim", "SUBSET"));

                  if (filter != nullptr) {
                    if (_storageManager->And(filter, comparisonResult, filter) !=
                        SAVIME_SUCCESS)
                      throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                  } else {
                    filter = comparisonResult;
                  }
                }
              }

              dimensionsSpecs.push_back(newDimSpec);
              newSubtar->AddDimensionsSpecification(newDimSpec);
            }

            if (isAllOrdered) {

              vector<RealIndex> lowerBounds;
              vector<RealIndex> upperBounds;
              vector<DimSpecPtr> _dimensionsSpecs;
              Literal lw, up;

              for (auto entry : subtar->GetDimSpecs()) {
                auto outDim =
                  _outputTAR->GetDataElement(entry.first)->GetDimension();
                lw = outDim->GetLowerBound();
                up = outDim->GetUpperBound();
                auto realLower = _storageManager->Logical2Real(
                  entry.second->GetDimension(), lw);
                auto realUpper = _storageManager->Logical2Real(
                  entry.second->GetDimension(), up);
                lowerBounds.push_back(realLower);
                upperBounds.push_back(realUpper);
                _dimensionsSpecs.push_back(entry.second);
              }
              _storageManager->SubsetDims(_dimensionsSpecs, lowerBounds,
                                         upperBounds, filter);

            } else {
              if (!filter->BitMask()->any_parallel(_numThreads, _workPerThread)) {
                _generator->TestAndDisposeSubtar(currentSubtar);
                continue;
              }
            }

            for (auto entry : subtar->GetDimSpecs()) {
              Literal logLower, logUpper;
              int64_t originalRealLowerBound, originalRealUpperBound;
              auto dimName = entry.first;
              auto specs = entry.second;
              auto dim = _outputTAR->GetDataElement(dimName)->GetDimension();
              auto originalDim =
                _inputTAR->GetDataElement(dimName)->GetDimension();
              auto newDimSpec = newSubtar->GetDimensionSpecificationFor(dimName);

              SET_LITERAL(logLower, originalDim->GetType(), dim->GetLowerBound());
              SET_LITERAL(logUpper, originalDim->GetType(), dim->GetUpperBound());

              if (newDimSpec->GetSpecsType() == PARTIAL) {
                DatasetPtr auxDs1, auxDs2, fitered, filterResult;

                if (originalDim->GetDimensionType() == IMPLICIT) {
                  if (_storageManager->Comparison(_GEQ, newDimSpec->GetDataset(),
                                                 dim->GetLowerBound(),
                                                 auxDs1) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (_storageManager->Comparison(_LEQ, newDimSpec->GetDataset(),
                                                 dim->GetUpperBound(),
                                                 auxDs2) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (_storageManager->And(auxDs1, auxDs2, fitered) !=
                      SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                } else {
                  DatasetPtr logical;

                  if (_storageManager->Real2Logical(originalDim, specs,
                                                   newDimSpec->GetDataset(),
                                                   logical) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Real2Logical", "SUBSET"));

                  if (_storageManager->Comparison(_GEQ, logical,
                                                 dim->GetLowerBound(),
                                                 auxDs1) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (_storageManager->Comparison(_LEQ, logical,
                                                 dim->GetUpperBound(),
                                                 auxDs2) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (_storageManager->And(auxDs1, auxDs2, fitered) !=
                      SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                }

                if (_storageManager->Filter(newDimSpec->GetDataset(), fitered,
                                           filterResult) != SAVIME_SUCCESS)
                  throw std::runtime_error(ERROR_MSG("Filter", "SUBSET"));

                newDimSpec->AlterDataset(filterResult);
              } else if (newDimSpec->GetSpecsType() == TOTAL) {
                if (_storageManager->PartiatMaterializeDim(
                  filter, specs, totalLength, matDim, realDim) !=
                    SAVIME_SUCCESS)
                  throw std::runtime_error(
                    ERROR_MSG("PartiatMaterializeDim", "SUBSET"));

                newDimSpec->AlterDataset(matDim);
              }
            }

            for (auto entry : subtar->GetDataSets()) {
              DatasetPtr dataset;
              if (_storageManager->Filter(entry.second, filter, dataset) !=
                  SAVIME_SUCCESS)
                throw std::runtime_error(ERROR_MSG("Filter", "SUBSET"));

              newSubtar->AddDataSet(entry.first, dataset);
            }

            std::sort(dimensionsSpecs.begin(), dimensionsSpecs.end(), compareAdj);
            ADJUST_SPECS(dimensionsSpecs);

          }// end if
          else { // if totally within the range
            for (auto entry : subtar->GetDataSets()) {
              if (_outputTAR->GetDataElement(entry.first) != nullptr)
                newSubtar->AddDataSet(entry.first, entry.second);
            }// end for

            for (auto entry : subtar->GetDimSpecs()) {
              newSubtar->AddDimensionsSpecification(entry.second);
            }// end for
          }// end else
        }

        break;

      } // END WHILE

      if (subtar == nullptr) {
        break;
      }

      mutex.lock();
      subtarMap[currentSubtar] = newSubtar;
      foundSubtarsIndexes.push_back(currentSubtar);
      mutex.unlock();

    CATCH()
  } // END FOR
  RETHROW()

  std::sort(foundSubtarsIndexes.begin(), foundSubtarsIndexes.end());
  for (int32_t i = 0; i < foundSubtarsIndexes.size(); i++) {
    auto newSubtar = subtarMap[foundSubtarsIndexes[i]];
    _outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
    _generator->TestAndDisposeSubtar(foundSubtarsIndexes[i]);
    _outputGenerator->SetSubtarsIndexMap(subtarIndex + i,
                                        foundSubtarsIndexes[i]);

  }

  if(foundSubtarsIndexes.empty()) {
    for (int32_t i = 0; i < _numSubtars; i++) {
      _outputGenerator->AddSubtar(subtarIndex + i, nullptr);
    }
  }

  return SAVIME_SUCCESS;
}