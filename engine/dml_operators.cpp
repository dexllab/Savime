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
#include <cassert>
#include <omp.h>
#include "../core/include/symbols.h"
#include "include/dml_operators.h"
#include "include/default_engine.h"
#include "include/filter.h"
#include "include/subset.h"
#include "include/aggregate_config.h"
#include "include/dimjoin.h"
#include "include/viz.h"
#include "include/aggregate_engine.h"

int scan(SubTARIndex subtarIndex, OperationPtr operation,
         ConfigurationManagerPtr configurationManager,
         QueryDataManagerPtr queryDataManager,
         MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
         EnginePtr engine) {

  DECLARE_TARS(inputTAR, outputTAR);
  DECLARE_GENERATOR(generator, inputTAR->GetName());

  while (true) {
    auto subtar = generator->GetSubtar(subtarIndex);
    if (subtar == nullptr)
      break;
    generator->TestAndDisposeSubtar(subtarIndex);
    subtarIndex++;
  }

  return SAVIME_SUCCESS;
}

int select(SubTARIndex subtarIndex, OperationPtr operation,
           ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine) {

  OMP_EXCEPTION_ENV()
  DECLARE_TARS(inputTAR, outputTAR);
  DECLARE_GENERATOR(generator, inputTAR->GetName());
  DECLARE_GENERATOR(outputGenerator, outputTAR->GetName());
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);
  SubtarPtr subtar[numSubtars];
  SubTARIndex subtarIndexes[numSubtars];
  int64_t offset, skew[numSubtars];
  int64_t lowerBoundinI[numSubtars];
  int64_t upperBoundinI[numSubtars];

  for (int32_t i = 0; i < numSubtars; i++) {
    subtar[i] = generator->GetSubtar(subtarIndex + i);
    subtarIndexes[i] = subtarIndex + i;
  }

  for (int32_t i = 0; i < numSubtars; i++) {

    if (subtar[i] == nullptr)
      break;

    if (subtarIndexes[i] > 0)
      offset = outputGenerator->GetSubtarsIndexMap(subtarIndexes[i] - 1);
    else
      offset = 0;

    lowerBoundinI[i] = offset;
    upperBoundinI[i] = offset + subtar[i]->GetFilledLength() - 1;
    outputGenerator->SetSubtarsIndexMap(subtarIndexes[i], upperBoundinI[i] + 1);
    skew[i] = upperBoundinI[i] - lowerBoundinI[i] + 1;
  }

  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      if (subtar[i] == nullptr)
        break;

      SubtarPtr newSubtar = SubtarPtr(new Subtar());

      if (outputTAR->GetDimensions().size() == 1 &&
        !outputTAR->GetDimensions().front()->GetName().compare(
          DEFAULT_SYNTHETIC_DIMENSION)) {

        for (auto entry : subtar[i]->GetDataSets()) {
          if (outputTAR->GetDataElement(entry.first) != nullptr)
            newSubtar->AddDataSet(entry.first, entry.second);
        }

        auto dimension = outputTAR->GetDataElement(DEFAULT_SYNTHETIC_DIMENSION)
                                  ->GetDimension();
        DimSpecPtr newDimSpecs = make_shared<DimensionSpecification>(
          UNSAVED_ID, dimension, lowerBoundinI[i], upperBoundinI[i], skew[i],
          (savime_size_t)1);
        newSubtar->AddDimensionsSpecification(newDimSpecs);

        int64_t totalLength = subtar[i]->GetFilledLength();

        for (auto entry : subtar[i]->GetDimSpecs()) {
          if (outputTAR->GetDataElement(entry.first) != nullptr) {
            auto dimSpec = entry.second;
            DatasetPtr dataset;
            if (storageManager->MaterializeDim(dimSpec, totalLength, dataset) !=
              SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("MaterializeDim", "SELECT"));
            newSubtar->AddDataSet(dimSpec->GetDimension()->GetName(), dataset);
          }
        }
      } else {
        for (auto entry : subtar[i]->GetDataSets()) {
          if (outputTAR->GetDataElement(entry.first) != nullptr)
            newSubtar->AddDataSet(entry.first, entry.second);
        }

        for (auto entry : subtar[i]->GetDimSpecs()) {
          newSubtar->AddDimensionsSpecification(entry.second);
        }
      }

      outputGenerator->AddSubtar(subtarIndexes[i], newSubtar);
      generator->TestAndDisposeSubtar(subtarIndexes[i]);
    CATCH()
  }

  RETHROW()

  return SAVIME_SUCCESS;
}

int filter(SubTARIndex subtarIndex, OperationPtr operation,
           ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine) {

  OMP_EXCEPTION_ENV()
  DECLARE_TARS(inputTAR, outputTAR);
  GET_INT_CONFIG_VAL(numThreads, MAX_THREADS);
  GET_INT_CONFIG_VAL(workPerThread, WORK_PER_THREAD);
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);
  ParameterPtr filterParam = operation->GetParameters().back();
  DECLARE_GENERATOR(generator, inputTAR->GetName());
  DECLARE_GENERATOR(filterGenerator, filterParam->tar->GetName());
  DECLARE_GENERATOR(outputGenerator, outputTAR->GetName());

  int32_t currentSubtar[numSubtars];
  SubtarPtr subtar[numSubtars], filterSubtar[numSubtars];
  DatasetPtr filterDs[numSubtars];

  for (int32_t i = 0; i < numSubtars; i++) {
    currentSubtar[i] = subtarIndex + i;

    if (outputGenerator->GetSubtarsIndexMap(currentSubtar[i] - 1) !=
      UNDEFINED) {
      currentSubtar[i] =
        outputGenerator->GetSubtarsIndexMap(currentSubtar[i] - 1) + 1;
    }

    while (true) {
      filterSubtar[i] = filterGenerator->GetSubtar(currentSubtar[i]);
      if (filterSubtar[i] == nullptr)
        break;

      filterDs[i] = filterSubtar[i]->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
      if (filterDs[i]->BitMask()->any_parallel(numThreads, workPerThread))
        break;

      filterGenerator->TestAndDisposeSubtar(currentSubtar[i]);
      currentSubtar[i]++;
    }

    subtar[i] = generator->GetSubtar(currentSubtar[i]);
    outputGenerator->SetSubtarsIndexMap(subtarIndex + i, currentSubtar[i]);
  }

  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      SubtarPtr newSubtar = SubtarPtr(new Subtar);

      if (subtar[i] == nullptr || filterDs[i] == nullptr)
        break;

      int64_t totalLength = subtar[i]->GetFilledLength();
      newSubtar->SetTAR(outputTAR);

      if (!filterDs[i]->BitMask()->all_parallel(numThreads, workPerThread)) {

        applyFilter(subtar[i], newSubtar, filterDs[i], storageManager);

      } else {
        for (auto entry : subtar[i]->GetDataSets()) {
          if (outputTAR->GetDataElement(entry.first) != nullptr)
            newSubtar->AddDataSet(entry.first, entry.second);
        }

        for (auto entry : subtar[i]->GetDimSpecs()) {
          newSubtar->AddDimensionsSpecification(entry.second);
        }
      }

      outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      generator->TestAndDisposeSubtar(currentSubtar[i]);
      filterGenerator->TestAndDisposeSubtar(currentSubtar[i]);
    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}

int subset(SubTARIndex subtarIndex, OperationPtr operation,
           ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine) {

  bool isEmptySubset;
  auto interParam = operation->GetParametersByName(EMPTY_SUBSET);
  if (interParam) {
    isEmptySubset = interParam->literal_bool;
    if (isEmptySubset)
      return SAVIME_SUCCESS;
  }

  OMP_EXCEPTION_ENV()
  GET_INT_CONFIG_VAL(numThreads, MAX_THREADS);
  GET_INT_CONFIG_VAL(workPerThread, WORK_PER_THREAD);
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);

  DECLARE_TARS(inputTAR, outputTAR);
  DECLARE_GENERATOR(generator, inputTAR->GetName());
  DECLARE_GENERATOR(outputGenerator, outputTAR->GetName());
  FilteredDimMap filteredDim;
  BoundMap lower_bounds, upper_bounds;
  DMLMutex mutex;
  SubtarMap subtarMap;
  SubtarIndexList foundSubtarsIndexes;

  subset_get_boundaries(operation, filteredDim, lower_bounds, upper_bounds);

  int32_t nextUntested = subtarIndex;
  auto previousMapping = outputGenerator->GetSubtarsIndexMap(subtarIndex - 1);
  if (previousMapping != -1) {
    nextUntested = previousMapping + 1;
  }

  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      SubtarPtr subtar, newSubtar = SubtarPtr(new Subtar());
      int32_t currentSubtar = 0;

      while (true) {

        SUBSET_GET_NEXT_UNTESTED(currentSubtar);
        subtar = generator->GetSubtar(currentSubtar);
        if (subtar == nullptr)
          break;

        SubsetIntersectionType currentSubtarIntersection = SUBSET_TOTAL;
        bool isAllOrdered = true;

        check_intersection(storageManager, subtar, outputTAR, inputTAR,
                           filteredDim, lower_bounds, upper_bounds,
                           currentSubtarIntersection, isAllOrdered);

        if (currentSubtarIntersection == SUBSET_NONE) {
          generator->TestAndDisposeSubtar(currentSubtar);
          continue;
        } else {
          savime_size_t totalLength = subtar->GetFilledLength();
          newSubtar->SetTAR(outputTAR);

          if (currentSubtarIntersection == SUBSET_PARTIAL) {
            DatasetPtr filter, comparisonResult, matDim, realDim;
            vector<DimSpecPtr> dimensionsSpecs;

            for (auto entry : subtar->GetDimSpecs()) {
              Literal logLower, logUpper;
              int64_t originalRealLowerBound, originalRealUpperBound;
              double subsetLowerBound, subsetUpperBound;
              auto dimName = entry.first;
              auto specs = entry.second;
              auto dim = outputTAR->GetDataElement(dimName)->GetDimension();
              auto originalDim =
                inputTAR->GetDataElement(dimName)->GetDimension();

              SET_LITERAL(logLower, originalDim->GetType(), dim->GetLowerBound());
              SET_LITERAL(logUpper, originalDim->GetType(), dim->GetUpperBound());
              originalRealLowerBound =
                storageManager->Logical2Real(originalDim, logLower);
              originalRealUpperBound =
                storageManager->Logical2Real(originalDim, logUpper);

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
                  outputTAR->GetDataElement(dimName)->GetDimension(),
                  newDimSpecsLowerBound, newDimSpecsUpperBound,
                  specs->GetSkew(), specs->GetAdjacency());
              } else if (specs->GetSpecsType() == PARTIAL) {
                newDimSpec = make_shared<DimensionSpecification>(
                  UNSAVED_ID,
                  outputTAR->GetDataElement(dimName)->GetDimension(),
                  specs->GetDataset(), newDimSpecsLowerBound,
                  newDimSpecsUpperBound, specs->GetSkew(),
                  specs->GetAdjacency());

              } else if (specs->GetSpecsType() == TOTAL) {
                newDimSpec = make_shared<DimensionSpecification>(
                  UNSAVED_ID,
                  outputTAR->GetDataElement(dimName)->GetDimension(),
                  specs->GetDataset(), newDimSpecsLowerBound,
                  newDimSpecsUpperBound);
              }

              if (!isAllOrdered) {
                if (newDimSpec->GetLowerBound() + offset !=
                  specs->GetLowerBound()) {
                  if (storageManager->ComparisonDim(
                    string(_GEQ), specs, totalLength,
                    Literal(subsetLowerBound),
                    comparisonResult) != SAVIME_SUCCESS)
                    throw std::runtime_error(
                      ERROR_MSG("ComparisonDim", "SUBSET"));

                  if (filter != nullptr) {
                    if (storageManager->And(filter, comparisonResult, filter) !=
                      SAVIME_SUCCESS)
                      throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                  } else {
                    filter = comparisonResult;
                  }
                }

                if (newDimSpec->GetUpperBound() + offset !=
                  specs->GetUpperBound()) {
                  if (storageManager->ComparisonDim(
                    _LEQ, specs, totalLength, subsetUpperBound,
                    comparisonResult) != SAVIME_SUCCESS)
                    throw std::runtime_error(
                      ERROR_MSG("ComparisonDim", "SUBSET"));

                  if (filter != nullptr) {
                    if (storageManager->And(filter, comparisonResult, filter) !=
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
                  outputTAR->GetDataElement(entry.first)->GetDimension();
                lw = outDim->GetLowerBound();
                up = outDim->GetUpperBound();
                auto realLower = storageManager->Logical2Real(
                  entry.second->GetDimension(), lw);
                auto realUpper = storageManager->Logical2Real(
                  entry.second->GetDimension(), up);
                lowerBounds.push_back(realLower);
                upperBounds.push_back(realUpper);
                _dimensionsSpecs.push_back(entry.second);
              }
              storageManager->SubsetDims(_dimensionsSpecs, lowerBounds,
                                         upperBounds, filter);

            } else {
              if (!filter->BitMask()->any_parallel(numThreads, workPerThread)) {
                generator->TestAndDisposeSubtar(currentSubtar);
                continue;
              }
            }

            for (auto entry : subtar->GetDimSpecs()) {
              Literal logLower, logUpper;
              int64_t originalRealLowerBound, originalRealUpperBound;
              auto dimName = entry.first;
              auto specs = entry.second;
              auto dim = outputTAR->GetDataElement(dimName)->GetDimension();
              auto originalDim =
                inputTAR->GetDataElement(dimName)->GetDimension();
              auto newDimSpec = newSubtar->GetDimensionSpecificationFor(dimName);

              SET_LITERAL(logLower, originalDim->GetType(), dim->GetLowerBound());
              SET_LITERAL(logUpper, originalDim->GetType(), dim->GetUpperBound());
              // originalRealLowerBound =
              //    storageManager->Logical2ApproxReal(originalDim, logLower);
              // originalRealUpperBound =
              //    storageManager->Logical2ApproxReal(originalDim, logUpper);

              if (newDimSpec->GetSpecsType() == PARTIAL) {
                DatasetPtr auxDs1, auxDs2, fitered, filterResult;

                if (originalDim->GetDimensionType() == IMPLICIT) {
                  if (storageManager->Comparison(_GEQ, newDimSpec->GetDataset(),
                                                 dim->GetLowerBound(),
                                                 auxDs1) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (storageManager->Comparison(_LEQ, newDimSpec->GetDataset(),
                                                 dim->GetUpperBound(),
                                                 auxDs2) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (storageManager->And(auxDs1, auxDs2, fitered) !=
                    SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                } else {
                  DatasetPtr logical;

                  if (storageManager->Real2Logical(originalDim, specs,
                                                   newDimSpec->GetDataset(),
                                                   logical) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Real2Logical", "SUBSET"));

                  if (storageManager->Comparison(_GEQ, logical,
                                                 dim->GetLowerBound(),
                                                 auxDs1) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (storageManager->Comparison(_LEQ, logical,
                                                 dim->GetUpperBound(),
                                                 auxDs2) != SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("Comparison", "SUBSET"));

                  if (storageManager->And(auxDs1, auxDs2, fitered) !=
                    SAVIME_SUCCESS)
                    throw std::runtime_error(ERROR_MSG("And", "SUBSET"));
                }

                if (storageManager->Filter(newDimSpec->GetDataset(), fitered,
                                           filterResult) != SAVIME_SUCCESS)
                  throw std::runtime_error(ERROR_MSG("Filter", "SUBSET"));

                newDimSpec->AlterDataset(filterResult);
              } else if (newDimSpec->GetSpecsType() == TOTAL) {
                if (storageManager->PartiatMaterializeDim(
                  filter, specs, totalLength, matDim, realDim) !=
                  SAVIME_SUCCESS)
                  throw std::runtime_error(
                    ERROR_MSG("PartiatMaterializeDim", "SUBSET"));

                newDimSpec->AlterDataset(matDim);
              }
            }

            for (auto entry : subtar->GetDataSets()) {
              DatasetPtr dataset;
              if (storageManager->Filter(entry.second, filter, dataset) !=
                SAVIME_SUCCESS)
                throw std::runtime_error(ERROR_MSG("Filter", "SUBSET"));

              newSubtar->AddDataSet(entry.first, dataset);
            }

            std::sort(dimensionsSpecs.begin(), dimensionsSpecs.end(), compareAdj);
            for (DimSpecPtr spec : dimensionsSpecs) {
              bool isPosterior = false;
              savime_size_t skew = 1;
              savime_size_t adjacency = 1;

              for (DimSpecPtr innerSpec : dimensionsSpecs) {
                if (isPosterior)
                  adjacency *= innerSpec->GetFilledLength();

                if (!spec->GetDimension()->GetName().compare(
                  innerSpec->GetDimension()->GetName())) {
                  isPosterior = true;
                }

                if (isPosterior)
                  skew *= innerSpec->GetFilledLength();
              } // end for

              spec->AlterSkew(skew);
              spec->AlterAdjacency(adjacency);

            }    // end for
          }      // end if
          else { // if totally within the range
            for (auto entry : subtar->GetDataSets()) {
              if (outputTAR->GetDataElement(entry.first) != nullptr)
                newSubtar->AddDataSet(entry.first, entry.second);
            } // end for

            for (auto entry : subtar->GetDimSpecs()) {
              newSubtar->AddDimensionsSpecification(entry.second);
            } // end for
          }   // end else
        }

        break;

      } // END WHILE

      if (subtar == nullptr)
        break;

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
    outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
    generator->TestAndDisposeSubtar(foundSubtarsIndexes[i]);
    outputGenerator->SetSubtarsIndexMap(subtarIndex + i,
                                        foundSubtarsIndexes[i]);
  }

  return SAVIME_SUCCESS;
}

int logical(SubTARIndex subtarIndex, OperationPtr operation,
            ConfigurationManagerPtr configurationManager,
            QueryDataManagerPtr queryDataManager,
            MetadataManagerPtr metadataManager,
            StorageManagerPtr storageManager, EnginePtr engine) {

  OMP_EXCEPTION_ENV()
  DECLARE_TARS(inputTAR, outputTAR);
  ParameterPtr logicalOperation = operation->GetParametersByName(OP);
  ParameterPtr operand1 = nullptr, operand2 = nullptr;
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);

  auto params = operation->GetParameters();
  if (params.size() == 4) {
    operand2 = params.back();
    params.pop_back();
    operand1 = params.back();
  } else {
    operand1 = operation->GetParameters().back();
  }

  TARGeneratorPtr generator, generatorOp1 = nullptr, generatorOp2 = nullptr;
  SET_GENERATOR(generator, inputTAR->GetName());
  SET_GENERATOR(generatorOp1, operand1->tar->GetName());
  if (operand2 != nullptr)
    SET_GENERATOR(generatorOp2, operand2->tar->GetName());

  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()
      SubtarPtr subtar, subtarOp1, subtarOp2;
      subtar = generator->GetSubtar(subtarIndex + i);
      subtarOp1 = generatorOp1->GetSubtar(subtarIndex + i);

      if (generatorOp2 != nullptr)
        subtarOp2 = generatorOp2->GetSubtar(subtarIndex + i);

      if (subtar == nullptr)
        break;

      SubtarPtr newSubtar = SubtarPtr(new Subtar);
      newSubtar->SetTAR(outputTAR);

      int64_t totalLength = subtar->GetFilledLength();
      for (auto entry : subtar->GetDimSpecs()) {
        newSubtar->AddDimensionsSpecification(entry.second);
      }

      // for (auto entry : subtar->GetDataSets()) {
      // newSubtar->AddDataSet(entry.first, entry.second);
      //}

      DatasetPtr filterDataset;
      if (!logicalOperation->literal_str.compare(_AND)) {
        auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
        auto dsOp2 = subtarOp2->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);

        if (storageManager->And(dsOp1, dsOp2, filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("And", "LOGICAL"));
      } else if (!logicalOperation->literal_str.compare(_OR)) {
        auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);
        auto dsOp2 = subtarOp2->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);

        if (storageManager->Or(dsOp1, dsOp2, filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Or", "LOGICAL"));
      } else if (!logicalOperation->literal_str.compare(_NOT)) {
        auto dsOp1 = subtarOp1->GetDataSetFor(DEFAULT_MASK_ATTRIBUTE);

        if (storageManager->Not(dsOp1, filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Not", "LOGICAL"));
      }

      newSubtar->AddDataSet(DEFAULT_MASK_ATTRIBUTE, filterDataset);

      auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
        ->GetGenerators()[outputTAR->GetName()];
      outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      generator->TestAndDisposeSubtar(subtarIndex + i);
      generatorOp1->TestAndDisposeSubtar(subtarIndex + i);

      if (generatorOp2 != nullptr)
        generatorOp2->TestAndDisposeSubtar(subtarIndex + i);

    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}

int comparison(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) {

  OMP_EXCEPTION_ENV()
  DECLARE_TARS(inputTAR, outputTAR);
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);
  ParameterPtr comparisonOperation = operation->GetParametersByName(OP);

  auto params = operation->GetParameters();
  ParameterPtr operand2 = params.back();
  params.pop_back();
  ParameterPtr operand1 = params.back();

  DECLARE_GENERATOR(generator, inputTAR->GetName());

  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()
      auto subtar = generator->GetSubtar(subtarIndex + i);
      if (subtar == nullptr)
        break;

      SubtarPtr newSubtar = SubtarPtr(new Subtar);
      DatasetPtr newDataset;

      newSubtar->SetTAR(outputTAR);

      int64_t totalLength = subtar->GetFilledLength();

      for (auto entry : subtar->GetDimSpecs()) {
        newSubtar->AddDimensionsSpecification(entry.second);
      }

      // for (auto entry : subtar->GetDataSets()) {
      //  newSubtar->AddDataSet(entry.first, entry.second);
      //}

      DatasetPtr filterDataset;
      if (operand1->type == LITERAL_DOUBLE_PARAM &&
        operand2->type == LITERAL_DOUBLE_PARAM) {
        throw std::runtime_error("Constant comparators not supported.");
      } else if (operand1->type == LITERAL_STRING_PARAM &&
        operand2->type == LITERAL_DOUBLE_PARAM) {
        if (inputTAR->GetDataElement(operand1->literal_str)->GetType() ==
          DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(operand1->literal_str);
          if (storageManager->ComparisonDim(
            comparisonOperation->literal_str, dimSpecs, totalLength,
            operand2->literal_dbl, filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        } else {
          auto dataset = subtar->GetDataSetFor(operand1->literal_str);
          if (storageManager->Comparison(comparisonOperation->literal_str,
                                         dataset, operand2->literal_dbl,
                                         filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        }

      } else if (operand1->type == LITERAL_DOUBLE_PARAM &&
        operand2->type == LITERAL_STRING_PARAM) {
        if (inputTAR->GetDataElement(operand2->literal_str)->GetType() ==
          DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(operand2->literal_str);
          if (storageManager->ComparisonDim(
            comparisonOperation->literal_str, dimSpecs, totalLength,
            operand1->literal_dbl, filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        } else {
          auto dataset = subtar->GetDataSetFor(operand2->literal_str);
          if (storageManager->Comparison(comparisonOperation->literal_str,
                                         dataset, operand1->literal_dbl,
                                         filterDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("ComparisonDim", "COMPARISON"));
        }
      } else if (operand1->type == LITERAL_STRING_PARAM &&
        operand2->type == LITERAL_STRING_PARAM) {
        DatasetPtr dsOperand1, dsOperand2;
        if (inputTAR->GetDataElement(operand1->literal_str)->GetType() ==
          DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(operand1->literal_str);
          if (storageManager->MaterializeDim(dimSpecs, totalLength, dsOperand1) !=
            SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("MaterializeDim", "COMPARISON"));
        } else {
          dsOperand1 = subtar->GetDataSetFor(operand1->literal_str);
        }

        if (inputTAR->GetDataElement(operand2->literal_str)->GetType() ==
          DIMENSION_SCHEMA_ELEMENT) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(operand2->literal_str);
          if (storageManager->MaterializeDim(dimSpecs, totalLength, dsOperand2) !=
            SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("MaterializeDim", "COMPARISON"));
        } else {
          dsOperand2 = subtar->GetDataSetFor(operand2->literal_str);
        }

        if (storageManager->Comparison(comparisonOperation->literal_str,
                                       dsOperand1, dsOperand2,
                                       filterDataset) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Comparison", "COMPARISON"));
      }

      filterDataset->BitMask()->resize(totalLength);
      newSubtar->AddDataSet(DEFAULT_MASK_ATTRIBUTE, filterDataset);

      auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
        ->GetGenerators()[outputTAR->GetName()];
      outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      generator->TestAndDisposeSubtar(subtarIndex + i);

    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}

int arithmetic(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) {

  OMP_EXCEPTION_ENV()
  DECLARE_TARS(inputTAR, outputTAR);
  ParameterPtr newMember = operation->GetParametersByName(NEW_MEMBER);
  ParameterPtr op = operation->GetParametersByName(OP);
  auto numSubtars = configurationManager->GetIntValue(MAX_PARA_SUBTARS);

  DECLARE_GENERATOR(generator, inputTAR->GetName());

  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {
    TRY()
      auto subtar = generator->GetSubtar(subtarIndex + i);

      if (subtar == nullptr)
        break;

      SubtarPtr newSubtar = SubtarPtr(new Subtar);
      DatasetPtr newDataset;

      newSubtar->SetTAR(outputTAR);

      int64_t totalLength = subtar->GetFilledLength();

      for (auto entry : subtar->GetDimSpecs()) {
        newSubtar->AddDimensionsSpecification(entry.second);
      }

      for (auto entry : subtar->GetDataSets()) {
        if (outputTAR->HasDataElement(entry.first))
          newSubtar->AddDataSet(entry.first, entry.second);
      }

      ParameterPtr operand0 = operation->GetParametersByName(OPERAND(0));
      ParameterPtr operand1 = operation->GetParametersByName(OPERAND(1));

      if (operand0->type == LITERAL_DOUBLE_PARAM) {
        DatasetPtr stretched;
        Literal literal;
        literal.Simplify(operand0->literal_dbl);
        DatasetPtr constantDs = storageManager->Create(
          literal.type, operand0->literal_dbl, 1, operand0->literal_dbl, 1);

        if (constantDs == nullptr)
          throw std::runtime_error("Could not create dataset.");

        storageManager->Stretch(constantDs, 1, totalLength, 1, stretched);

        if (operand1 == nullptr) {
          if (storageManager->Apply(op->literal_str, stretched, Literal(0.0),
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        } else if (operand1->type == LITERAL_DOUBLE_PARAM) {
          Literal literalOp1;
          literalOp1.Simplify(operand1->literal_dbl);

          if (storageManager->Apply(op->literal_str, stretched, literalOp1,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        } else if (operand1->type == LITERAL_STRING_PARAM) {
          auto dataset = subtar->GetDataSetFor(operand1->literal_str);

          if (!dataset) {
            auto dimSpecs =
              subtar->GetDimensionSpecificationFor(operand1->literal_str);

            if (storageManager->MaterializeDim(dimSpecs, totalLength, dataset) !=
              SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
          }

          if (storageManager->Apply(op->literal_str, dataset, literal,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        }
      }

      if (operand0->type == LITERAL_STRING_PARAM) {
        auto dataset = subtar->GetDataSetFor(operand0->literal_str);

        if (!dataset) {
          auto dimSpecs =
            subtar->GetDimensionSpecificationFor(operand0->literal_str);

          if (storageManager->MaterializeDim(dimSpecs, totalLength, dataset) !=
            SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        }

        if (operand1 == nullptr) {
          double operand = 0;

          if (storageManager->Apply(op->literal_str, dataset, operand,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        } else if (operand1->type == LITERAL_DOUBLE_PARAM) {
          Literal literal;
          literal.Simplify(operand1->literal_dbl);

          if (storageManager->Apply(op->literal_str, dataset, literal,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));

        } else if (operand1->type == LITERAL_STRING_PARAM) {
          auto dataset1 = subtar->GetDataSetFor(operand1->literal_str);
          if (!dataset1) {
            auto dimSpecs =
              subtar->GetDimensionSpecificationFor(operand1->literal_str);

            if (storageManager->MaterializeDim(dimSpecs, totalLength, dataset1) !=
              SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("MaterializeDim", "ARITHMETIC"));
          }

          if (storageManager->Apply(op->literal_str, dataset, dataset1,
                                    newDataset) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("apply", "ARITHMETIC"));
        }
      }

      newSubtar->AddDataSet(newMember->literal_str, newDataset);
      auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
        ->GetGenerators()[outputTAR->GetName()];

      outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      generator->TestAndDisposeSubtar(subtarIndex + i);
    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}

int cross_join(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine) {

  auto leftTAR = operation->GetParametersByName(OPERAND(0))->tar;
  auto rightTAR = operation->GetParametersByName(OPERAND(1))->tar;
  assert(leftTAR != nullptr && rightTAR != nullptr);
  TARPtr outputTAR = operation->GetResultingTAR();
  auto numSubtars = configurationManager->GetIntValue(MAX_PARA_SUBTARS);

  bool freeBufferedSubtars =
    configurationManager->GetBooleanValue(FREE_BUFFERED_SUBTARS);

  DECLARE_GENERATOR(leftGenerator, leftTAR->GetName());
  DECLARE_GENERATOR(rightGenerator, rightTAR->GetName());
  DECLARE_GENERATOR(outputGenerator, outputTAR->GetName());

  int32_t leftSubtarIndexes[numSubtars], rightSubtarIndexes[numSubtars];
  SubtarPtr leftSubtars[numSubtars], rightSubtars[numSubtars];

  for (int32_t i = 0; i < numSubtars; i++) {
    rightSubtarIndexes[i] =
      outputGenerator->GetSubtarsIndexMap(1, subtarIndex + i - 1) + 1;
    rightSubtars[i] = rightGenerator->GetSubtar(rightSubtarIndexes[i]);

    if (rightSubtars[i] == nullptr) {
      leftSubtarIndexes[i] =
        outputGenerator->GetSubtarsIndexMap(0, subtarIndex + i - 1) + 1;
      leftGenerator->TestAndDisposeSubtar(leftSubtarIndexes[i] - 1);
      rightSubtarIndexes[i] = 0;
      rightSubtars[i] = rightGenerator->GetSubtar(rightSubtarIndexes[i]);
    } else if (subtarIndex + i > 0) {
      leftSubtarIndexes[i] =
        outputGenerator->GetSubtarsIndexMap(0, subtarIndex + i - 1);
    } else {
      leftSubtarIndexes[i] = 0;
    }

    leftSubtars[i] = leftGenerator->GetSubtar(leftSubtarIndexes[i]);
  }

  OMP_EXCEPTION_ENV()
  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()

      SubtarPtr newSubtar = SubtarPtr(new Subtar);
      int32_t leftSubtarIndex = leftSubtarIndexes[i];
      int32_t rightSubtarIndex = rightSubtarIndexes[i];
      SubtarPtr rightSubtar = rightSubtars[i], leftSubtar = leftSubtars[i];

      if (leftSubtar == nullptr || rightSubtar == nullptr) {
        int32_t lastSubtar =
          outputGenerator->GetSubtarsIndexMap(1, subtarIndex + i - 2);
        if (!freeBufferedSubtars) {
          for (int32_t i = 0; i <= lastSubtar; i++) {
            rightGenerator->TestAndDisposeSubtar(i);
          }
        }
        break;
      }

      int64_t leftSubtarLen = leftSubtar->GetFilledLength();
      int64_t rightSubtarLen = rightSubtar->GetFilledLength();

      for (auto entry : leftSubtar->GetDimSpecs()) {
        string dimName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DimSpecPtr dimspec = entry.second;
        DimSpecPtr newDimspec;

        if (dimspec->GetSpecsType() == ORDERED) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetLowerBound(), dimspec->GetUpperBound(),
            dimspec->GetSkew() * rightSubtarLen,
            dimspec->GetAdjacency() * rightSubtarLen);

        } else if (dimspec->GetSpecsType() == PARTIAL) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetDataset(), dimspec->GetLowerBound(),
            dimspec->GetUpperBound(), dimspec->GetSkew() * rightSubtarLen,
            dimspec->GetAdjacency() * rightSubtarLen);
        } else {
          DatasetPtr matDim;
          if (storageManager->Stretch(dimspec->GetDataset(), leftSubtarLen,
                                      rightSubtarLen, 1,
                                      matDim) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, outputTAR->GetDataElement(dimName)->GetDimension(),
            matDim, dimspec->GetLowerBound(), dimspec->GetUpperBound());
        }

        newSubtar->AddDimensionsSpecification(newDimspec);
      }

      for (auto entry : rightSubtar->GetDimSpecs()) {
        string dimName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DimSpecPtr dimspec = entry.second;
        DimSpecPtr newDimspec;

        if (dimspec->GetSpecsType() == ORDERED) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetLowerBound(), dimspec->GetUpperBound(),
            dimspec->GetSkew(), dimspec->GetAdjacency());

        } else if (dimspec->GetSpecsType() == PARTIAL) {
          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, outputTAR->GetDataElement(dimName)->GetDimension(),
            dimspec->GetDataset(), dimspec->GetLowerBound(),
            dimspec->GetUpperBound(), dimspec->GetSkew(),
            dimspec->GetAdjacency());
        } else {
          DatasetPtr matDim;
          if (storageManager->Stretch(dimspec->GetDataset(), rightSubtarLen, 1,
                                      leftSubtarLen, matDim) != SAVIME_SUCCESS)
            throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

          newDimspec = make_shared<DimensionSpecification>(
            UNSAVED_ID, outputTAR->GetDataElement(dimName)->GetDimension(),
            matDim, dimspec->GetLowerBound(), dimspec->GetUpperBound());
        }

        newSubtar->AddDimensionsSpecification(newDimspec);
      }

      for (auto entry : leftSubtar->GetDataSets()) {
        string attName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (storageManager->Stretch(ds, leftSubtarLen, rightSubtarLen, 1,
                                    joinedDs) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

        newSubtar->AddDataSet(attName, joinedDs);
      }

      for (auto entry : rightSubtar->GetDataSets()) {
        string attName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (storageManager->Stretch(ds, rightSubtarLen, 1, leftSubtarLen,
                                    joinedDs) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Stretch", "CROSS"));

        newSubtar->AddDataSet(attName, joinedDs);
      }

      if (freeBufferedSubtars) {
        rightGenerator->TestAndDisposeSubtar(rightSubtarIndex);
      }

      outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
      outputGenerator->SetSubtarsIndexMap(0, subtarIndex + i, leftSubtarIndex);
      outputGenerator->SetSubtarsIndexMap(1, subtarIndex + i, rightSubtarIndex);
    CATCH()
  }
  RETHROW()

  return SAVIME_SUCCESS;
}

int equijoin(SubTARIndex subtarIndex, OperationPtr operation,
             ConfigurationManagerPtr configurationManager,
             QueryDataManagerPtr queryDataManager,
             MetadataManagerPtr metadataManager,
             StorageManagerPtr storageManager, EnginePtr engine) {
  return SAVIME_SUCCESS;
}

int dimjoin(SubTARIndex subtarIndex, OperationPtr operation,
            ConfigurationManagerPtr configurationManager,
            QueryDataManagerPtr queryDataManager,
            MetadataManagerPtr metadataManager,
            StorageManagerPtr storageManager, EnginePtr engine) {

  CHECK_INTERSECTION();
  GET_INT_CONFIG_VAL(numThreads, MAX_THREADS);
  GET_INT_CONFIG_VAL(workPerThread, WORK_PER_THREAD);
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);
  GET_BOOL_CONFIG_VAL(freeBufferedSubtars, FREE_BUFFERED_SUBTARS);
  DECLARE_DIMJOIN_TARS();

  // Obtaining subtar generator
  DECLARE_GENERATOR(leftGenerator, leftTAR->GetName());
  DECLARE_GENERATOR(rightGenerator, rightTAR->GetName());
  DECLARE_GENERATOR(outputGenerator, outputTAR->GetName());

  DMLMutex mutex;
  SubtarMap subtarMap;
  SubtarIndexList foundSubtarsIndexes;
  int32_t nextUntested =
    outputGenerator->GetSubtarsIndexMap(2, subtarIndex - 1) + 1;
  map<string, string> leftDims, rightDims;
  createDimensionMappings(operation, leftDims, rightDims);

  OMP_EXCEPTION_ENV()
  SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
  for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

    TRY()

      SubtarPtr newSubtar = SubtarPtr(new Subtar);
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

        if (!checkIntersection(outputTAR, leftSubtar, rightSubtar, leftDims,
                               ranges, storageManager)) {
          continue;
        } else {

          DatasetPtr matchLeftDim, matchRightDim;
          leftSubtarLen = leftSubtar->GetFilledLength();
          rightSubtarLen = rightSubtar->GetFilledLength();

          createSingularJoinDims(outputTAR, leftSubtar, rightSubtar, leftDims,
                                 storageManager, numThreads, workPerThread,
                                 matchLeftDim, matchRightDim);

          if (storageManager->Match(matchLeftDim, matchRightDim, finalLeftMap,
                                    finalRightMap) != SAVIME_SUCCESS) {
            throw std::runtime_error(ERROR_MSG("MatchDim", "DIMJOIN"));
          }

          /*If there are matches between subtars, process. Otherwise, skip.*/
          if (finalLeftMap == NULL || finalRightMap == NULL) {
            continue;
          } else {
            finalLeftMap->HasIndexes() = true;
            finalRightMap->HasIndexes() = true;
            break;
          }
        }
      }

      if (finalSubtar) {
        newSubtar = NULL;
        break;
      }

      for (auto entry : leftSubtar->GetDataSets()) {
        string attName = LEFT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (storageManager->Filter(ds, finalLeftMap, ds) != SAVIME_SUCCESS)
          throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));

        newSubtar->AddDataSet(attName, ds);
      }

      for (auto entry : rightSubtar->GetDataSets()) {
        string attName = RIGHT_DATAELEMENT_PREFIX + entry.first;
        DatasetPtr ds = entry.second;
        DatasetPtr joinedDs;

        if (storageManager->Filter(ds, finalRightMap, ds) != SAVIME_SUCCESS)
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
          outputTAR->GetDataElement(dimName)->GetDimension());

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
            outputTAR->GetDataElement(dimName)->GetDimension());
          hasTotalDims = hasTotalDims || (dimspec->GetSpecsType() == TOTAL);
          rightDimSpecs.push_back(newDimspec);
        }
      }

      if (hasTotalDims || hasPartialJoinDims) {
        for (DimSpecPtr dimSpecs : leftDimSpecs) {
          string originalDimName =
            dimSpecs->GetDimension()->GetName().substr(5, string::npos);
          DimensionType dimType = dimSpecs->GetDimension()->GetDimensionType();
          DatasetPtr ds, joinedDs, joinedDsReal;
          DimSpecPtr originalDimspecs =
            leftSubtar->GetDimensionSpecificationFor(originalDimName);

          if (storageManager->PartiatMaterializeDim(
            finalLeftMap, originalDimspecs, leftSubtarLen, joinedDs,
            joinedDsReal) != SAVIME_SUCCESS)
            throw std::runtime_error(
              ERROR_MSG("PartiatMaterializeDim", "DIMJOIN"));

          if (dimType == EXPLICIT) {
            if (storageManager->Logical2Real(dimSpecs->GetDimension(),
                                             originalDimspecs, joinedDs,
                                             joinedDs) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
          }

          DimSpecPtr newDimSpecs = make_shared<DimensionSpecification>(
            UNSAVED_ID, dimSpecs->GetDimension(), joinedDs,
            dimSpecs->GetLowerBound(), dimSpecs->GetUpperBound());

          newSubtar->AddDimensionsSpecification(newDimSpecs);
        }

        for (DimSpecPtr dimSpecs : rightDimSpecs) {
          string originalDimName =
            dimSpecs->GetDimension()->GetName().substr(6, string::npos);
          DimensionType dimType = dimSpecs->GetDimension()->GetDimensionType();
          DatasetPtr ds, strechedDimDs, joinedDs, joinedDsReal;
          DimSpecPtr originalDimspecs =
            rightSubtar->GetDimensionSpecificationFor(originalDimName);

          if (storageManager->PartiatMaterializeDim(
            finalRightMap, originalDimspecs, rightSubtarLen, joinedDs,
            joinedDsReal) != SAVIME_SUCCESS)
            throw std::runtime_error(
              ERROR_MSG("PartiatMaterializeDim", "DIMJOIN"));

          if (dimType == EXPLICIT) {
            if (storageManager->Logical2Real(dimSpecs->GetDimension(),
                                             originalDimspecs, joinedDs,
                                             joinedDs) != SAVIME_SUCCESS)
              throw std::runtime_error(ERROR_MSG("Filter", "DIMJOIN"));
          }

          DimSpecPtr newDimSpecs = make_shared<DimensionSpecification>(
            UNSAVED_ID, dimSpecs->GetDimension(), joinedDs,
            dimSpecs->GetLowerBound(), dimSpecs->GetUpperBound());

          newSubtar->AddDimensionsSpecification(newDimSpecs);
        }

      } else {

        leftDimSpecs.sort(compareDimSpecsByAdj);
        rightDimSpecs.sort(compareDimSpecsByAdj);
        while (rightDimSpecs.size()) {
          leftDimSpecs.push_back(rightDimSpecs.front());
          rightDimSpecs.pop_front();
        }

        for (DimSpecPtr spec : leftDimSpecs) {

          bool isPosterior = false;
          savime_size_t skew = 1;
          savime_size_t adjacency = 1;

          for (DimSpecPtr innerSpec : leftDimSpecs) {
            if (isPosterior)
              adjacency *= innerSpec->GetFilledLength();

            if (!spec->GetDimension()->GetName().compare(
              innerSpec->GetDimension()->GetName())) {
              isPosterior = true;
            }

            if (isPosterior)
              skew *= innerSpec->GetFilledLength();
          }
          spec->AlterSkew(skew);
          spec->AlterAdjacency(adjacency);
          newSubtar->AddDimensionsSpecification(spec);
        }
      }

      if (freeBufferedSubtars) {
        rightGenerator->TestAndDisposeSubtar(rightSubtarIndex);
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
    outputGenerator->AddSubtar(subtarIndex + i, newSubtar);
    outputGenerator->SetSubtarsIndexMap(2, subtarIndex + i,
                                        foundSubtarsIndexes[i]);
  }

  return SAVIME_SUCCESS;
}

int slice(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine) {
  return SAVIME_FAILURE;
}

int aggregate(SubTARIndex subtarIndex, OperationPtr operation,
              ConfigurationManagerPtr configurationManager,
              QueryDataManagerPtr queryDataManager,
              MetadataManagerPtr metadataManager,
              StorageManagerPtr storageManager, EnginePtr engine) {

  OMP_EXCEPTION_ENV()
  DECLARE_TARS(inputTAR, outputTAR);
  DECLARE_GENERATOR(generator, inputTAR->GetName());
  DECLARE_GENERATOR(outputGenerator, outputTAR->GetName());
  GET_INT_CONFIG_VAL(numCores, MAX_THREADS);
  GET_INT_CONFIG_VAL(workPerThread, WORK_PER_THREAD);
  GET_INT_CONFIG_VAL(numSubtars, MAX_PARA_SUBTARS);

  SubtarPtr newSubtar = SubtarPtr(new Subtar);
  newSubtar->SetTAR(outputTAR);
  unordered_map<string, AggregateBufferPtr> globalAggBuffer, globalAuxBuffer;
  vector<AggregateConfigurationPtr> aggConfigs(numSubtars);

  AggregateConfigurationPtr aggConfig =
    createConfiguration(inputTAR, operation);
  setAggregateMode(aggConfig, configurationManager);
  setSavimeDatasets(aggConfig, storageManager, numCores, workPerThread);
  setGlobalAggregatorBuffers(aggConfig, globalAggBuffer, globalAuxBuffer);

  for (int32_t i = 0; i < numSubtars; i++) {
    aggConfigs[i] = aggConfig->Clone();
  }
  auto outputLen = aggConfig->GetTotalLength();

  vector<SubtarPtr> subtar(numSubtars);
  vector<int64_t> currentSubtars(numSubtars);
  unordered_map<string, vector<AggregateBufferPtr>> aggregateBuffers(
    numSubtars);
  unordered_map<string, vector<AggregateBufferPtr>> auxAggregateBuffers(
    numSubtars);
  for (auto func : aggConfig->functions) {
    aggregateBuffers[func->attribName] = vector<AggregateBufferPtr>(numSubtars);
    auxAggregateBuffers[func->attribName] =
      vector<AggregateBufferPtr>(numSubtars);
  }

  if (subtarIndex != FIRST_SUBTAR) {
    return SAVIME_SUCCESS;
  }

  int32_t currentSubtar = subtarIndex;
  bool hasSubtars = true;

  while (true) {

    for (int32_t i = 0; i < numSubtars; i++) {
      currentSubtars[i] = currentSubtar;
      subtar[i] = generator->GetSubtar(currentSubtar);
      currentSubtar++;
    }

    SET_SUBTARS_THREADS(numSubtars);
#pragma omp parallel
    for (int32_t i = SUB_THREADS_FIRST(); i <= SUB_THREADS_LAST(); i++) {

      TRY()
        if (subtar[i] == nullptr) {
          hasSubtars = false;
          for (auto func : aggConfigs[i]->functions) {
            aggregateBuffers[func->attribName][i] = nullptr;
            auxAggregateBuffers[func->attribName][i] = nullptr;
          }
          break;
        }

        AggregateBufferPtr localAggBuffer, localAuxBuffer;
        for (auto func : aggConfigs[i]->functions) {
          setLocalAggregatorBuffers(aggConfigs[i], func, localAggBuffer,
                                    localAuxBuffer, outputLen);
          aggregateBuffers[func->attribName][i] = localAggBuffer;
          auxAggregateBuffers[func->attribName][i] = localAuxBuffer;
        }

        int64_t subtarLen = subtar[i]->GetFilledLength();
        createLogicalIndexesBuffer(aggConfigs[i], storageManager, subtar[i]);
        setInputBuffers(aggConfigs[i], storageManager, subtar[i]);

        for (auto func : aggConfigs[i]->functions) {
          auto type = inputTAR->GetDataElement(func->paramName)->GetDataType();
          AbstractAggregateEnginePtr engine = buildAggregateEngine(
            aggConfigs[i], func, type, subtarLen, numCores, workPerThread);

          engine->Run(localAggBuffer, localAuxBuffer, outputLen);
        }

        generator->TestAndDisposeSubtar(currentSubtars[i]);
      CATCH()
    }
    RETHROW()

    /*Reducing aggregations. */
    for (auto func : aggConfigs[0]->functions) {

      auto type = inputTAR->GetDataElement(func->paramName)->GetDataType();
      AbstractAggregateEnginePtr engine = buildAggregateEngine(
        aggConfigs[0], func, type, 0, numCores, workPerThread);

      engine->Reduce(globalAggBuffer[func->attribName],
                     globalAuxBuffer[func->attribName],
                     aggregateBuffers[func->attribName],
                     auxAggregateBuffers[func->attribName], outputLen);
    }

    if (!hasSubtars)
      break;
  }

  for (auto func : aggConfig->functions) {

    auto type = inputTAR->GetDataElement(func->paramName)->GetDataType();
    AbstractAggregateEnginePtr engine = buildAggregateEngine(
      aggConfigs[0], func, type, 0, numCores, workPerThread);

    engine->Finalize(globalAggBuffer[func->attribName],
                     globalAuxBuffer[func->attribName], outputLen);
  }

  if (aggConfig->mode == BUFFERED) {

    for (auto &dim : outputTAR->GetDimensions()) {
      DimSpecPtr newDimSpec = DimSpecPtr(new DimensionSpecification(
        UNSAVED_ID, dim, 0, dim->CurrentUpperBound(),
        aggConfig->skew[aggConfig->name2index[dim->GetName()]],
        aggConfig->adj[aggConfig->name2index[dim->GetName()]]));

      newSubtar->AddDimensionsSpecification(newDimSpec);
    }

    for (auto entry : aggConfig->datasets) {
      newSubtar->AddDataSet(entry.first, entry.second);
    }

    newSubtar = filterAggregatedSubtar(
      newSubtar, globalAggBuffer.begin()->second->Bitmask(), numCores,
      workPerThread, storageManager);
  } else {

    auto aggBuffer = globalAggBuffer.begin()->second;
    int64_t outputLen = aggBuffer->getIndexesMap()->size(), i = 0;
    unordered_map<int32_t, DatasetPtr> dimensionDatasets;
    unordered_map<int32_t, DatasetHandlerPtr> dimensionHandlers;
    unordered_map<int32_t, RealIndex *> dimensionBuffers;
    unordered_map<string, DatasetPtr> attributeDatasets;
    unordered_map<string, DatasetHandlerPtr> attributeHandlers;
    unordered_map<string, double *> attributeBuffers;

    /*Creating datasets for new total dimensions*/
    for (DimensionPtr dim : outputTAR->GetDimensions()) {
      auto ds = storageManager->Create(REAL_INDEX, outputLen);
      dimensionDatasets[aggConfig->name2index[dim->GetName()]] = ds;
      auto handler = storageManager->GetHandler(ds);
      dimensionHandlers[aggConfig->name2index[dim->GetName()]] = handler;
      RealIndex *buffer = (RealIndex *)handler->GetBuffer();
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
    for (DimensionPtr dim : outputTAR->GetDimensions()) {

      DimSpecPtr newDimSpec = make_shared<DimensionSpecification>(
        UNSAVED_ID, dim,
        dimensionDatasets[aggConfig->name2index[dim->GetName()]], 0,
        dim->CurrentUpperBound());

      if (dim->GetDimensionType() == IMPLICIT) {
        DatasetPtr logicalIndexesDataset;
        storageManager->Real2Logical(
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
    for (auto func : aggConfig->functions) {
      auto ds = storageManager->Create(DOUBLE, outputLen);
      attributeDatasets[func->attribName] = ds;
      auto handler = storageManager->GetHandler(ds);
      attributeHandlers[func->attribName] = handler;
      double *buffer = (double *)handler->GetBuffer();
      attributeBuffers[func->attribName] = buffer;
    }

    i = 0;
    for (auto entry : aggBuffer->getMap()) {
      for (auto func : aggConfig->functions) {
        attributeBuffers[func->attribName][i] = entry.second;
      }
      i++;
    }

    for (auto func : aggConfig->functions) {
      newSubtar->AddDataSet(func->attribName,
                            attributeDatasets[func->attribName]);
      attributeHandlers[func->attribName]->Close();
    }
  }

  outputGenerator->AddSubtar(0, newSubtar);

  for (auto h : aggConfig->handlersToClose)
    h->Close();

  return SAVIME_SUCCESS;
}

int split(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine) {
  /* try {
     ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);

     TARPtr inputTAR = inputTarParam->tar;
     assert(inputTAR != nullptr);

     TARPtr outputTAR = operation->GetResultingTAR();
     assert(outputTAR != nullptr);

     // Checking if iterator mode is enabled
     bool iteratorModeEnabled =
         configurationManager->GetBooleanValue(ITERATOR_MODE_ENABLED);

     // Obtaining subtar generator
     auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
                          ->GetGenerators()[inputTAR->GetName()];
     auto outputGenerator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
                                ->GetGenerators()[outputTAR->GetName()];

     while (true) {
       SubtarPtr subtar;
       int32_t lastSubtar = 0, currentSubtar = 0;
       currentSubtar = outputGenerator->GetSubtarsIndexMap(subtarIndex - 1) + 1;

       subtar = generator->GetSubtar(currentSubtar);
       if (subtar == nullptr)
         return SAVIME_SUCCESS;
       int64_t totalLength = subtar->GetFilledLength();

       // outputGenerator->AddSubtar(subtarIndex, newSubtar);
       DimSpecPtr splitDimension = nullptr;
       int64_t greatestAdj = 0;
       bool hasTotalDimension = false;

       for (auto entry : subtar->GetDimSpecs()) {
         DimSpecPtr dimSpecs = entry.second;
         if (dimSpecs->GetFilledLength() > 1) {
           if (dimSpecs->adjacency > greatestAdj) {
             splitDimension = dimSpecs;
             greatestAdj = dimSpecs->adjacency;
           }
         }

         if (dimSpecs->type == TOTAL)
           hasTotalDimension = true;
       }

       if (splitDimension != nullptr) {

         int64_t maxSplitLength =
             configurationManager->GetLongValue(MAX_SPLIT_LEN);

         if (splitDimension->GetFilledLength() > maxSplitLength)
           throw std::runtime_error(
               "Leftmost dimension is too large for split operation. Consider"
               "loading smaller subtars.");

         if (!hasTotalDimension) {
           int64_t subtarsNo = splitDimension->GetFilledLength();
           vector<SubtarPtr> subtars;
           map<string, vector<DatasetPtr>> datasets;

           for (auto entry : subtar->GetDataSets()) {
             vector<DatasetPtr> _datasets;
             if (storageManager->Split(entry.second, totalLength, subtarsNo,
                                       _datasets) != SAVIME_SUCCESS)
               throw std::runtime_error(ERROR_MSG("split", "SPLIT"));

             datasets[entry.first] = _datasets;
           }

           if (splitDimension->type == ORDERED) {
             for (int64_t index = splitDimension->lower_bound;
                  index <= splitDimension->upper_bound; index++) {
               SubtarPtr newSubtar = SubtarPtr(new Subtar());
               DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification());
               dimSpecs->adjacency = splitDimension->adjacency;
               dimSpecs->dataset = splitDimension->dataset;
               dimSpecs->dimension = outputTAR->GetDataElement(
                   splitDimension->dimension->GetName());

               dimSpecs->lower_bound = index;
               dimSpecs->upper_bound = index;
               dimSpecs->skew = splitDimension->skew / dimSpecs->adjacency;
               dimSpecs->type = splitDimension->type;
               dimSpecs->materialized = nullptr;
               newSubtar->AddDimensionsSpecification(dimSpecs);

               for (auto entry : subtar->GetDimSpecs()) {
                 if (splitDimension->dimension->GetName() != entry.first) {
                   DimSpecPtr dimSpecs =
                       DimSpecPtr(new DimensionSpecification());

                   if (entry.second->GetFilledLength() == 1 &&
                       entry.second->adjacency > splitDimension->adjacency) {
                     dimSpecs->adjacency =
                         entry.second->adjacency /
   splitDimension->GetFilledLength();
                   } else {
                     dimSpecs->adjacency = entry.second->adjacency;
                   }

                   dimSpecs->dataset = entry.second->dataset;
                   dimSpecs->dimension = outputTAR->GetDataElement(
                       entry.second->dimension->GetName());
                   dimSpecs->lower_bound = entry.second->lower_bound;
                   dimSpecs->upper_bound = entry.second->upper_bound;
                   dimSpecs->skew = entry.second->skew;
                   dimSpecs->type = entry.second->type;
                   dimSpecs->materialized = NULL;
                   newSubtar->AddDimensionsSpecification(dimSpecs);
                 }
               }

               for (auto entry : subtar->GetDataSets()) {
                 DatasetPtr ds =
                     datasets[entry.first][index - splitDimension->lower_bound];
                 newSubtar->AddDataSet(entry.first, ds);
               }

               subtars.push_back(newSubtar);
             }
           } else {
             vector<DatasetPtr> _partialDimDatasets;
             if (storageManager->Split(splitDimension->dataset, totalLength,
                                       subtarsNo,
                                       _partialDimDatasets) != SAVIME_SUCCESS)
               throw std::runtime_error(ERROR_MSG("split", "SPLIT"));

             for (int64_t index = splitDimension->lower_bound;
                  index <= splitDimension->upper_bound; index++) {
               SubtarPtr newSubtar = SubtarPtr(new Subtar());
               DimSpecPtr dimSpecs = DimSpecPtr(new DimensionSpecification());
               dimSpecs->adjacency = splitDimension->adjacency;
               dimSpecs->dataset =
                   _partialDimDatasets[index - dimSpecs->lower_bound];
               dimSpecs->dimension = outputTAR->GetDataElement(
                   splitDimension->dimension->GetName());

               dimSpecs->lower_bound = index;
               dimSpecs->upper_bound = index;
               dimSpecs->skew = splitDimension->skew / dimSpecs->adjacency;
               dimSpecs->type = splitDimension->type;
               dimSpecs->materialized = nullptr;
               newSubtar->AddDimensionsSpecification(dimSpecs);

               for (auto entry : subtar->GetDimSpecs()) {
                 if (splitDimension->dimension->GetName() != entry.first) {
                   DimSpecPtr dimSpecs =
                       DimSpecPtr(new DimensionSpecification());
                   if (entry.second->GetFilledLength() == 1 &&
                       entry.second->adjacency > splitDimension->adjacency) {
                     dimSpecs->adjacency =
                         entry.second->adjacency /
   splitDimension->GetFilledLength();
                   } else {
                     dimSpecs->adjacency = entry.second->adjacency;
                   }
                   dimSpecs->dataset = entry.second->dataset;
                   dimSpecs->dimension = outputTAR->GetDataElement(
                       entry.second->dimension->GetName());
                   dimSpecs->lower_bound = entry.second->lower_bound;
                   dimSpecs->upper_bound = entry.second->upper_bound;
                   dimSpecs->skew = entry.second->skew;
                   dimSpecs->type = entry.second->type;
                   dimSpecs->materialized = nullptr;
                   newSubtar->AddDimensionsSpecification(dimSpecs);
                 }
               }

               for (auto entry : subtar->GetDataSets()) {
                 DatasetPtr ds =
                     datasets[entry.first][index - splitDimension->lower_bound];
                 newSubtar->AddDataSet(entry.first, ds);
               }

               subtars.push_back(newSubtar);
             }
           }

           for (auto s : subtars) {
             outputGenerator->AddSubtar(subtarIndex++, s);
           }

           subtarIndex--;
         } else {
           outputGenerator->AddSubtar(subtarIndex, subtar);
         }
       } else {
         outputGenerator->AddSubtar(subtarIndex, subtar);
       }

       generator->TestAndDisposeSubtar(subtarIndex);
       outputGenerator->SetSubtarsIndexMap(subtarIndex, currentSubtar);

       if (iteratorModeEnabled)
         break;
       subtarIndex++;
     }
   } catch (std::exception &e) {
     string error = queryDataManager->GetErrorResponse();
     queryDataManager->SetErrorResponseText(e.what());
     return SAVIME_FAILURE;
   }
 */
  return SAVIME_SUCCESS;
}

int store(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine) {
  const char *error_store =
    "Invalid parameters for store operation. Expected STORE(tar, tar_name),";
  try {
    if (operation->GetParameters().size() != 3)
      throw std::runtime_error(error_store);

    ParameterPtr inputTarParam = operation->GetParametersByName(OPERAND(0));
    ParameterPtr newNameParam = operation->GetParametersByName(OPERAND(1));
    TARSPtr defaultTARS = metadataManager->GetTARS(
      configurationManager->GetIntValue(DEFAULT_TARS));

    if (inputTarParam == nullptr || newNameParam == nullptr)
      throw std::runtime_error(error_store);

    if (inputTarParam->tar == nullptr)
      throw std::runtime_error(error_store);

    TARPtr inputTAR = inputTarParam->tar;
    TARPtr outputTAR = inputTAR->Clone(false, false, false);
    string newName = newNameParam->literal_str;
    newName.erase(std::remove(newName.begin(), newName.end(), '"'),
                  newName.end());

    if (!metadataManager->ValidateIdentifier(newName, "tar"))
      throw std::runtime_error("Invalid identifier for TAR: " + newName);

    if (metadataManager->GetTARByName(defaultTARS, newName) != nullptr)
      throw std::runtime_error("TAR " + newName + " already exists.");

    outputTAR->AlterTAR(UNSAVED_ID, newName);

    if (metadataManager->SaveTAR(defaultTARS, outputTAR) != SAVIME_SUCCESS)
      throw std::runtime_error("Could not save TAR: " + newName);

    // Obtaining subtar generator
    auto generator = (std::dynamic_pointer_cast<DefaultEngine>(engine))
      ->GetGenerators()[inputTAR->GetName()];
    SubtarPtr subtar;
    int32_t subtarCount = 0;

    while (true) {
      subtar = generator->GetSubtar(subtarCount);
      if (subtar == nullptr)
        break;

      if (metadataManager->SaveSubtar(outputTAR, subtar) != SAVIME_SUCCESS)
        throw std::runtime_error("Could not save subtar.");
      subtarCount++;
    }

    metadataManager->RegisterQuery(queryDataManager->GetQueryText());

  } catch (std::exception &e) {
    throw e;
  }

  return SAVIME_SUCCESS;
}

int user_defined(SubTARIndex subtarIndex, OperationPtr operation,
                 ConfigurationManagerPtr configurationManager,
                 QueryDataManagerPtr queryDataManager,
                 MetadataManagerPtr metadataManager,
                 StorageManagerPtr storageManager, EnginePtr engine) {
#define _CATALYZE "catalyze"
#define _STORE "store"

  if (operation->GetParametersByName(OPERATOR_NAME)->literal_str == _CATALYZE) {
#ifdef CATALYST
    return catalyze(subtarIndex, operation, configurationManager,
                    queryDataManager, metadataManager, storageManager, engine);
#else
    return SAVIME_SUCCESS;
#endif
  } else if (operation->GetParametersByName(OPERATOR_NAME)->literal_str ==
    _STORE) {
    return store(subtarIndex, operation, configurationManager, queryDataManager,
                 metadataManager, storageManager, engine);
    ;
  }
}