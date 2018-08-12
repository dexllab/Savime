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
#ifndef DEFAULT_TEMPLATE_H
#define DEFAULT_TEMPLATE_H

#include "../core/include/util.h"
#include "../core/include/savime_hash.h"
#include "../core/include/query_data_manager.h"
#include "../core/include/storage_manager.h"
#include "../core/include/abstract_storage_manager.h"
#include "default_storage_manager.h"
#include <algorithm>
#include <cmath>
#include <omp.h>

template <class T1, class T2>
class TemplateStorageManager : public AbstractStorageManager {
  StorageManagerPtr _storageManager;
  ConfigurationManagerPtr _configurationManager;
  SystemLoggerPtr _systemLogger;

public:
  TemplateStorageManager(StorageManagerPtr storageManager,
                         ConfigurationManagerPtr configurationManager,
                         SystemLoggerPtr systemLogger) {
    _storageManager = storageManager;
    _configurationManager = configurationManager;
    _systemLogger = systemLogger;
  }

  RealIndex Logical2Real(DimensionPtr dimension, Literal _logicalIndex) {
    T2 logicalIndex;
    GET_LITERAL(logicalIndex, _logicalIndex, T2);

    double DIFF = 0.000001;
    double intpart;
    RealIndex realIndex = INVALID_EXACT_REAL_INDEX;

    if (dimension->GetDimensionType() == IMPLICIT) {
      // If it is in the range
      if (dimension->GetLowerBound() <= logicalIndex &&
        dimension->GetUpperBound() >= logicalIndex) {
        double fRealIndex = 0.0;
        double preamble = 1 / dimension->GetSpacing();

        fRealIndex =
          (logicalIndex * preamble - dimension->GetLowerBound() * preamble);
        double mod = std::modf(fRealIndex, &intpart);

        if (mod < DIFF) {
          // returns lowest index closest to the logical value
          realIndex = (RealIndex)intpart;
        }
      }
    } else if (dimension->GetDimensionType() == EXPLICIT) {

      auto handler = _storageManager->GetHandler(dimension->GetDataset());
      T1 *buffer = (T1 *)handler->GetBuffer();

      if (dimension->GetDataset()->Sorted()) {
        RealIndex first = 0,
          last = dimension->GetDataset()->GetEntryCount() - 1;
        RealIndex middle = (last + first) / 2;

        if (buffer[first] <= logicalIndex && buffer[last] >= logicalIndex) {
          while (first <= last) {
            if (abs(buffer[middle] - logicalIndex) < DIFF) {
              realIndex = middle;
              break;
            } else if (buffer[middle] < logicalIndex) {
              first = middle + 1;
            } else {
              last = middle - 1;
            }
            middle = (first + last) / 2;
          }
        }
      } else {

        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread =
          _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t entryCount = dimension->GetDataset()->GetEntryCount();

        SET_THREADS(entryCount, minWorkPerThread, numCores);
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          if (buffer[i] == logicalIndex) {
            realIndex = i;
          }

          if (realIndex != INVALID_EXACT_REAL_INDEX)
            break;
        }
      }

      handler->Close();
    }

    return realIndex;
  }

  IndexPair Logical2ApproxReal(DimensionPtr dimension, Literal _logicalIndex) {
    T2 logicalIndex;
    GET_LITERAL(logicalIndex, _logicalIndex, T2);

    double DIFF = 0.000001;
    double intpart;
    RealIndex realIndex = INVALID_EXACT_REAL_INDEX;

    if (dimension->GetDimensionType() == IMPLICIT) {
      if (dimension->GetLowerBound() > logicalIndex)
        return {0, 0};

      if (dimension->GetUpperBound() < logicalIndex)
        return {dimension->GetRealUpperBound(), dimension->GetRealUpperBound()};

      double fRealIndex =
        (logicalIndex - dimension->GetLowerBound()) / dimension->GetSpacing();

      std::modf(fRealIndex, &intpart);
      realIndex = (RealIndex)intpart;
      realIndex = std::min(realIndex, dimension->GetRealUpperBound());

      if (realIndex == dimension->GetRealUpperBound() || realIndex == intpart)
        return {realIndex, realIndex};
      else
        return {realIndex, realIndex + 1};

    } else if (dimension->GetDimensionType() == EXPLICIT) {
      int numCores = _configurationManager->GetIntValue(MAX_THREADS);

      auto handler = _storageManager->GetHandler(dimension->GetDataset());
      T1 *buffer = (T1 *)handler->GetBuffer();

      if (dimension->GetDataset()->Sorted()) {
        RealIndex first = 0,
          last = dimension->GetDataset()->GetEntryCount() - 1;
        RealIndex middle = (last + first) / 2;

        if (buffer[first] > logicalIndex) {
          handler->Close();
          return {0, 0};
        }

        if (buffer[last] < logicalIndex) {
          handler->Close();
          return {dimension->GetRealUpperBound(),
                  dimension->GetRealUpperBound()};
        }

        while (first <= last) {
          if (abs(buffer[middle] - logicalIndex) < DIFF) {
            realIndex = middle;
            break;
          } else if (buffer[middle] < logicalIndex) {
            first = middle + 1;
          } else {
            last = middle - 1;
          }
          middle = (first + last) / 2;
        }

        if (realIndex == INVALID_EXACT_REAL_INDEX) {
          realIndex = middle;
        }

        if (logicalIndex < buffer[middle]) {
          handler->Close();
          return {realIndex - 1, realIndex};
        } else if (logicalIndex > buffer[middle]) {
          handler->Close();
          return {realIndex, realIndex + 1};
        } else {
          handler->Close();
          return {realIndex, realIndex};
        }

      } else {

        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread =
          _configurationManager->GetIntValue(WORK_PER_THREAD);
        int64_t entryCount = dimension->GetDataset()->GetEntryCount();
        double minDiff[numCores];
        int64_t positions[numCores];

        /*Search for values with minimum distance.*/
        SET_THREADS(entryCount, minWorkPerThread, numCores);
#pragma omp parallel
        {
          minDiff[omp_get_thread_num()] = std::numeric_limits<double>::max();

          for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
            auto diff = abs(buffer[i] - logicalIndex);
            if (diff < minDiff[omp_get_thread_num()]) {
              positions[omp_get_thread_num()] = i;
              minDiff[omp_get_thread_num()] = diff;
            }
          }
        }

        /*Reduce to get the min value among all threads*/
        IndexPair pair;
        double minDiffAll = std::numeric_limits<double>::max();
        for (int64_t i = 0; i < numCores; ++i) {
          if (minDiff[i] < minDiffAll) {
            minDiffAll = minDiff[i];
            if (buffer[positions[i]] < logicalIndex) {
              pair.inf = positions[i];
              pair.sup = positions[i] + 1;
            } else if (buffer[positions[i]] > logicalIndex) {
              pair.inf = positions[i] - 1;
              pair.sup = positions[i];
            } else {
              pair.inf = positions[i];
              pair.sup = positions[i];
            }
          }
        }

        handler->Close();
        pair.inf = std::max(pair.inf, (RealIndex)0);
        pair.sup =
          std::min(pair.sup,
                   (RealIndex)(dimension->GetDataset()->GetEntryCount() - 1));
        return pair;
      }
    }
  }

  SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr logicalIndexes,
                            DatasetPtr &destinyDataset) {
    bool invalidMapping = false;
    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(logicalIndexes->GetEntryCount(), minWorkPerThread, numCores);

    destinyDataset = _storageManager->Create(DataType(REAL_INDEX, 1),
                                             logicalIndexes->GetEntryCount());

    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr logicalIndexesHandler =
      _storageManager->GetHandler(logicalIndexes);

    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);

    T1 *logicalBuffer = (T1 *)logicalIndexesHandler->GetBuffer();
    RealIndex *destinyBuffer = (RealIndex *)destinyHandler->GetBuffer();

    if (dimension->GetDimensionType() == IMPLICIT) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        destinyBuffer[i] =
          (RealIndex)((logicalBuffer[i] - dimension->GetLowerBound()) /
            dimension->GetSpacing());

        if (destinyBuffer[i] < dimSpecs->GetLowerBound() ||
          destinyBuffer[i] > dimSpecs->GetUpperBound()) {
          invalidMapping = true;
          break;
        }
      }
    } else if (dimension->GetDimensionType() == EXPLICIT) {
      auto dimensionHandler =
        _storageManager->GetHandler(dimension->GetDataset());
      T1 *dimensionBuffer = (T1 *)dimensionHandler->GetBuffer();

#ifdef TBB_SUPPORT
      tbb::concurrent_unordered_map<T1, RealIndex> indexMap;

#pragma omp parallel for
      for (SubTARPosition i = 0; i < dimension->GetDataset()->GetEntryCount();
           ++i) {
        indexMap[dimensionBuffer[i]] = i;
      }
#else
      std::unordered_map<T1, RealIndex> indexMap;
      for (SubTARPosition i = 0; i < dimension->GetDataset()->GetEntryCount();
           ++i) {
        indexMap[dimensionBuffer[i]] = i;
      }
#endif

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        if (indexMap.find(logicalBuffer[i]) != indexMap.end()) {
          destinyBuffer[i] = indexMap[logicalBuffer[i]];
          if (destinyBuffer[i] < dimSpecs->GetLowerBound() ||
            destinyBuffer[i] > dimSpecs->GetUpperBound()) {
            invalidMapping = true;
            break;
          }
        } else {
          invalidMapping = true;
          break;
        }
      }

      dimensionHandler->Close();
    }

    logicalIndexesHandler->Close();
    destinyHandler->Close();

    if (!invalidMapping)
      return SAVIME_SUCCESS;
    else
      return SAVIME_FAILURE;
  }

  SavimeResult UnsafeLogical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                                  DatasetPtr logicalIndexes,
                                  DatasetPtr &destinyDataset) {

    bool invalidMapping = false;
    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(logicalIndexes->GetEntryCount(), minWorkPerThread, numCores);

    destinyDataset = _storageManager->Create(DataType(REAL_INDEX, 1),
                                             logicalIndexes->GetEntryCount());

    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr logicalIndexesHandler =
      _storageManager->GetHandler(logicalIndexes);

    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);

    T1 *logicalBuffer = (T1 *)logicalIndexesHandler->GetBuffer();
    RealIndex *destinyBuffer = (RealIndex *)destinyHandler->GetBuffer();

    if (dimension->GetDimensionType() == IMPLICIT) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        destinyBuffer[i] =
          (RealIndex)(logicalBuffer[i] - dimension->GetLowerBound()) /
            dimension->GetSpacing();

        // if (destinyBuffer[i] < dimSpecs->lower_bound ||
        //    destinyBuffer[i] > dimSpecs->upper_bound) {
        //  destinyBuffer[i] = INVALID_REAL_INDEX;
        //}
      }
    } else if (dimension->GetDimensionType() == EXPLICIT) {
      auto dimensionHandler =
        _storageManager->GetHandler(dimension->GetDataset());
      T2 *dimensionBuffer = (T2 *)dimensionHandler->GetBuffer();

      // TODO: MAKE MAPPING PARALLEL
      std::unordered_map<T2, RealIndex> indexMap;
      for (SubTARPosition i = 0; i < dimension->GetDataset()->GetEntryCount();
           ++i) {
        indexMap[dimensionBuffer[i]] = i;
      }

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        if (indexMap.find((T2)logicalBuffer[i]) != indexMap.end()) {
          destinyBuffer[i] = indexMap[logicalBuffer[i]];
          // if (destinyBuffer[i] < dimSpecs->lower_bound ||
          //    destinyBuffer[i] > dimSpecs->upper_bound) {
          //  destinyBuffer[i] = INVALID_REAL_INDEX;
          //}
        } else {
          destinyBuffer[i] = INVALID_REAL_INDEX;
        }
      }

      dimensionHandler->Close();
    }

    logicalIndexesHandler->Close();
    destinyHandler->Close();

    if (!invalidMapping)
      return SAVIME_SUCCESS;
    else
      return SAVIME_FAILURE;
  }

  Literal Real2Logical(DimensionPtr dimension, RealIndex realIndex) {
    Literal _logicalIndex;
    T1 logicalIndex = 0;

    if (dimension->GetDimensionType() == IMPLICIT) {
      logicalIndex = (T1)(realIndex * dimension->GetSpacing() +
        dimension->GetLowerBound());
    } else if (dimension->GetDimensionType() == EXPLICIT) {
      auto handler = _storageManager->GetHandler(dimension->GetDataset());
      T1 *buffer = (T1 *)handler->GetBuffer();

      if (realIndex < dimension->GetDataset()->GetEntryCount())
        logicalIndex = buffer[realIndex];

      handler->Close();
    }

    SET_LITERAL(_logicalIndex, dimension->GetType(), logicalIndex);
    return _logicalIndex;
  }

  SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr realIndexes,
                            DatasetPtr &destinyDataset) {
    bool invalidMapping = false;
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(realIndexes->GetEntryCount(), minWorkPerThread, numCores);

    destinyDataset = _storageManager->Create(dimension->GetType(),
                                             realIndexes->GetEntryCount());
    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr realIndexesHandler =
      _storageManager->GetHandler(realIndexes);
    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);
    RealIndex *realBuffer = (RealIndex *)realIndexesHandler->GetBuffer();
    T1 *destinyBuffer = (T1 *)destinyHandler->GetBuffer();

    if (dimension->GetDimensionType() == IMPLICIT) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        if (realBuffer[i] < dimSpecs->GetLowerBound() ||
          realBuffer[i] > dimSpecs->GetUpperBound()) {
          invalidMapping = true;
          break;
        }

        destinyBuffer[i] = (T1)(realBuffer[i] * dimension->GetSpacing() +
          dimension->GetLowerBound());
      }
    } else if (dimension->GetDimensionType() == EXPLICIT) {
      auto dimensionHandler =
        _storageManager->GetHandler(dimension->GetDataset());
      T1 *dimensionBuffer = (T1 *)dimensionHandler->GetBuffer();

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        if (realBuffer[i] < dimSpecs->GetLowerBound() ||
          realBuffer[i] > dimSpecs->GetUpperBound()) {
          invalidMapping = true;
          break;
        }

        if (realBuffer[i] < dimension->GetDataset()->GetEntryCount()) {
          destinyBuffer[i] = dimensionBuffer[realBuffer[i]];
        } else {
          invalidMapping = false;
          break;
        }
      }

      dimensionHandler->Close();
    }

    realIndexesHandler->Close();
    destinyHandler->Close();

    if (!invalidMapping)
      return SAVIME_SUCCESS;
    else
      return SAVIME_FAILURE;
  }

  SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2,
                                   DimensionPtr &destinyDim) {

    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
    DimensionPtr dims[2] = {dim1, dim2};
    DimSpecPtr dummyDims[2];
    DatasetPtr materializedDimensions[2];

    for (int32_t i = 0; i < 2; i++) {
      dummyDims[i] = make_shared<DimensionSpecification>(
        UNSAVED_ID, dims[i], 0, dims[i]->CurrentUpperBound(), 1, 1);

      _storageManager->MaterializeDim(dummyDims[i], dims[i]->GetCurrentLength(),
                                      materializedDimensions[i]);
    }

    DatasetHandlerPtr handler1 =
      _storageManager->GetHandler(materializedDimensions[0]);
    T1 *buffer1 = (T1 *)handler1->GetBuffer();
    DatasetHandlerPtr handler2 =
      _storageManager->GetHandler(materializedDimensions[1]);
    T2 *buffer2 = (T2 *)handler2->GetBuffer();

    DatasetPtr filterDs = make_shared<Dataset>(dim1->GetCurrentLength());
    filterDs->Addlistener(
      std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
    filterDs->HasIndexes() = false;
    filterDs->Sorted() = false;

    if (CheckSorted(materializedDimensions[1])) {

      SET_THREADS_ALIGNED(dim1->GetCurrentLength(), workPerThread, numCores,
                          filterDs->GetBitsPerBlock());
#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          T1 value = buffer1[i];

          RealIndex first = 0, last = dim2->GetCurrentLength();
          RealIndex middle = (last + first) / 2;

          while (first <= last) {
            if (buffer2[middle] < value) {
              first = middle + 1;
            } else if (buffer2[middle] == value) {
              (*filterDs->BitMask())[i] = 1;
              break;
            } else {
              last = middle - 1;
            }

            middle = (first + last) / 2;
          }
        }
      }
    } else if (CheckSorted(materializedDimensions[0])) {

      SET_THREADS_ALIGNED(dim2->GetCurrentLength(), workPerThread, numCores,
                          filterDs->GetBitsPerBlock());
#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          T2 value = buffer2[i];

          RealIndex first = 0, last = dim1->GetCurrentLength();
          RealIndex middle = (last + first) / 2;

          while (first <= last) {
            if (buffer1[middle] < value) {
              first = middle + 1;
            } else if (buffer1[middle] == value) {
              (*filterDs->BitMask())[i] = 1;
              break;
            } else {
              last = middle - 1;
            }

            middle = (first + last) / 2;
          }
        }
      }

    } else {

      SET_THREADS_ALIGNED(dim1->GetCurrentLength(), workPerThread, numCores,
                          filterDs->GetBitsPerBlock());

#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          T1 value = buffer1[i];

          RealIndex last = dim2->GetLength();

          for (SubTARPosition j = 0; j < last; j++) {
            if (buffer2[j] == value) {
              (*filterDs->BitMask())[i] = 1;

              break;
            }
          }
        }
      }
    }

    // destinyDim = DimensionPtr(new Dimension());
    if (filterDs->BitMask()->any_parallel(numCores, workPerThread)) {

      DatasetPtr intersectedDimDs;
      _storageManager->Filter(materializedDimensions[0], filterDs,
                              intersectedDimDs);
      _storageManager->CheckSorted(intersectedDimDs);

      destinyDim = make_shared<Dimension>(UNSAVED_ID, "", intersectedDimDs);
      destinyDim->CurrentUpperBound() = (RealIndex)destinyDim->GetUpperBound();

    } else {
      destinyDim =
        make_shared<Dimension>(UNSAVED_ID, "", dim1->GetType(), 0, 0, 1);
    }

    handler1->Close();
    handler2->Close();
    return SAVIME_SUCCESS;
  }

  bool CheckSorted(DatasetPtr dataset) {
    bool isSorted = true;
    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(dataset->GetEntryCount(), minWorkPerThread, numCores);
    startPositionPerCore[0] = 1;

    dataset->Sorted() = true;
    DatasetHandlerPtr dsHandler = _storageManager->GetHandler(dataset);
    T1 *buffer = (T1 *)dsHandler->GetBuffer();

#pragma omp parallel
    {
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        if (buffer[i - 1] > buffer[i] || !isSorted) {
          dataset->Sorted() = isSorted = false;
          break;
        }
      }
    }

    dsHandler->Close();
    return dataset->Sorted();
  }

  SavimeResult Copy(DatasetPtr originDataset, int64_t lowerBound,
                    int64_t upperBound, int64_t offsetInDestiny,
                    int64_t spacingInDestiny, DatasetPtr destinyDataset) {

    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(upperBound - lowerBound, minWorkPerThread, numCores);

    DatasetHandlerPtr originHandler =
      _storageManager->GetHandler(originDataset);
    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);

    // T1 *originBuffer = (T1 *)originHandler->GetBuffer();
    auto originBuffer =
      BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());
    // T2 *destinyBuffer = (T2 *)destinyHandler->GetBuffer();
    auto destinyBuffer =
      BUILD_VECTOR<T2>(destinyHandler->GetBuffer(), originDataset->GetType());

    if (!originDataset->GetType().isVector()) {
#pragma omp parallel for
      for (SubTARPosition i = lowerBound; i <= upperBound; i++) {
        SubTARPosition pos =
          (i - lowerBound) * spacingInDestiny + offsetInDestiny;
        (*destinyBuffer)[pos] = (T2)(*originBuffer)[i];
      }
    } else {
#pragma omp parallel for
      for (SubTARPosition i = lowerBound; i <= upperBound; i++) {
        SubTARPosition pos =
          (i - lowerBound) * spacingInDestiny + offsetInDestiny;
        destinyBuffer->copyTuple(pos, &(*originBuffer)[i]);
      }
    }

    originHandler->Close();
    destinyHandler->Close();
    return SAVIME_SUCCESS;
  }

  SavimeResult Copy(DatasetPtr originDataset, Mapping mapping,
                    DatasetPtr destinyDataset, int64_t &copied) {
#define INVALID -1

    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(originDataset->GetEntryCount(), minWorkPerThread, numCores);

    DatasetHandlerPtr originHandler =
      _storageManager->GetHandler(originDataset);
    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);
    copied = 0;

    auto originBuffer =
      BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

    auto destinyBuffer =
      BUILD_VECTOR<T2>(destinyHandler->GetBuffer(), originDataset->GetType());

    if (!originDataset->GetType().isVector()) {
#pragma omp parallel for reduction(+ : copied)
      for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
        SubTARPosition pos = (*mapping)[i];
        if (pos != INVALID) {
          (*destinyBuffer)[pos] = (T2)(*originBuffer)[i];
          copied++;
        }
      }
    } else {
#pragma omp parallel for reduction(+ : copied)
      for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
        SubTARPosition pos = (*mapping)[i];
        if (pos != INVALID) {
          destinyBuffer->copyTuple(pos, &(*originBuffer)[i]);
          copied++;
        }
      }
    }

    originHandler->Close();
    destinyHandler->Close();
    // delete originBuffer;
    // delete destinyBuffer;
    return SAVIME_SUCCESS;
  }

  SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping,
                    DatasetPtr destinyDataset, int64_t &copied) {
#define INVALID -1

    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(originDataset->GetEntryCount(), minWorkPerThread, numCores);

    DatasetHandlerPtr originHandler =
      _storageManager->GetHandler(originDataset);
    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);
    DatasetHandlerPtr mappingHandler = _storageManager->GetHandler(mapping);
    copied = 0;

    auto originBuffer =
      BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

    auto destinyBuffer =
      BUILD_VECTOR<T2>(destinyHandler->GetBuffer(), originDataset->GetType());

    SubTARPosition *mappingBuffer =
      (SubTARPosition *)mappingHandler->GetBuffer();

    if (!originDataset->GetType().isVector()) {
#pragma omp parallel for reduction(+ : copied)
      for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
        SubTARPosition pos = mappingBuffer[i];
        if (pos != INVALID) {
          (*destinyBuffer)[pos] = (T2)(*originBuffer)[i];
          copied++;
        }
      }
    } else {
#pragma omp parallel for reduction(+ : copied)
      for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
        SubTARPosition pos = mappingBuffer[i];
        if (pos != INVALID) {
          destinyBuffer->copyTuple(pos, &(*originBuffer)[i]);
          copied++;
        }
      }
    }

    originHandler->Close();
    destinyHandler->Close();
    mappingHandler->Close();
    // delete originBuffer;
    // delete destinyBuffer;
    return SAVIME_SUCCESS;
  }

  SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet,
                      DataType type, DatasetPtr &destinyDataset) {
    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    int64_t counters[numCores], partialCounters[numCores];
    memset((char *)counters, 0, sizeof(int64_t) * numCores);
    memset((char *)partialCounters, 0, sizeof(int64_t) * numCores);

    if (!filterDataSet->HasIndexes())
      _storageManager->FromBitMaskToPosition(filterDataSet, true);

    SET_THREADS(filterDataSet->GetEntryCount(), minWorkPerThread, numCores);

    DatasetHandlerPtr originHandler =
      _storageManager->GetHandler(originDataset);
    DatasetHandlerPtr filterHandler =
      _storageManager->GetHandler(filterDataSet);
    RealIndex *filterBuffer = (RealIndex *)filterHandler->GetBuffer();

    destinyDataset =
      _storageManager->Create(type, filterDataSet->GetEntryCount());
    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);

    auto destinyBuffer =
      BUILD_VECTOR<T1>(destinyHandler->GetBuffer(), originDataset->GetType());

    auto originBuffer =
      BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

    if (!originDataset->GetType().isVector()) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        (*destinyBuffer)[i] = (*originBuffer)[filterBuffer[i]];
      }
    } else {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        destinyBuffer->copyTuple(i, &(*originBuffer)[filterBuffer[i]]);
      }
    }

    originHandler->Close();
    filterHandler->Close();
    destinyHandler->Close();

    // delete destinyBuffer;
    // delete originBuffer;
    return SAVIME_SUCCESS;
  }

  SavimeResult Comparison(string op, DatasetPtr operand1, DatasetPtr operand2,
                          DatasetPtr &destinyDataset) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t entryCount = operand1->GetEntryCount() <= operand2->GetEntryCount()
                         ? operand1->GetEntryCount()
                         : operand2->GetEntryCount();

    DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
    DatasetHandlerPtr op2Handler = _storageManager->GetHandler(operand2);

    auto op1Buffer =
      BUILD_VECTOR<T1>(op1Handler->GetBuffer(), operand1->GetType());

    auto op2Buffer =
      BUILD_VECTOR<T2>(op2Handler->GetBuffer(), operand2->GetType());

    destinyDataset = make_shared<Dataset>(entryCount);
    destinyDataset->Addlistener(
      std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
    destinyDataset->HasIndexes() = false;
    destinyDataset->Sorted() = false;

    SET_THREADS_ALIGNED(entryCount, minWorkPerThread, numCores,
                        destinyDataset->GetBitsPerBlock());

    if (!op.compare(_EQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] == (*op2Buffer)[i];
    } else if (!op.compare(_NEQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] != (*op2Buffer)[i];
    } else if (!op.compare(_LE)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] < (*op2Buffer)[i];
    } else if (!op.compare(_GE)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] > (*op2Buffer)[i];
    } else if (!op.compare(_LEQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] <= (*op2Buffer)[i];
    } else if (!op.compare(_GEQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] >= (*op2Buffer)[i];
    } else {
      throw std::runtime_error("Invalid comparison operation.");
    }

    op1Handler->Close();
    op2Handler->Close();
    // delete op1Buffer;
    // delete op2Buffer;

    return SAVIME_SUCCESS;
  }

  SavimeResult Comparison(string op, DatasetPtr operand1, Literal _operand2,
                          DatasetPtr &destinyDataset) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t entryCount = operand1->GetEntryCount();

    DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);

    auto op1Buffer =
      BUILD_VECTOR<T1>(op1Handler->GetBuffer(), operand1->GetType());
    T2 operand2;
    GET_LITERAL(operand2, _operand2, T2);

    destinyDataset = make_shared<Dataset>(entryCount);
    destinyDataset->Addlistener(
      std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
    destinyDataset->HasIndexes() = false;
    destinyDataset->Sorted() = false;

    SET_THREADS_ALIGNED(entryCount, minWorkPerThread, numCores,
                        destinyDataset->GetBitsPerBlock());

    if (!op.compare(_EQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] == operand2;
    } else if (!op.compare(_NEQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] != operand2;
    } else if (!op.compare(_LE)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] < operand2;
    } else if (!op.compare(_GE)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] > operand2;
    } else if (!op.compare(_LEQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] <= operand2;
    } else if (!op.compare(_GEQ)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
        (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] >= operand2;
    } else {
      throw std::runtime_error("Invalid comparison operation.");
    }

    op1Handler->Close();
    // delete op1Buffer;
    return SAVIME_SUCCESS;
  }

  SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs,
                          vector<RealIndex> lowerBounds,
                          vector<RealIndex> upperBounds,
                          DatasetPtr &destinyDataset) {
    vector<DimSpecPtr> subsetSpecs;
    int64_t offset = 0, subsetLen = 1;

    for (int32_t i = 0; i < dimSpecs.size(); i++) {
      lowerBounds[i] = std::max(lowerBounds[i], dimSpecs[i]->GetLowerBound());
      upperBounds[i] = std::min(upperBounds[i], dimSpecs[i]->GetUpperBound());
    }

    for (int32_t i = 0; i < dimSpecs.size(); i++) {
      offset += (lowerBounds[i] - dimSpecs[i]->GetLowerBound()) *
        dimSpecs[i]->GetAdjacency();
      subsetLen *= (upperBounds[i] - lowerBounds[i] + 1);
    }

    for (int32_t i = 0; i < dimSpecs.size(); i++) {
      DimSpecPtr subSpecs = make_shared<DimensionSpecification>(
        UNSAVED_ID, dimSpecs[i]->GetDimension(), lowerBounds[i],
        upperBounds[i], 1, dimSpecs[i]->GetAdjacency());
      subsetSpecs.push_back(subSpecs);
    }

    std::sort(subsetSpecs.begin(), subsetSpecs.end(), compareAdj);
    std::sort(dimSpecs.begin(), dimSpecs.end(), compareAdj);

    for (DimSpecPtr spec : subsetSpecs) {
      bool isPosterior = false;
      savime_size_t skew = 1;
      savime_size_t adjacency = 1;

      for (DimSpecPtr innerSpec : subsetSpecs) {
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
    }

    int64_t subsetSkews[subsetSpecs.size()];
    int64_t subsetSkewsMul[subsetSpecs.size()];
    int64_t subsetSkewsShift[subsetSpecs.size()];
    int64_t subsetAdjacenciesMul[subsetSpecs.size()];
    int64_t subsetAdjacenciesShift[subsetSpecs.size()];
    int64_t dimSpecsAdjacencies[subsetSpecs.size()];

    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    SET_THREADS(subsetLen, minWorkPerThread, numCores);

    destinyDataset =
      _storageManager->Create(DataType(REAL_INDEX, 1), subsetLen);
    destinyDataset->HasIndexes() = true;
    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr handler = _storageManager->GetHandler(destinyDataset);
    SubTARPosition *buffer = (SubTARPosition *)handler->GetBuffer();

    for (SubTARPosition dim = 0; dim < subsetSpecs.size(); dim++) {
      subsetSkews[dim] = subsetSpecs[dim]->GetSkew();
      fast_division(subsetLen, subsetSpecs[dim]->GetSkew(), subsetSkewsMul[dim],
                    subsetSkewsShift[dim]);
      fast_division(subsetLen, subsetSpecs[dim]->GetAdjacency(),
                    subsetAdjacenciesMul[dim], subsetAdjacenciesShift[dim]);
    }

    for (int64_t dim = 0; dim < dimSpecs.size(); dim++) {
      dimSpecsAdjacencies[dim] = dimSpecs[dim]->GetAdjacency();
    }

    int32_t numDim = subsetSpecs.size();
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      RealIndex index = 0;
      RealIndex realIndexes[numDim];

      for (int64_t dim = 0; dim < numDim; dim++) {
        // ------------------Slow Divisions------------------------------
        // realIndexes[dim] = (i%subsetSpecs[dim]->skew)/subsetSpecs[dim]
        //                                                   ->adjacency;
        // realIndexes[dim] = (i%subsetSkews[dim])/subsetAdjacencies[dim];
        // ------------------Fast Divisions------------------------------
        realIndexes[dim] = REMAINDER(i, subsetSkewsMul[dim],
                                     subsetSkewsShift[dim], subsetSkews[dim]);
        realIndexes[dim] = DIVIDE(realIndexes[dim], subsetAdjacenciesMul[dim],
                                  subsetAdjacenciesShift[dim]);
        // int64_t subsetSkewDiv =
        //    (i * subsetSkewsMul[dim]) >> subsetSkewsShift[dim];
        // realIndexes[dim] = i - subsetSkewDiv * subsetSkews[dim];
        // realIndexes[dim] = (realIndexes[dim] * subsetAdjacenciesMul[dim]) >>
        //                   subsetAdjacenciesShift[dim];
      }

      for (int64_t dim = 0; dim < numDim; dim++) {
        index += realIndexes[dim] * dimSpecsAdjacencies[dim];
      }

      buffer[i] = index + offset;
    }

    handler->Close();

    return SAVIME_SUCCESS;
  }

  SavimeResult ComparisonOrderedDim(string op, DimSpecPtr dimSpecs,
                                    Literal operand2, int64_t totalLength,
                                    DatasetPtr &destinyDataset) {

    /*This function either sets all bits, leaves them unset, or does nothing.*/
    typedef enum { __SET_ALL, __UNSET_ALL, __DO_NOTHING } CompOrderDimBehavior;
    CompOrderDimBehavior behavior = __DO_NOTHING;

    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    bool isInRange = false;
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);

    T2 _operand2;
    GET_LITERAL(_operand2, operand2, T2);

    // Exact real index is != INVALID_REAL_INDEX when there is a perfect match
    RealIndex exactRealIndex =
      Logical2Real(dimSpecs->GetDimension(), _operand2);

    auto approxPair = Logical2ApproxReal(dimSpecs->GetDimension(), _operand2);
    RealIndex approxRealIndex = approxPair.inf;

    T1 _dimSpecsLowerBound, _dimSpecsUpperBound;
    Literal dimSpecsLowerBound =
      Real2Logical(dimSpecs->GetDimension(), dimSpecs->GetLowerBound());
    GET_LITERAL(_dimSpecsLowerBound, dimSpecsLowerBound, T1);

    Literal dimSpecsUpperBound =
      Real2Logical(dimSpecs->GetDimension(), dimSpecs->GetUpperBound());
    GET_LITERAL(_dimSpecsUpperBound, dimSpecsUpperBound, T1);

    if (op == _EQ) {
      if (exactRealIndex == INVALID_REAL_INDEX) {
        behavior = __UNSET_ALL;
      } else {
        behavior = __DO_NOTHING;
      }
    } else if (op == _NEQ) {
      if (exactRealIndex == INVALID_REAL_INDEX) {
        behavior = __SET_ALL;
      } else {
        behavior = __DO_NOTHING;
      }
    } else if (op == _LE || op == _LEQ) {
      if (_operand2 > _dimSpecsUpperBound) {
        behavior = __SET_ALL;
      } else if (_operand2 < _dimSpecsLowerBound) {
        behavior = __UNSET_ALL;
      } else {
        behavior = __DO_NOTHING;
      }
    } else if (op == _GE || op == _GEQ) {
      if (_operand2 > _dimSpecsUpperBound) {
        behavior = __SET_ALL;
      } else if (_operand2 < _dimSpecsLowerBound) {
        behavior = __UNSET_ALL;
      } else {
        behavior = __DO_NOTHING;
      }
    }

    if (behavior == __SET_ALL || behavior == __UNSET_ALL) {
      destinyDataset = make_shared<Dataset>(totalLength);
      destinyDataset->Addlistener(
        std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
      destinyDataset->HasIndexes() = false;
      destinyDataset->Sorted() = false;

      if (behavior == __SET_ALL) {
        destinyDataset->BitMask()->set_parallel(numCores, minWorkPerThread);
      }
    } else {
      // Signaling that nothing was done.
      destinyDataset = nullptr;
    }

    return SAVIME_SUCCESS;
  }

  SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, Literal _operand2,
                             int64_t totalLength, DatasetPtr &destinyDataset) {
    bool fastDimComparsionPossible = false;
    auto dimension = dimSpecs->GetDimension();
    auto dataset = dimension->GetDataset();
    bool sortedDataset = (dataset == nullptr) ? false : dataset->Sorted();
    T1 operand2;
    GET_LITERAL(operand2, _operand2, T1);

    fastDimComparsionPossible |= dimSpecs->GetSpecsType() == ORDERED &&
      dimension->GetDimensionType() == IMPLICIT;
    fastDimComparsionPossible |=
      dimSpecs->GetSpecsType() == ORDERED && sortedDataset;

    if (fastDimComparsionPossible) {
      ComparisonOrderedDim(op, dimSpecs, _operand2, totalLength,
                           destinyDataset);
    }

    if (!fastDimComparsionPossible || destinyDataset == nullptr) {
      DatasetPtr materializeDimDataset;
      if (_storageManager->MaterializeDim(
        dimSpecs, totalLength, materializeDimDataset) != SAVIME_SUCCESS)
        return SAVIME_FAILURE;

      return _storageManager->Comparison(op, materializeDimDataset, _operand2,
                                         destinyDataset);
    }

    return SAVIME_SUCCESS;
  }

  SavimeResult Apply(string op, DatasetPtr operand1, DatasetPtr operand2,
                     DatasetPtr &destinyDataset) {
    throw runtime_error("Unsupported apply operation in default template.");
  }

  SavimeResult Apply(string op, DatasetPtr operand1, Literal _operand2,
                     DataType type, DatasetPtr &destinyDataset) {
    throw runtime_error("Unsupported apply operation in default template.");
  }

  SavimeResult MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength,
                              DataType type, DatasetPtr &destinyDataset) {

#define MIN(X, Y) (X < Y) ? X : Y
#define MAX(X, Y) (X > Y) ? X : Y

    if (dimSpecs->GetMaterialized() != nullptr) {
      destinyDataset = dimSpecs->GetMaterialized();
      return SAVIME_SUCCESS;
    }

    int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);

    int64_t dimLength = dimSpecs->GetFilledLength();
    int64_t adjacency = dimSpecs->GetAdjacency();
    int32_t numThreads = MIN(numCores, dimLength);
    int32_t availableThreads = (numCores - numThreads) / numThreads;
    availableThreads = MAX(MIN(availableThreads, adjacency), 1);

    destinyDataset = _storageManager->Create(type, totalLength);
    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyDataset);
    T1 *destinyBuffer = (T1 *)destinyHandler->GetBuffer();

    if (dimSpecs->GetSpecsType() == ORDERED) {
      int64_t entriesInBlock = dimLength * dimSpecs->GetAdjacency();
      int64_t copies = totalLength / entriesInBlock;

      if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
        double dimspecs_lower_bound = dimSpecs->GetLowerBound();
        double dimension_lower_bound =
          dimSpecs->GetDimension()->GetLowerBound();
        double spacing = dimSpecs->GetDimension()->GetSpacing();

        double preamble1 =
          dimspecs_lower_bound * spacing + dimension_lower_bound;

        omp_set_num_threads(numThreads);
#pragma omp parallel for
        for (int64_t i = 0; i < dimLength; ++i) {
          int64_t offset = i * adjacency;
          double rangeMark = preamble1 + i * spacing;
          omp_set_num_threads(availableThreads);
#pragma omp parallel for
          for (int64_t adjMark = 0; adjMark < adjacency; ++adjMark) {
            destinyBuffer[offset + adjMark] = rangeMark;
          }
        }
      } else {
        DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
        DatasetHandlerPtr mappingHandler =
          _storageManager->GetHandler(mappingDataset);
        T1 *mappingBuffer = (T1 *)mappingHandler->GetBuffer();

        int64_t dimspecs_lower_bound = dimSpecs->GetLowerBound();
        int64_t adjacency = dimSpecs->GetAdjacency();

        omp_set_num_threads(numThreads);
#pragma omp parallel for
        for (int64_t i = dimSpecs->GetLowerBound();
             i <= dimSpecs->GetUpperBound(); ++i) {
          double rangeMark = mappingBuffer[i];

          omp_set_num_threads(availableThreads);
#pragma omp parallel for
          for (int64_t adjMark = 0; adjMark < adjacency; ++adjMark) {
            destinyBuffer[(i - dimspecs_lower_bound) * adjacency + adjMark] =
              rangeMark;
          }
        }
        mappingHandler->Close();
      }

      omp_set_num_threads(MIN(numCores, copies));
#pragma omp parallel for
      for (int64_t i = 1; i < copies; ++i) {
        mempcpy((char *)&(destinyBuffer[entriesInBlock * i]),
                (char *)destinyBuffer, sizeof(T1) * entriesInBlock);
      }

    } else if (dimSpecs->GetSpecsType() == PARTIAL) {

      if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        T1 *dimDatasetBuffer = (T1 *)dimDataSetHandler->GetBuffer();
        int64_t adjacency = dimSpecs->GetAdjacency();

        omp_set_num_threads(numThreads);
#pragma omp parallel for
        for (int64_t i = 0; i < dimLength; ++i) {

          omp_set_num_threads(availableThreads);
#pragma omp parallel for
          for (int64_t adjMark = 0; adjMark < adjacency; ++adjMark) {
            destinyBuffer[i * adjacency + adjMark] = dimDatasetBuffer[i];
          }
        }

        dimDataSetHandler->Close();
      } else {
        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        RealIndex *dimDatasetBuffer =
          (RealIndex *)dimDataSetHandler->GetBuffer();

        DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
        DatasetHandlerPtr mappingHandler =
          _storageManager->GetHandler(mappingDataset);
        T1 *mappingBuffer = (T1 *)mappingHandler->GetBuffer();

        int64_t adjacency = dimSpecs->GetAdjacency();

        omp_set_num_threads(numThreads);
#pragma omp parallel for
        for (int64_t i = 0; i < dimLength; ++i) {

          omp_set_num_threads(availableThreads);
#pragma omp parallel for
          for (int64_t adjMark = 0; adjMark < adjacency; ++adjMark) {
            destinyBuffer[i * adjacency + adjMark] =
              mappingBuffer[dimDatasetBuffer[i]];
          }
        }

        dimDataSetHandler->Close();
        mappingHandler->Close();
      }

      int64_t entriesInBlock = dimLength * dimSpecs->GetAdjacency();
      int64_t copies = totalLength / entriesInBlock;

      omp_set_num_threads(MIN(numCores, copies));
#pragma omp parallel for
      for (int64_t i = 1; i < copies; ++i) {
        mempcpy((char *)&(destinyBuffer[entriesInBlock * i]),
                (char *)destinyBuffer, sizeof(T1) * entriesInBlock);
      }

      // destinyHandler->Close();
    } else if (dimSpecs->GetSpecsType() == TOTAL) {
      if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
        destinyDataset = dimSpecs->GetDataset();
      } else {
        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        RealIndex *dimDatasetBuffer =
          (RealIndex *)dimDataSetHandler->GetBuffer();

        DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
        DatasetHandlerPtr mappingHandler =
          _storageManager->GetHandler(mappingDataset);
        T1 *mappingBuffer = (T1 *)mappingHandler->GetBuffer();

        omp_set_num_threads(MIN(numCores, totalLength));
#pragma omp parallel for
        for (int64_t i = 0; i < totalLength; ++i) {
          destinyBuffer[i] = mappingBuffer[dimDatasetBuffer[i]];
        }

        dimDataSetHandler->Close();
        mappingHandler->Close();
      }
    }
    destinyHandler->Close();

    // destinyDataset=dimSpecs->materialized=destinyDataset;
    return SAVIME_SUCCESS;
  }

  SavimeResult PartiatMaterializeDim(DatasetPtr filter, DimSpecPtr dimSpecs,
                                     savime_size_t totalLength, DataType type,
                                     DatasetPtr &destinyLogicalDataset,
                                     DatasetPtr &destinyRealDataset) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);

    if (!filter->HasIndexes())
      _storageManager->FromBitMaskToPosition(filter, true);

    int64_t dimLength = dimSpecs->GetFilledLength();
    destinyLogicalDataset =
      _storageManager->Create(type, filter->GetEntryCount());
    if (destinyLogicalDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr destinyHandler =
      _storageManager->GetHandler(destinyLogicalDataset);
    T1 *destinyBuffer = (T1 *)destinyHandler->GetBuffer();

    SET_THREADS(filter->GetEntryCount(), workPerThread, numCores);

    DatasetHandlerPtr filTerHandler = _storageManager->GetHandler(filter);
    SubTARPosition *filterBuffer = (SubTARPosition *)filTerHandler->GetBuffer();

    if (dimSpecs->GetSpecsType() == ORDERED) {
      destinyRealDataset = _storageManager->Create(DataType(REAL_INDEX, 1),
                                                   filter->GetEntryCount());
      DatasetHandlerPtr realHandler =
        _storageManager->GetHandler(destinyRealDataset);
      RealIndex *realBuffer = (RealIndex *)realHandler->GetBuffer();

      if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
        double preamble0 = dimSpecs->GetLowerBound();
        double preamble1 =
          dimSpecs->GetLowerBound() * dimSpecs->GetDimension()->GetSpacing() +
            dimSpecs->GetDimension()->GetLowerBound();

        int64_t preamble2 = dimSpecs->GetAdjacency() * dimLength;
        int64_t adjacency = dimSpecs->GetAdjacency();
        double spacing = dimSpecs->GetDimension()->GetSpacing();

        int64_t mul_adj, shift_adj;
        int64_t mul_preamble2, shift_preamble2;
        fast_division(totalLength, adjacency, mul_adj, shift_adj);
        fast_division(totalLength, preamble2, mul_preamble2, shift_preamble2);

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          //-----------------Slow division solution--------------------------
          // int64_t preamble4 = ((filterBuffer[i] % (preamble2)) / adjacency);
          //-----------------Fast division solution--------------------------
          int64_t preamble41 = REMAINDER(filterBuffer[i], mul_preamble2,
                                         shift_preamble2, preamble2);
          int64_t preamble4 = DIVIDE(preamble41, mul_adj, shift_adj);

          realBuffer[i] = preamble0 + preamble4;
          destinyBuffer[i] = preamble1 + preamble4 * spacing;
        }
      } else {
        DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
        DatasetHandlerPtr mappingHandler =
          _storageManager->GetHandler(mappingDataset);
        T1 *mappingBuffer = (T1 *)mappingHandler->GetBuffer();

        int64_t preamble1 = dimSpecs->GetAdjacency() * dimLength;
        int64_t adjacency = dimSpecs->GetAdjacency();
        int64_t lower_bound = dimSpecs->GetLowerBound();

        int64_t mul_adj, shift_adj;
        int64_t mul_preamble1, shift_preamble1;
        fast_division(totalLength, adjacency, mul_adj, shift_adj);
        fast_division(totalLength, preamble1, mul_preamble1, shift_preamble1);

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          //-----------------Slow division solution--------------------------
          // realBuffer[i] =
          //    (((filterBuffer[i]) % (preamble1)) / adjacency) + lower_bound;
          //-----------------Fast division solution--------------------------
          int64_t preamble1_1 = REMAINDER(filterBuffer[i], mul_preamble1,
                                          shift_preamble1, preamble1);
          int64_t preamble1_2 = DIVIDE(preamble1_1, mul_adj, shift_adj);
          realBuffer[i] = preamble1_2 + lower_bound;
          destinyBuffer[i] = mappingBuffer[realBuffer[i]];
        }

        mappingHandler->Close();
      }
      realHandler->Close();

    } else if (dimSpecs->GetSpecsType() == PARTIAL) {
      if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        T1 *dimDatasetBuffer = (T1 *)dimDataSetHandler->GetBuffer();

        int64_t preamble1 = dimSpecs->GetAdjacency() * dimLength;
        int64_t adjacency = dimSpecs->GetAdjacency();
        int64_t mul_adj, shift_adj;
        int64_t mul_preamble1, shift_preamble1;
        fast_division(totalLength, adjacency, mul_adj, shift_adj);
        fast_division(totalLength, preamble1, mul_preamble1, shift_preamble1);

#pragma omp parallel
        for (int64_t i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          //-----------------Slow division solution--------------------------
          // destinyBuffer[i] =
          //    dimDatasetBuffer[((filterBuffer[i] % (preamble1)) / adjacency)];
          //-----------------Fast division solution--------------------------
          int64_t preamble1_1 = REMAINDER(filterBuffer[i], mul_preamble1,
                                          shift_preamble1, preamble1);
          int64_t preamble1_2 = DIVIDE(preamble1_1, mul_adj, shift_adj);
          destinyBuffer[i] = dimDatasetBuffer[preamble1_2];
        }

        dimDataSetHandler->Close();
      } else {
        destinyRealDataset = _storageManager->Create(DataType(REAL_INDEX, 1),
                                                     filter->GetEntryCount());
        DatasetHandlerPtr realHandler =
          _storageManager->GetHandler(destinyRealDataset);
        RealIndex *realBuffer = (RealIndex *)realHandler->GetBuffer();

        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        RealIndex *dimDatasetBuffer =
          (RealIndex *)dimDataSetHandler->GetBuffer();

        DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
        DatasetHandlerPtr mappingHandler =
          _storageManager->GetHandler(mappingDataset);
        T1 *mappingBuffer = (T1 *)mappingHandler->GetBuffer();

        int64_t preamble1 = dimSpecs->GetAdjacency() * dimLength;
        int64_t adjacency = dimSpecs->GetAdjacency();
        int64_t mul_adj, shift_adj;
        int64_t mul_preamble1, shift_preamble1;
        fast_division(totalLength, adjacency, mul_adj, shift_adj);
        fast_division(totalLength, preamble1, mul_preamble1, shift_preamble1);

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          //-----------------Slow division solution--------------------------
          // realBuffer[i] =
          //    dimDatasetBuffer[((filterBuffer[i] % (preamble1)) / adjacency)];
          //-----------------Fast division solution--------------------------
          int64_t preamble1_1 = REMAINDER(filterBuffer[i], mul_preamble1,
                                          shift_preamble1, preamble1);
          int64_t preamble1_2 = DIVIDE(preamble1_1, mul_adj, shift_adj);
          realBuffer[i] = dimDatasetBuffer[preamble1_2];
          destinyBuffer[i] = mappingBuffer[realBuffer[i]];
        }

        realHandler->Close();
        dimDataSetHandler->Close();
        mappingHandler->Close();
      }
    } else if (dimSpecs->GetSpecsType() == TOTAL) {
      if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        T1 *dimDatasetBuffer = (T1 *)dimDataSetHandler->GetBuffer();

#pragma omp parallel
        for (int64_t i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          destinyBuffer[i] = dimDatasetBuffer[filterBuffer[i]];
        }

        dimDataSetHandler->Close();
      } else {
        destinyRealDataset = _storageManager->Create(DataType(REAL_INDEX, 1),
                                                     filter->GetEntryCount());

        DatasetHandlerPtr realHandler =
          _storageManager->GetHandler(destinyRealDataset);
        RealIndex *realBuffer = (RealIndex *)realHandler->GetBuffer();

        DatasetHandlerPtr dimDataSetHandler =
          _storageManager->GetHandler(dimSpecs->GetDataset());
        RealIndex *dimDatasetBuffer =
          (RealIndex *)dimDataSetHandler->GetBuffer();

        DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();

        DatasetHandlerPtr mappingHandler =
          _storageManager->GetHandler(mappingDataset);
        T1 *mappingBuffer = (T1 *)mappingHandler->GetBuffer();

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          realBuffer[i] = dimDatasetBuffer[filterBuffer[i]];
          destinyBuffer[i] = mappingBuffer[realBuffer[i]];
        }

        dimDataSetHandler->Close();
        mappingHandler->Close();
        realHandler->Close();
      }
    }

    destinyHandler->Close();
    filTerHandler->Close();

    return SAVIME_SUCCESS;
  }

  SavimeResult Match(DatasetPtr ds1, DatasetPtr ds2, DatasetPtr &ds1Mapping,
                     DatasetPtr &ds2Mapping) {

    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
    bool hasRestrictions = ds1->BitMask() != nullptr;

    int64_t matchesPerThread[numCores] = {0};
    int64_t matchesStartPos[numCores] = {0};
    int64_t matchesFinalPos[numCores] = {0};

    ParallelSavimeHashMap<T2, SubTARPosition> parallelHash;
    DatasetHandlerPtr ds1Handler, ds2Handler;
    shared_ptr<SavimeBuffer<T1>> ds1Buffer;
    shared_ptr<SavimeBuffer<T2>> ds2Buffer;
    ds1Handler = _storageManager->GetHandler(ds1);
    ds1Buffer = BUILD_VECTOR<T1>(ds1Handler->GetBuffer(), ds1->GetType());
    ds2Handler = _storageManager->GetHandler(ds2);
    ds2Buffer = BUILD_VECTOR<T2>(ds2Handler->GetBuffer(), ds2->GetType());

    if (hasRestrictions) {

      SET_THREADS(ds2->GetEntryCount(), workPerThread, numCores);
#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
          parallelHash.Put((*ds2Buffer)[i], i);
        }
      }

      RESET_THREADS(ds1->GetEntryCount(), workPerThread, numCores);
#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
          auto key = (T2)(*ds1Buffer)[i];
          if (!(*ds1->BitMask())[i]) {
            if (parallelHash.map.find(key) != parallelHash.map.end())
              matchesPerThread[omp_get_thread_num()] +=
                parallelHash.map.count((T2)(*ds1Buffer)[i]);
          }
        }
      } // end parallel

      int64_t numMatches = 0;
      for (int32_t i = 0; i < numCores; i++) {
        matchesStartPos[i] = numMatches;
        numMatches += matchesPerThread[i];
        matchesFinalPos[i] = numMatches;
      }

      /*if there are no matches. Nothing to do here.*/
      if (numMatches == 0)
        return SAVIME_SUCCESS;

      ds1Mapping = _storageManager->Create(SUBTAR_POSITION, numMatches);
      if (ds1Mapping == nullptr)
        throw runtime_error("Could not create dataset.");

      ds2Mapping = _storageManager->Create(SUBTAR_POSITION, numMatches);
      if (ds2Mapping == nullptr)
        throw runtime_error("Could not create dataset.");

      ds1Mapping->HasIndexes() = true;
      ds2Mapping->HasIndexes() = true;

      DatasetHandlerPtr ds1MapHandler = _storageManager->GetHandler(ds1Mapping);
      DatasetHandlerPtr ds2MapHandler = _storageManager->GetHandler(ds2Mapping);
      SubTARPosition *ds1MapBuffer =
        (SubTARPosition *)ds1MapHandler->GetBuffer();
      SubTARPosition *ds2MapBuffer =
        (SubTARPosition *)ds2MapHandler->GetBuffer();

#pragma omp parallel
      {
        int64_t ds2Pos = 0, matchCounts = 0;
        int64_t offset = matchesStartPos[omp_get_thread_num()];

        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {

          if ((*ds1->BitMask())[i]) {
            continue;
          }

          auto rangeIt = parallelHash.map.equal_range((T2)(*ds1Buffer)[i]);
          for (auto it = rangeIt.first; it != rangeIt.second; ++it) {
            ds1MapBuffer[offset + matchCounts] = i;
            ds2MapBuffer[offset + matchCounts] = it->second;
            matchCounts++;
          }
        }
      } // end parallel

      ds1MapHandler->Close();
      ds2MapHandler->Close();
    } else {

      SET_THREADS(ds2->GetEntryCount(), workPerThread, numCores);
#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
          parallelHash.Put((*ds2Buffer)[i], i);
        }
      }

      RESET_THREADS(ds1->GetEntryCount(), workPerThread, numCores);
#pragma omp parallel
      {
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
          auto key = (T2)(*ds1Buffer)[i];
          if (parallelHash.map.find(key) != parallelHash.map.end())
            matchesPerThread[omp_get_thread_num()] +=
              parallelHash.map.count((T2)(*ds1Buffer)[i]);
        }
      } // end parallel

      int64_t numMatches = 0;
      for (int32_t i = 0; i < numCores; i++) {
        matchesStartPos[i] = numMatches;
        numMatches += matchesPerThread[i];
        matchesFinalPos[i] = numMatches;
      }

      /*if there are no matches. Nothing to do here.*/
      if (numMatches == 0)
        return SAVIME_SUCCESS;

      ds1Mapping = _storageManager->Create(SUBTAR_POSITION, numMatches);
      if (ds1Mapping == nullptr)
        throw runtime_error("Could not create dataset.");

      ds2Mapping = _storageManager->Create(SUBTAR_POSITION, numMatches);
      if (ds2Mapping == nullptr)
        throw runtime_error("Could not create dataset.");

      ds1Mapping->HasIndexes() = true;
      ds2Mapping->HasIndexes() = true;

      DatasetHandlerPtr ds1MapHandler = _storageManager->GetHandler(ds1Mapping);
      DatasetHandlerPtr ds2MapHandler = _storageManager->GetHandler(ds2Mapping);
      SubTARPosition *ds1MapBuffer =
        (SubTARPosition *)ds1MapHandler->GetBuffer();
      SubTARPosition *ds2MapBuffer =
        (SubTARPosition *)ds2MapHandler->GetBuffer();

#pragma omp parallel
      {
        int64_t ds2Pos = 0, matchCounts = 0;
        SubTARPosition offset = matchesStartPos[omp_get_thread_num()];
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
          auto rangeIt = parallelHash.map.equal_range((T2)(*ds1Buffer)[i]);
          for (auto it = rangeIt.first; it != rangeIt.second; ++it) {
            ds1MapBuffer[offset + matchCounts] = i;
            ds2MapBuffer[offset + matchCounts] = it->second;
            matchCounts++;
          }
        }
      } // end parallel

      ds1MapHandler->Close();
      ds2MapHandler->Close();
    }

    ds1Handler->Close();
    ds2Handler->Close();

    return SAVIME_SUCCESS;
  }

  SavimeResult MatchDim(DimSpecPtr dim1, int64_t totalLen1, DimSpecPtr dim2,
                        int64_t totalLen2, DatasetPtr &ds1Mapping,
                        DatasetPtr &ds2Mapping) {

    DatasetPtr dim1Materialized, dim2Materialized;
    MaterializeDim(dim1, totalLen1, dim1->GetDimension()->GetType(),
                   dim1Materialized);

    MaterializeDim(dim2, totalLen2, dim2->GetDimension()->GetType(),
                   dim2Materialized);

    return Match(dim1Materialized, dim2Materialized, ds1Mapping, ds2Mapping);
  }

  SavimeResult Stretch(DatasetPtr origin, int64_t entryCount,
                       int64_t recordsRepetitions, int64_t datasetRepetitions,
                       DataType type, DatasetPtr &destinyDataset) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t totalDestinySize =
      entryCount * recordsRepetitions * datasetRepetitions;
    int64_t singleDatasetSize = entryCount * recordsRepetitions;

    DatasetHandlerPtr originHandler = _storageManager->GetHandler(origin);

    auto originBuffer =
      BUILD_VECTOR<T1>(originHandler->GetBuffer(), origin->GetType());

    destinyDataset = _storageManager->Create(type, totalDestinySize);
    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr handler = _storageManager->GetHandler(destinyDataset);

    auto buffer = BUILD_VECTOR<T1>(handler->GetBuffer(), origin->GetType());

    SET_THREADS(totalDestinySize, workPerThread, numCores);

    int64_t mul_singleDsSize, shift_singleDsSize;
    int64_t mul_recRepetitions, shift_recRepetitions;
    fast_division(totalDestinySize, singleDatasetSize, mul_singleDsSize,
                  shift_singleDsSize);
    fast_division(totalDestinySize, recordsRepetitions, mul_recRepetitions,
                  shift_recRepetitions);

    if (!origin->GetType().isVector()) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        //------------------Slow Divisions-----------------------------------
        // int64_t originIndex = i % singleDatasetSize;
        // originIndex = originIndex / recordsRepetitions;
        //------------------Fast Divisions-----------------------------------
        RealIndex originIndex = REMAINDER(
          i, mul_singleDsSize, shift_singleDsSize, singleDatasetSize);
        originIndex =
          DIVIDE(originIndex, mul_recRepetitions, shift_recRepetitions);
        (*buffer)[i] = (*originBuffer)[originIndex];
      }
    } else {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        int64_t originIndex = REMAINDER(i, mul_singleDsSize, shift_singleDsSize,
                                        singleDatasetSize);
        originIndex =
          DIVIDE(originIndex, mul_recRepetitions, shift_recRepetitions);
        buffer->copyTuple(i, &(*originBuffer)[originIndex]);
      }
    }

    handler->Close();
    originHandler->Close();
    // delete originBuffer;
    // delete buffer;
    return SAVIME_SUCCESS;
  }

  SavimeResult Split(DatasetPtr origin, int64_t totalLength, int64_t parts,
                     vector<DatasetPtr> &brokenDatasets) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);

    int64_t partitionSize = totalLength / parts;
    vector<T1 *> buffers;
    vector<DatasetHandlerPtr> handlers;

    if ((partitionSize * parts) != totalLength)
      throw std::runtime_error("Invalid number of parts.");

    if (origin->GetType().isVector())
      throw std::runtime_error("Unsupported split with vector datasets.");

    DatasetHandlerPtr originHandler = _storageManager->GetHandler(origin);
    T1 *originBuffer = (T1 *)originHandler->GetBuffer();
    brokenDatasets.resize(parts);

    handlers.resize(parts);
    buffers.resize(parts);

    SET_THREADS(totalLength, workPerThread, numCores);

    //#pragma omp parallel for
    for (int64_t i = 0; i < parts; i++) {
      brokenDatasets[i] =
        _storageManager->Create(origin->GetType(), partitionSize);

      if (brokenDatasets[i] == nullptr)
        throw std::runtime_error("Could not create dataset.");

      handlers[i] = _storageManager->GetHandler(brokenDatasets[i]);
      buffers[i] = (T1 *)handlers[i]->GetBuffer();
    }

    int64_t mul_part_size, shift_part_size;
    fast_division(totalLength, partitionSize, mul_part_size, shift_part_size);

#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      // int64_t bufferIndex = i / partitionSize;
      // int64_t internIndex = i % partitionSize;
      SubTARPosition bufferIndex = DIVIDE(i, mul_part_size, shift_part_size);
      SubTARPosition internIndex =
        REMAINDER(i, mul_part_size, shift_part_size, partitionSize);
      buffers[bufferIndex][internIndex] = originBuffer[i];
    }

    //#pragma omp parallel for
    for (int64_t i = 0; i < parts; i++) {
      handlers[i]->Close();
    }

    originHandler->Close();

    return SAVIME_SUCCESS;
  }
};

#endif /* DEFAULT_TEMPLATE_H */
