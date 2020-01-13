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
#include "../core/include/metadata.h"
#include "../core/include/savime_hash.h"
#include "../core/include/query_data_manager.h"
#include "../core/include/storage_manager.h"
#include "../core/include/abstract_storage_manager.h"
#include "default_storage_manager.h"
#include <algorithm>
#include <cmath>
#include <omp.h>
#include <cstdlib>

#define TOLERANCE 1e-06
#define IMPLICIT_LOGICAL2REAL(DIM, PRE, INDEX, OUT)                            \
  double intpart;                                                              \
  double fRealIndex = 0.0;                                                     \
  fRealIndex = (INDEX * PRE - dimension->GetLowerBound() * PRE);               \
  /*double mod = abs(std::modf(fRealIndex, &intpart));*/                       \
  double modNoAbs = fRealIndex - (int64_t)fRealIndex;                          \
  double mod = abs(modNoAbs);                                                  \
  intpart = fRealIndex - modNoAbs;                                             \
  if (mod < TOLERANCE) {                                                       \
    OUT = (RealIndex)intpart;                                                  \
  } else if ((1 - mod) < TOLERANCE) {                                          \
    OUT = (RealIndex)intpart + 1;                                              \
  } else {                                                                     \
    OUT = INVALID_REAL_INDEX;                                                  \
  }


#define BIN_SEARCH(BUFFER, VALUE, OUT, SIZE)                                   \
  RealIndex first = 0, last = SIZE;                                            \
  RealIndex middle = (last + first) / 2;                                       \
                                                                               \
  if (BUFFER[first] <= VALUE && BUFFER[last] >= VALUE) {                       \
    while (first <= last) {                                                    \
      if (abs((double)(BUFFER[middle] - VALUE)) < TOLERANCE) {                 \
        OUT = middle;                                                          \
        break;                                                                 \
      } else if (BUFFER[middle] < VALUE) {                                     \
        first = middle + 1;                                                    \
      } else {                                                                 \
        last = middle - 1;                                                     \
      }                                                                        \
      middle = (first + last) / 2;                                             \
    }                                                                          \
  }                                                                            \


#define BIN_SEARCH_POS(BUFFER, VALUE, OUT, SIZE)                               \
  RealIndex first = 0, last = SIZE;                                            \
  RealIndex middle = (last + first) / 2;                                       \
                                                                               \
  while (first <= last) {                                                      \
    if (abs((double)(BUFFER[middle] - VALUE)) < TOLERANCE) {                   \
      OUT = 1;                                                                 \
      break;                                                                   \
    } else if (BUFFER[middle] < VALUE) {                                       \
      first = middle + 1;                                                      \
    } else {                                                                   \
      last = middle - 1;                                                       \
    }                                                                          \
    middle = (first + last) / 2;                                               \
  }                                                                            \


#define IMPLICIT_REAL2LOGICAL(DIMENSION, REAL)                                 \
  (REAL * DIMENSION->GetSpacing()) + DIMENSION->GetLowerBound()

template<class T1, class T2>
class TemplateStorageManager : public AbstractStorageManager {
    StorageManagerPtr _storageManager;
    ConfigurationManagerPtr _configurationManager;
    SystemLoggerPtr _systemLogger;


    struct TemplateStorageManagerMap {
        std::unordered_map<T2, RealIndex> map;
        RealIndex operator[](int64_t index){ return map[index]; }

    };
    typedef std::shared_ptr<TemplateStorageManagerMap> TemplateStorageManagerMapPtr;

    std::map<int64_t,  TemplateStorageManagerMapPtr> _cacheMaps;
    mutex _cacheMapsmutex;
    int64_t _nextToken = 0x01;

    bool checkMapCache(int64_t token){
      bool hasCache = false;
      _cacheMapsmutex.lock();
      if( _cacheMaps.find(token) != _cacheMaps.end()){
        hasCache = true;
      } else {
        hasCache = false;
      }
      _cacheMapsmutex.unlock();
      return hasCache;
    }

    bool checkMapCache(const DimensionPtr &dim){
      return checkMapCache(dim->token);
    }

    void addMapToCache(TemplateStorageManagerMapPtr map, DimensionPtr dim){
      _cacheMapsmutex.lock();
      _nextToken++;
      dim->token = _nextToken;
      _cacheMaps[dim->token] = map;
      _cacheMapsmutex.unlock();
    }

public:
    TemplateStorageManager(StorageManagerPtr storageManager,
                           ConfigurationManagerPtr configurationManager,
                           SystemLoggerPtr systemLogger) {
      _storageManager = storageManager;
      _configurationManager = configurationManager;
      _systemLogger = systemLogger;
    }

    RealIndex Logical2Real(DimensionPtr dimension, Literal _logicalIndex) override {

      RealIndex realIndex = INVALID_EXACT_REAL_INDEX;
      T2 logicalIndex;

      GET_LITERAL(logicalIndex, _logicalIndex, T2);

      if (dimension->GetDimensionType() == IMPLICIT) {

        if (dimension->GetLowerBound() <= logicalIndex &&
            dimension->GetUpperBound() >= logicalIndex) {
          double preamble = 1 / dimension->GetSpacing();
          IMPLICIT_LOGICAL2REAL(dimension, preamble, logicalIndex, realIndex);
          if (realIndex < 0 || realIndex > dimension->GetRealUpperBound())
            realIndex = INVALID_EXACT_REAL_INDEX;
        }
      } else if (dimension->GetDimensionType() == EXPLICIT) {

        auto handler = _storageManager->GetHandler(dimension->GetDataset());
        T1 *buffer = (T1 *) handler->GetBuffer();

        if (dimension->GetDataset()->Sorted()) {
          BIN_SEARCH(buffer, logicalIndex, realIndex,
                     dimension->GetDataset()->GetEntryCount() - 1);

        } else {

          int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
          int32_t minWorkPerThread =
            _configurationManager->GetIntValue(WORK_PER_THREAD);
          int64_t entryCount = dimension->GetDataset()->GetEntryCount();

          SET_THREADS(entryCount, minWorkPerThread, numCores);
#pragma omp parallel
          for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
            if (abs((double) buffer[i] - logicalIndex) < TOLERANCE) {
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


    IndexPair Logical2ApproxReal(DimensionPtr dimension, Literal _logicalIndex) override {

      T2 logicalIndex;
      GET_LITERAL(logicalIndex, _logicalIndex, T2);

      double intpart;
      RealIndex realIndex = INVALID_EXACT_REAL_INDEX;

      if (dimension->GetDimensionType() == IMPLICIT) {
        if (dimension->GetLowerBound() > logicalIndex)
          return {0, 0};

        if (dimension->GetUpperBound() < logicalIndex)
          return {dimension->GetRealUpperBound(), dimension->GetRealUpperBound()};

        double preamble = 1 / dimension->GetSpacing();
        IMPLICIT_LOGICAL2REAL(dimension, preamble, logicalIndex, realIndex);

        if (realIndex == INVALID_EXACT_REAL_INDEX) {
          realIndex =
            std::min((RealIndex) intpart, dimension->GetRealUpperBound());
          return {realIndex, realIndex + 1};
        } else {
          return {realIndex, realIndex};
        }

      } else if (dimension->GetDimensionType() == EXPLICIT) {

        int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
        int32_t minWorkPerThread =
          _configurationManager->GetIntValue(WORK_PER_THREAD);

        auto handler = _storageManager->GetHandler(dimension->GetDataset());
        T1 *buffer = (T1 *) handler->GetBuffer();

        if (dimension->GetDataset()->Sorted()) {
          RealIndex _first = 0,
            _last = dimension->GetDataset()->GetEntryCount() - 1;


          if (buffer[_first] > logicalIndex) {
            handler->Close();
            return {0, 0};
          }

          if (buffer[_last] < logicalIndex) {
            handler->Close();
            return {dimension->GetRealUpperBound(),
                    dimension->GetRealUpperBound()};
          }

          BIN_SEARCH(buffer, logicalIndex, realIndex, _last);

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


          int64_t entryCount = dimension->GetDataset()->GetEntryCount();
          double minDiff[numCores];
          int64_t positions[numCores];

          SET_THREADS(entryCount, minWorkPerThread, numCores);
#pragma omp parallel
          {
            minDiff[omp_get_thread_num()] = std::numeric_limits<double>::max();
            positions[omp_get_thread_num()] = 0;

            for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
              auto diff = std::abs((double) (buffer[i] - logicalIndex));
              if (diff < minDiff[omp_get_thread_num()]) {
                positions[omp_get_thread_num()] = i;
                minDiff[omp_get_thread_num()] = diff;
              }
            }
          }

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
          pair.inf = std::max(pair.inf, (RealIndex) 0);
          pair.sup =
            std::min(pair.sup,
                     (RealIndex) (dimension->GetDataset()->GetEntryCount() - 1));
          return pair;
        }
      }
    }


    SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                              DatasetPtr logicalIndexes,
                              DatasetPtr &destinyDataset) override {

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

      T1 *logicalBuffer = (T1 *) logicalIndexesHandler->GetBuffer();
      auto *destinyBuffer = (RealIndex *) destinyHandler->GetBuffer();

      double preamble = 1 / dimension->GetSpacing();
      if (dimension->GetDimensionType() == IMPLICIT) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {

          destinyBuffer[i] = INVALID_REAL_INDEX;
          IMPLICIT_LOGICAL2REAL(dimension, preamble, logicalBuffer[i], destinyBuffer[i]);

          if (destinyBuffer[i] < dimSpecs->GetLowerBound() ||
              destinyBuffer[i] > dimSpecs->GetUpperBound()) {
            invalidMapping = true;
            break;
          }
        }

      } else if (dimension->GetDimensionType() == EXPLICIT) {
        auto dimensionHandler =
          _storageManager->GetHandler(dimension->GetDataset());
        T1 *dimensionBuffer = (T1 *) dimensionHandler->GetBuffer();

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
                                    DatasetPtr &destinyDataset) override {

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

      T1 *logicalBuffer = (T1 *) logicalIndexesHandler->GetBuffer();
      auto *destinyBuffer = (RealIndex *) destinyHandler->GetBuffer();

      if (dimension->GetDimensionType() == IMPLICIT) {

        double preamble = 1 / dimension->GetSpacing();
        double preamble2 = dimension->GetLowerBound() * preamble;
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          //destinyBuffer[i] =
          //   (RealIndex)(logicalBuffer[i] - dimension->GetLowerBound()) /
          //    dimension->GetSpacing();

          //destinyBuffer[i] = INVALID_REAL_INDEX;
          //IMPLICIT_LOGICAL2REAL(dimension, preamble, logicalBuffer[i], destinyBuffer[i]);

          double fRealIndex = (logicalBuffer[i] * preamble - preamble2);
          double modNoAbs = fRealIndex - (int64_t)fRealIndex;
          double mod = abs(modNoAbs);
          double intpart = fRealIndex - modNoAbs;

          if (mod < TOLERANCE) {
            destinyBuffer[i] = (RealIndex)intpart;
          } else if ((1 - mod) < TOLERANCE) {
            destinyBuffer[i] = (RealIndex)intpart + 1;
          } else {
            destinyBuffer[i] = INVALID_REAL_INDEX;
          }

          // if (destinyBuffer[i] < dimSpecs->lower_bound ||
          //    destinyBuffer[i] > dimSpecs->upper_bound) {
          //  destinyBuffer[i] = INVALID_REAL_INDEX;
          //}
        }
      } else if (dimension->GetDimensionType() == EXPLICIT) {
        auto dimensionHandler =
          _storageManager->GetHandler(dimension->GetDataset());
        T2 *dimensionBuffer = (T2 *) dimensionHandler->GetBuffer();

/*
#ifdef TBB_SUPPORT
        tbb::concurrent_unordered_map<T2, RealIndex> indexMap;
#pragma omp parallel for
        for (SubTARPosition i = 0; i < dimension->GetDataset()->GetEntryCount();
             ++i) {
          indexMap[dimensionBuffer[i]] = i;
        }
#else
*/
        TemplateStorageManagerMapPtr indexMap = make_shared<TemplateStorageManagerMap>();

        if(checkMapCache(dimension)){
          _cacheMapsmutex.lock();
          indexMap = _cacheMaps[dimension->token];
          _cacheMapsmutex.unlock();
        } else {
          for (SubTARPosition i = 0; i < dimension->GetDataset()->GetEntryCount();
               ++i) {
            indexMap->map[dimensionBuffer[i]] = i;
          }
          //addMapToCache(indexMap, dimension);
        }

//#endif


#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          if (indexMap->map.find((T2) logicalBuffer[i]) != indexMap->map.end()) {
            destinyBuffer[i] = indexMap->map[logicalBuffer[i]];
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

    Literal Real2Logical(DimensionPtr dimension, RealIndex realIndex) override {

      Literal _logicalIndex;
      T1 logicalIndex = 0;

      if (dimension->GetDimensionType() == IMPLICIT) {
        //logicalIndex = (T1)(realIndex * dimension->GetSpacing() +
        //  dimension->GetLowerBound());
        logicalIndex = (T1) IMPLICIT_REAL2LOGICAL(dimension, realIndex);

      } else if (dimension->GetDimensionType() == EXPLICIT) {
        auto handler = _storageManager->GetHandler(dimension->GetDataset());
        T1 *buffer = (T1 *) handler->GetBuffer();

        if (realIndex < dimension->GetDataset()->GetEntryCount())
          logicalIndex = buffer[realIndex];

        handler->Close();
      }

      SET_LITERAL(_logicalIndex, dimension->GetType(), logicalIndex);
      return _logicalIndex;
    }

    SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs,
                              DatasetPtr realIndexes,
                              DatasetPtr &destinyDataset) override {

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
      auto *realBuffer = (RealIndex *) realIndexesHandler->GetBuffer();
      T1 *destinyBuffer = (T1 *) destinyHandler->GetBuffer();

      if (dimension->GetDimensionType() == IMPLICIT) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          if (realBuffer[i] < dimSpecs->GetLowerBound() ||
              realBuffer[i] > dimSpecs->GetUpperBound()) {
            invalidMapping = true;
            break;
          }

          destinyBuffer[i] = (T1) IMPLICIT_REAL2LOGICAL(dimension, realBuffer[i]);
        }
      } else if (dimension->GetDimensionType() == EXPLICIT) {
        auto dimensionHandler =
          _storageManager->GetHandler(dimension->GetDataset());
        T1 *dimensionBuffer = (T1 *) dimensionHandler->GetBuffer();

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
                                     DimensionPtr &destinyDim) override {

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
      T1 *buffer1 = (T1 *) handler1->GetBuffer();
      DatasetHandlerPtr handler2 =
        _storageManager->GetHandler(materializedDimensions[1]);
      T2 *buffer2 = (T2 *) handler2->GetBuffer();

      DatasetPtr filterDs = make_shared<Dataset>(dim1->GetCurrentLength());
      filterDs->AddListener(
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
            BIN_SEARCH_POS(buffer2, value, (*filterDs->BitMask())[i], dim2->GetCurrentLength());
          }
        }
      } else if (CheckSorted(materializedDimensions[0])) {

        SET_THREADS_ALIGNED(dim2->GetCurrentLength(), workPerThread, numCores,
                            filterDs->GetBitsPerBlock());
#pragma omp parallel
        {
          for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
            T2 value = buffer2[i];
            BIN_SEARCH_POS(buffer1, value, (*filterDs->BitMask())[i], dim1->GetCurrentLength());
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


        bool isEquallySpaced = false;
        double lowerBound;
        double upperBound;
        double spacing;

        if(_storageManager->CheckSorted(intersectedDimDs) &&
            dim1->GetDimensionType() == IMPLICIT && dim2->GetDimensionType() == IMPLICIT){

          auto intersectDimHandler = _storageManager->GetHandler(intersectedDimDs);
          T1 * intersectBuffer = (T1 *) intersectDimHandler->GetBuffer();


          if(intersectedDimDs->GetEntryCount() > 2) {

            SET_THREADS(intersectedDimDs->GetEntryCount(), workPerThread, numCores);
            startPositionPerCore[0] = 1;

            isEquallySpaced = true;
            lowerBound = intersectBuffer[0];
            upperBound = intersectBuffer[intersectedDimDs->GetEntryCount()-1];
            spacing = intersectBuffer[1] - intersectBuffer[0];
            double tol = MIN_SPACING;

#pragma omp parallel
            {
              for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
                double diff = intersectBuffer[i] - intersectBuffer[i - 1];
                if (abs(diff - spacing) > tol || !isEquallySpaced) {
                  isEquallySpaced = false;
                  break;
                }
              }
            }
            //isEquallySpaced = false;
          //Has a single element in the intersection
          } else {
            isEquallySpaced = false;
          }

        }

        if(!isEquallySpaced) {
          destinyDim = make_shared<Dimension>(UNSAVED_ID, "", intersectedDimDs);
          destinyDim->CurrentUpperBound() = (RealIndex) destinyDim->GetUpperBound();
        } else {
          destinyDim = make_shared<Dimension>(UNSAVED_ID, "", dim1->GetType(), lowerBound, upperBound, spacing);
          destinyDim->CurrentUpperBound() = (RealIndex) destinyDim->GetUpperBound();
        }


      } else {
        destinyDim =
          make_shared<Dimension>(-2, "", dim1->GetType(), 0, 0, 1);
      }

      handler1->Close();
      handler2->Close();
      return SAVIME_SUCCESS;
    }

    bool CheckSorted(DatasetPtr dataset) override {
      bool isSorted = true;
      int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int32_t minWorkPerThread =
        _configurationManager->GetIntValue(WORK_PER_THREAD);

      SET_THREADS(dataset->GetEntryCount(), minWorkPerThread, numCores);
      startPositionPerCore[0] = 1;

      dataset->Sorted() = true;
      DatasetHandlerPtr dsHandler = _storageManager->GetHandler(dataset);
      T1 *buffer = (T1 *) dsHandler->GetBuffer();

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

    SavimeResult Copy(DatasetPtr originDataset, SubTARPosition lowerBound,
                      SubTARPosition upperBound, SubTARPosition offsetInDestiny,
                      savime_size_t spacingInDestiny, DatasetPtr destinyDataset) override {

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
          (*destinyBuffer)[pos] = (T2) (*originBuffer)[i];
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
                      DatasetPtr destinyDataset, int64_t &copied) override {

      int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int32_t minWorkPerThread =
        _configurationManager->GetIntValue(WORK_PER_THREAD);

      SET_THREADS(originDataset->GetEntryCount(), minWorkPerThread, numCores);

      DatasetHandlerPtr originHandler =
        _storageManager->GetHandler(originDataset);
      DatasetHandlerPtr destinyHandler =
        _storageManager->GetHandler(destinyDataset);
      copied = 0;
      int64_t totalCopied = 0;

      auto originBuffer =
        BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

      auto destinyBuffer =
        BUILD_VECTOR<T2>(destinyHandler->GetBuffer(), originDataset->GetType());

      if (!originDataset->GetType().isVector()) {
#pragma omp parallel for reduction(+ : totalCopied)
        for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
          SubTARPosition pos = (*mapping)[i];
          if (pos != INVALID_SUBTAR_POSITION) {
            (*destinyBuffer)[pos] = (T2) (*originBuffer)[i];
            totalCopied++;
          }
        }
      } else {
#pragma omp parallel for reduction(+ : totalCopied)
        for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
          SubTARPosition pos = (*mapping)[i];
          if (pos != INVALID_SUBTAR_POSITION) {
            destinyBuffer->copyTuple(pos, &(*originBuffer)[i]);
            totalCopied++;
          }
        }
      }
      copied = totalCopied;


      originHandler->Close();
      destinyHandler->Close();
      // delete originBuffer;
      // delete destinyBuffer;
      return SAVIME_SUCCESS;
    }

    SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping,
                      DatasetPtr destinyDataset, int64_t &copied) override {

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
      int64_t totalCopied = 0;

      auto originBuffer =
        BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

      auto destinyBuffer =
        BUILD_VECTOR<T2>(destinyHandler->GetBuffer(), originDataset->GetType());

      SubTARPosition *mappingBuffer =
        (SubTARPosition *) mappingHandler->GetBuffer();

      if (!originDataset->GetType().isVector()) {
#pragma omp parallel for reduction(+ : totalCopied)
        for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
          SubTARPosition pos = mappingBuffer[i];
          if (pos != INVALID_SUBTAR_POSITION) {
            (*destinyBuffer)[pos] = (T2) (*originBuffer)[i];
            totalCopied++;
          }
        }
      } else {
#pragma omp parallel for reduction(+ : totalCopied)
        for (SubTARPosition i = 0; i < originDataset->GetEntryCount(); i++) {
          SubTARPosition pos = mappingBuffer[i];
          if (pos != INVALID_SUBTAR_POSITION) {
            destinyBuffer->copyTuple(pos, &(*originBuffer)[i]);
            totalCopied++;
          }
        }
      }

      copied = totalCopied;

      originHandler->Close();
      destinyHandler->Close();
      mappingHandler->Close();

      return SAVIME_SUCCESS;
    }

    SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet,
                        DataType type, DatasetPtr &destinyDataset) override {
      int32_t numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int32_t minWorkPerThread =
        _configurationManager->GetIntValue(WORK_PER_THREAD);

      int64_t counters[numCores], partialCounters[numCores];
      memset((char *) counters, 0, sizeof(int64_t) * numCores);
      memset((char *) partialCounters, 0, sizeof(int64_t) * numCores);

      if (!filterDataSet->HasIndexes())
        _storageManager->FromBitMaskToPosition(filterDataSet, true);

      SET_THREADS(filterDataSet->GetEntryCount(), minWorkPerThread, numCores);

      DatasetHandlerPtr originHandler =
        _storageManager->GetHandler(originDataset);
      DatasetHandlerPtr filterHandler =
        _storageManager->GetHandler(filterDataSet);
      auto *filterBuffer = (RealIndex *) filterHandler->GetBuffer();

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

      return SAVIME_SUCCESS;
    }

    SavimeResult Comparison(string op, DatasetPtr operand1, DatasetPtr operand2,
                            DatasetPtr &destinyDataset) override {
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
      destinyDataset->AddListener(
        std::dynamic_pointer_cast<DefaultStorageManager>(_storageManager));
      destinyDataset->HasIndexes() = false;
      destinyDataset->Sorted() = false;

      SET_THREADS_ALIGNED(entryCount, minWorkPerThread, numCores,
                          (int32_t) destinyDataset->GetBitsPerBlock());

      if (op == _EQ) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
          (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] == (*op2Buffer)[i];
      } else if (op == _NEQ) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
          (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] != (*op2Buffer)[i];
      } else if (op == _LE) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
          (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] < (*op2Buffer)[i];
      } else if (op == _GE) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
          (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] > (*op2Buffer)[i];
      } else if (op == _LEQ) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
          (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] <= (*op2Buffer)[i];
      } else if (op == _GEQ) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i)
          (*destinyDataset->BitMask())[i] = (*op1Buffer)[i] >= (*op2Buffer)[i];
      } else {
        throw std::runtime_error("Invalid comparison operation.");
      }

      op1Handler->Close();
      op2Handler->Close();

      return SAVIME_SUCCESS;
    }

    SavimeResult Comparison(string op, DatasetPtr operand1, Literal _operand2,
                            DatasetPtr &destinyDataset) override {
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
      destinyDataset->AddListener(
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
                            DatasetPtr &destinyDataset) override {

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

      ADJUST_SPECS(subsetSpecs);

      int64_t subsetStrides[subsetSpecs.size()];
      int64_t subsetStridesMul[subsetSpecs.size()];
      int64_t subsetStridesShift[subsetSpecs.size()];
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
      SubTARPosition *buffer = (SubTARPosition *) handler->GetBuffer();

      for (SubTARPosition dim = 0; dim < subsetSpecs.size(); dim++) {
        subsetStrides[dim] = subsetSpecs[dim]->GetStride();
        fast_division(subsetLen,
                      subsetSpecs[dim]->GetStride(), subsetStridesMul[dim],
                      subsetStridesShift[dim]);
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
          // realIndexes[dim] = (i%subsetSpecs[dim]->stride)/subsetSpecs[dim]
          //                                                   ->adjacency;
          // realIndexes[dim] = (i%subsetStrides[dim])/subsetAdjacencies[dim];
          // ------------------Fast Divisions------------------------------
          realIndexes[dim] = REMAINDER(i, subsetStridesMul[dim],
                                       subsetStridesShift[dim], subsetStrides[dim]);
          realIndexes[dim] = DIVIDE(realIndexes[dim], subsetAdjacenciesMul[dim],
                                    subsetAdjacenciesShift[dim]);
          // int64_t subsetSkewDiv =
          //    (i * subsetStridesMul[dim]) >> subsetStridesShift[dim];
          // realIndexes[dim] = i - subsetSkewDiv * subsetStrides[dim];
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
                                      DatasetPtr &destinyDataset) override {

      /*This function either sets all bits, leaves them unset, or does nothing.*/
      typedef enum {
          __SET_ALL, __UNSET_ALL, __DO_NOTHING
      } CompOrderDimBehavior;
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
        destinyDataset->AddListener(
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
                               int64_t totalLength, DatasetPtr &destinyDataset) override {
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

      fastDimComparsionPossible = false;

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
                       DatasetPtr &destinyDataset) override {
      throw runtime_error("Unsupported apply operation in default template.");
    }

    SavimeResult Apply(string op, DatasetPtr operand1, Literal _operand2,
                       DataType type, DatasetPtr &destinyDataset) override {
      throw runtime_error("Unsupported apply operation in default template.");
    }

    SavimeResult MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength,
                                DataType type, DatasetPtr &destinyDataset) override {

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
      T1 *destinyBuffer = (T1 *) destinyHandler->GetBuffer();

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
//#pragma omp parallel for
            for (int64_t adjMark = 0; adjMark < adjacency; ++adjMark) {
              destinyBuffer[offset + adjMark] = rangeMark;
            }
          }
        } else {
          DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
          DatasetHandlerPtr mappingHandler =
            _storageManager->GetHandler(mappingDataset);
          T1 *mappingBuffer = (T1 *) mappingHandler->GetBuffer();

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
          mempcpy((char *) &(destinyBuffer[entriesInBlock * i]),
                  (char *) destinyBuffer, sizeof(T1) * entriesInBlock);
        }

      } else if (dimSpecs->GetSpecsType() == PARTIAL) {

        if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
          DatasetHandlerPtr dimDataSetHandler =
            _storageManager->GetHandler(dimSpecs->GetDataset());
          T1 *dimDatasetBuffer = (T1 *) dimDataSetHandler->GetBuffer();
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
            (RealIndex *) dimDataSetHandler->GetBuffer();

          DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
          DatasetHandlerPtr mappingHandler =
            _storageManager->GetHandler(mappingDataset);
          T1 *mappingBuffer = (T1 *) mappingHandler->GetBuffer();

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
          mempcpy((char *) &(destinyBuffer[entriesInBlock * i]),
                  (char *) destinyBuffer, sizeof(T1) * entriesInBlock);
        }

        // destinyHandler->Close();
      } else if (dimSpecs->GetSpecsType() == TOTAL) {
        if (dimSpecs->GetDimension()->GetDimensionType() == IMPLICIT) {
          destinyDataset = dimSpecs->GetDataset();
        } else {
          DatasetHandlerPtr dimDataSetHandler =
            _storageManager->GetHandler(dimSpecs->GetDataset());
          RealIndex *dimDatasetBuffer =
            (RealIndex *) dimDataSetHandler->GetBuffer();

          DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
          DatasetHandlerPtr mappingHandler =
            _storageManager->GetHandler(mappingDataset);
          T1 *mappingBuffer = (T1 *) mappingHandler->GetBuffer();

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
                                       DatasetPtr &destinyRealDataset) override {

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
      T1 *destinyBuffer = (T1 *) destinyHandler->GetBuffer();

      SET_THREADS(filter->GetEntryCount(), workPerThread, numCores);

      DatasetHandlerPtr filTerHandler = _storageManager->GetHandler(filter);
      SubTARPosition *filterBuffer = (SubTARPosition *) filTerHandler->GetBuffer();

      if (dimSpecs->GetSpecsType() == ORDERED) {
        destinyRealDataset = _storageManager->Create(DataType(REAL_INDEX, 1),
                                                     filter->GetEntryCount());
        DatasetHandlerPtr realHandler =
          _storageManager->GetHandler(destinyRealDataset);
        RealIndex *realBuffer = (RealIndex *) realHandler->GetBuffer();

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
          T1 *mappingBuffer = (T1 *) mappingHandler->GetBuffer();

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
          T1 *dimDatasetBuffer = (T1 *) dimDataSetHandler->GetBuffer();

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
          RealIndex *realBuffer = (RealIndex *) realHandler->GetBuffer();

          DatasetHandlerPtr dimDataSetHandler =
            _storageManager->GetHandler(dimSpecs->GetDataset());
          RealIndex *dimDatasetBuffer =
            (RealIndex *) dimDataSetHandler->GetBuffer();

          DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();
          DatasetHandlerPtr mappingHandler =
            _storageManager->GetHandler(mappingDataset);
          T1 *mappingBuffer = (T1 *) mappingHandler->GetBuffer();

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
          T1 *dimDatasetBuffer = (T1 *) dimDataSetHandler->GetBuffer();

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
          RealIndex *realBuffer = (RealIndex *) realHandler->GetBuffer();

          DatasetHandlerPtr dimDataSetHandler =
            _storageManager->GetHandler(dimSpecs->GetDataset());
          RealIndex *dimDatasetBuffer =
            (RealIndex *) dimDataSetHandler->GetBuffer();

          DatasetPtr mappingDataset = dimSpecs->GetDimension()->GetDataset();

          DatasetHandlerPtr mappingHandler =
            _storageManager->GetHandler(mappingDataset);
          T1 *mappingBuffer = (T1 *) mappingHandler->GetBuffer();

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


    SavimeResult MatchSorted(DatasetPtr ds1, DatasetPtr ds2, DatasetPtr &ds1Mapping,
                             DatasetPtr &ds2Mapping) {

#define ADVANCE(START, LEN, VAL, BUFFER, ENTRY_COUNT) \
        START = START + LEN;\
        if(START < ENTRY_COUNT){\
          LEN = 1; \
          VAL = BUFFER[START]; \
          auto POS = START + LEN; \
          while(POS < ENTRY_COUNT && BUFFER[START] == BUFFER[POS]){ \
              POS++; \
          } \
          LEN = POS - START;\
        }

      int numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
      bool hasRestrictions = ds1->BitMask() != nullptr;

      vector<int64_t> matchesPerThread(numCores);
      std::fill(matchesPerThread.begin(), matchesPerThread.end(), 0);
      vector<int64_t> matchesStartPos(numCores);
      std::fill(matchesStartPos.begin(), matchesStartPos.end(), 0);
      vector<int64_t> matchesFinalPos(numCores);
      std::fill(matchesFinalPos.begin(), matchesFinalPos.end(), 0);

      DatasetHandlerPtr ds1Handler, ds2Handler;
      shared_ptr<SavimeBuffer<T1>> ds1Buffer;
      shared_ptr<SavimeBuffer<T2>> ds2Buffer;
      ds1Handler = _storageManager->GetHandler(ds1);
      ds1Buffer = BUILD_VECTOR<T1>(ds1Handler->GetBuffer(), ds1->GetType());
      ds2Handler = _storageManager->GetHandler(ds2);
      ds2Buffer = BUILD_VECTOR<T2>(ds2Handler->GetBuffer(), ds2->GetType());


      /*
      SET_THREADS(ds1->GetEntryCount(), workPerThread, numCores);
#pragma omp parallel
      {

        int64_t localMatchesPerThread = 0;
        SubTARPosition leftGroupStart = THREAD_FIRST(), leftGroupLen = 0; T1 leftGropuVal;
        SubTARPosition rightGroupStart = 0, rightGroupLen = 0; T2 rightGropuVal;
        ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), THREAD_LAST());
        ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());

        while (leftGroupStart < THREAD_LAST() && rightGroupStart < ds2->GetEntryCount()) {
          if (!hasRestrictions || !(*ds1->BitMask())[leftGroupStart]) {

            if (leftGropuVal == rightGropuVal) {
              localMatchesPerThread += leftGroupLen*rightGroupLen;
            } else if (leftGropuVal > rightGropuVal) {
              ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());
              continue;
            }

          }
          ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), THREAD_LAST());
        }

        matchesPerThread[omp_get_thread_num()] = localMatchesPerThread;
      } // end parallel

*/

      {

        int64_t localMatchesPerThread = 0;
        SubTARPosition leftGroupStart = 0, leftGroupLen = 0;
        T1 leftGropuVal;
        SubTARPosition rightGroupStart = 0, rightGroupLen = 0;
        T2 rightGropuVal;
        ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), ds1->GetEntryCount());
        ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());

        while (leftGroupStart < ds1->GetEntryCount() && rightGroupStart < ds2->GetEntryCount()) {
          if (!hasRestrictions || !(*ds1->BitMask())[leftGroupStart]) {

            if (leftGropuVal == rightGropuVal) {
              localMatchesPerThread += leftGroupLen * rightGroupLen;
            } else if (leftGropuVal > rightGropuVal) {
              ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());
              continue;
            }

          }
          ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), ds1->GetEntryCount());
        }

        matchesPerThread[0] = localMatchesPerThread;

      }

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
        (SubTARPosition *) ds1MapHandler->GetBuffer();
      SubTARPosition *ds2MapBuffer =
        (SubTARPosition *) ds2MapHandler->GetBuffer();

/*
#pragma omp parallel
      {

        int64_t matchCounts = 0;
        int64_t offset = matchesStartPos[omp_get_thread_num()];
        SubTARPosition leftGroupStart = THREAD_FIRST(), leftGroupLen = 0; T1 leftGropuVal;
        SubTARPosition rightGroupStart = 0, rightGroupLen = 0; T2 rightGropuVal;
        ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), THREAD_LAST());
        ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());

        while (leftGroupStart < THREAD_LAST() && rightGroupStart < ds2->GetEntryCount()) {
          if (!hasRestrictions || !(*ds1->BitMask())[leftGroupStart]) {
            if (leftGropuVal == rightGropuVal) {

              for(SubTARPosition lMark = leftGroupStart; lMark < leftGroupStart+leftGroupLen; lMark++){
                for(SubTARPosition rMark = rightGroupStart; rMark < rightGroupStart+rightGroupLen; rMark++){
                  ds1MapBuffer[offset + matchCounts] = lMark;
                  ds2MapBuffer[offset + matchCounts] = rMark;
                  matchCounts++;
                }
              }

            } else if (leftGropuVal > rightGropuVal) {
              ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());
              continue;
            }
          }

          ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), THREAD_LAST());
        }
      } // end parallel

*/
      {
        int64_t matchCounts = 0;
        int64_t offset = matchesStartPos[0];
        SubTARPosition leftGroupStart = 0, leftGroupLen = 0;
        T1 leftGropuVal;
        SubTARPosition rightGroupStart = 0, rightGroupLen = 0;
        T2 rightGropuVal;
        ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), ds1->GetEntryCount());
        ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());

        while (leftGroupStart < ds1->GetEntryCount() && rightGroupStart < ds2->GetEntryCount()) {
          if (!hasRestrictions || !(*ds1->BitMask())[leftGroupStart]) {
            if (leftGropuVal == rightGropuVal) {

              auto leftGroupBound = leftGroupStart + leftGroupLen;
              auto rightGroupBound =  rightGroupStart + rightGroupLen;

              for (SubTARPosition lMark = leftGroupStart; lMark < leftGroupBound; lMark++) {
                for (SubTARPosition rMark = rightGroupStart; rMark < rightGroupBound; rMark++) {
                  //ds1MapBuffer[offset + matchCounts] = lMark;
                  ds1MapBuffer[matchCounts] = lMark;
                  //ds2MapBuffer[offset + matchCounts] = rMark;
                  ds2MapBuffer[matchCounts] = rMark;
                  matchCounts++;
                }
              }

            } else if (leftGropuVal > rightGropuVal) {
              ADVANCE(rightGroupStart, rightGroupLen, rightGropuVal, (*ds2Buffer), ds2->GetEntryCount());
              continue;
            }
          }

          ADVANCE(leftGroupStart, leftGroupLen, leftGropuVal, (*ds1Buffer), ds1->GetEntryCount());
        }
      }

      ds1MapHandler->Close();
      ds2MapHandler->Close();

      ds1Handler->Close();
      ds2Handler->Close();

      return SAVIME_SUCCESS;
    }


    SavimeResult Match(DatasetPtr ds1, DatasetPtr ds2, DatasetPtr &ds1Mapping,
                       DatasetPtr &ds2Mapping) override {

      int numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
      bool hasRestrictions = ds1->BitMask() != nullptr;

      if (ds1->Sorted() && ds2->Sorted())
        return MatchSorted(ds1, ds2, ds1Mapping, ds2Mapping);

      int64_t matchesPerThread[numCores];
      memset(matchesPerThread, 0, numCores * sizeof(int64_t));
      int64_t matchesStartPos[numCores];
      memset(matchesStartPos, 0, numCores * sizeof(int64_t));
      int64_t matchesFinalPos[numCores];
      memset(matchesFinalPos, 0, numCores * sizeof(int64_t));

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
            auto key = (T2) (*ds1Buffer)[i];
            if (!(*ds1->BitMask())[i]) {
              if (parallelHash.map.find(key) != parallelHash.map.end())
                matchesPerThread[omp_get_thread_num()] +=
                  parallelHash.map.count((T2) (*ds1Buffer)[i]);
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
          (SubTARPosition *) ds1MapHandler->GetBuffer();
        SubTARPosition *ds2MapBuffer =
          (SubTARPosition *) ds2MapHandler->GetBuffer();

#pragma omp parallel
        {
          int64_t ds2Pos = 0, matchCounts = 0;
          int64_t offset = matchesStartPos[omp_get_thread_num()];

          for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {

            if ((*ds1->BitMask())[i]) {
              continue;
            }

            auto rangeIt = parallelHash.map.equal_range((T2) (*ds1Buffer)[i]);
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
            auto key = (T2) (*ds1Buffer)[i];
            if (parallelHash.map.find(key) != parallelHash.map.end())
              matchesPerThread[omp_get_thread_num()] +=
                parallelHash.map.count((T2) (*ds1Buffer)[i]);
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
          (SubTARPosition *) ds1MapHandler->GetBuffer();
        SubTARPosition *ds2MapBuffer =
          (SubTARPosition *) ds2MapHandler->GetBuffer();

#pragma omp parallel
        {
          int64_t ds2Pos = 0, matchCounts = 0;
          SubTARPosition offset = matchesStartPos[omp_get_thread_num()];
          for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); i++) {
            auto rangeIt = parallelHash.map.equal_range((T2) (*ds1Buffer)[i]);
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
                          DatasetPtr &ds2Mapping) override {

      DatasetPtr dim1Materialized, dim2Materialized;
      MaterializeDim(dim1, totalLen1, dim1->GetDimension()->GetType(),
                     dim1Materialized);

      MaterializeDim(dim2, totalLen2, dim2->GetDimension()->GetType(),
                     dim2Materialized);

      return Match(dim1Materialized, dim2Materialized, ds1Mapping, ds2Mapping);
    }

    SavimeResult Stretch(DatasetPtr origin, int64_t entryCount,
                         int64_t recordsRepetitions, int64_t datasetRepetitions,
                         DataType type, DatasetPtr &destinyDataset) override {
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

/*
    SavimeResult Reorient(DatasetPtr originDataset, vector<DimSpecPtr> dimSpecs, savime_size_t totalLength,
                          int32_t newMajor, int64_t partitionSize, DatasetPtr &destinyDataset) override {

      int numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);

      auto newMajorSpecs = dimSpecs[newMajor];
      auto currentMajorSpecs = dimSpecs[0];

      bool allSpanToOne = true;
      for (int32_t i = 0; i < newMajor; i++) {
        allSpanToOne &= dimSpecs[i]->GetSpannedLength() == 1;
      }

      if (allSpanToOne) {
        destinyDataset = originDataset;
        return SAVIME_SUCCESS;
      }

      auto originHandler = _storageManager->GetHandler(originDataset);
      auto originBuffer = BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

      destinyDataset = _storageManager->Create(originDataset->GetType(), originDataset->GetEntryCount());
      auto destinyHander = _storageManager->GetHandler(destinyDataset);
      auto destinyBuffer = BUILD_VECTOR<T1>(destinyHander->GetBuffer(), destinyDataset->GetType());

      omp_set_num_threads(numCores);
      int64_t adj = newMajorSpecs->GetAdjacency();
      int64_t dimLen = newMajorSpecs->GetSpannedLength();
      int64_t stride = newMajorSpecs->GetStride();
      int64_t currentAdj = currentMajorSpecs->GetAdjacency();

      int64_t adjMul, adjShift;
      fast_division(totalLength, adj, adjMul, adjShift);
      int64_t dimLenMul, dimLenShift;
      fast_division(totalLength, dimLen, dimLenMul, dimLenShift);

//      SET_THREADS(partitions, 1, numCores);
//#pragma omp parallel
//      for (int64_t partitionNo = THREAD_FIRST(); partitionNo < THREAD_LAST(); partitionNo++) {
//        int64_t partitionOffset = partitionNo * partitionSize;
//        int64_t partitionEnd = partitionOffset + partitionSize;
        vector<int64_t> acc(newMajorSpecs->GetSpannedLength());
        std::fill(acc.begin(), acc.end(), 0);

        for (SubTARPosition i = 0; i < totalLength; i++) {
          //SLOW DIVISION: int64_t offset = (i/adj)%dimLen;
          int64_t preamble = DIVIDE(i, adjMul, adjShift);
          int64_t idx = REMAINDER(preamble, dimLenMul, dimLenShift, dimLen);
          int64_t offset = acc[idx];
          acc[idx]++;
          //printf("%ld %ld %ld\n", partitionNo, idx*partitionAdj + offset, i);
          //partitionsData[partitionNo][idx * partitionAdj + offset] = (*originBuffer)[i];
          (*destinyBuffer)[idx * currentAdj + offset] = (*originBuffer)[i];
        }
      //}

      originHandler->Close();
      destinyHander->Close();

    }
*/


    SavimeResult Reorient(DatasetPtr originDataset, vector<DimSpecPtr> dimSpecs, savime_size_t totalLength,
                          int32_t newMajor, int64_t partitionSize, DatasetPtr &destinyDataset) override {

      int numCores = _configurationManager->GetIntValue(MAX_THREADS);
      int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);

      auto newMajorSpecs = dimSpecs[newMajor];
      auto currentMajorSpecs = dimSpecs[0];

      bool allSpanToOne = true;
      for (int32_t i = 0; i < newMajor; i++) {
        allSpanToOne &= dimSpecs[i]->GetSpannedLength() == 1;
      }

      if (allSpanToOne) {
        destinyDataset = originDataset;
        return SAVIME_SUCCESS;
      }

      if (partitionSize % newMajorSpecs->GetStride() != 0)
        throw std::runtime_error("Invalid partition size.");

      if (originDataset->GetType().isVector())
        throw std::runtime_error("Unsupported split with vector datasets.");


      savime_size_t partitions = totalLength / partitionSize;

      vector<vector<T1>> partitionsData(partitions);

      for (auto &partitionData : partitionsData) {
        partitionData.resize(partitionSize);
      }

      auto originHandler = _storageManager->GetHandler(originDataset);
      auto originBuffer = BUILD_VECTOR<T1>(originHandler->GetBuffer(), originDataset->GetType());

      destinyDataset = _storageManager->Create(originDataset->GetType(), originDataset->GetEntryCount());
      auto destinyHander = _storageManager->GetHandler(destinyDataset);
      auto destinyBuffer = BUILD_VECTOR<T1>(destinyHander->GetBuffer(), destinyDataset->GetType());

      omp_set_num_threads(numCores);
      int64_t adj = newMajorSpecs->GetAdjacency();
      int64_t dimLen = newMajorSpecs->GetSpannedLength();
      int64_t stride = newMajorSpecs->GetStride();
      int64_t partitionAdj = partitionSize / stride;

      int64_t adjMul, adjShift;
      fast_division(totalLength, adj, adjMul, adjShift);
      int64_t dimLenMul, dimLenShift;
      fast_division(totalLength, dimLen, dimLenMul, dimLenShift);

      SET_THREADS(partitions, 1, numCores);
#pragma omp parallel
      for (int64_t partitionNo = THREAD_FIRST(); partitionNo < THREAD_LAST(); partitionNo++) {
        int64_t partitionOffset = partitionNo * partitionSize;
        int64_t partitionEnd = partitionOffset + partitionSize;
        vector<int64_t> acc(newMajorSpecs->GetSpannedLength());
        std::fill(acc.begin(), acc.end(), 0);

        for (SubTARPosition i = partitionOffset; i < partitionEnd; i++) {
          //SLOW DIVISION: int64_t offset = (i/adj)%dimLen;
          int64_t preamble = DIVIDE(i, adjMul, adjShift);
          int64_t idx = REMAINDER(preamble, dimLenMul, dimLenShift, dimLen);
          int64_t offset = acc[idx];
          acc[idx]++;
          //printf("%ld %ld %ld\n", partitionNo, idx*partitionAdj + offset, i);
          partitionsData[partitionNo][idx * partitionAdj + offset] = (*originBuffer)[i];
        }
      }

      //int64_t linesNumber = partitionSize / adj;
      int64_t linesNumber = dimLen;
      //SubTARPosition i = 0;

      RESET_THREADS(linesNumber, 1, numCores);
#pragma omp parallel
      for (int64_t line = THREAD_FIRST(); line < THREAD_LAST(); line++) {
        //for(int64_t line = 0; line < linesNumber; line++){
        for (int32_t partitionNo = 0; partitionNo < partitions; partitionNo++) {
          for (int32_t currentAdj = 0; currentAdj < partitionAdj; currentAdj++) {

            SubTARPosition i = line * partitions * partitionAdj + partitionNo * partitionAdj + currentAdj;
            //printf("-%ld %ld %ld\n", partitionNo, line + currentAdj, i);
            (*destinyBuffer)[i] = partitionsData[partitionNo][line*partitionAdj + currentAdj];
          }
        }
      }

      originHandler->Close();
      destinyHander->Close();

    }


    SavimeResult Split(DatasetPtr origin, int64_t totalLength, int64_t parts,
                       vector<DatasetPtr> &brokenDatasets) override {

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
      T1 *originBuffer = (T1 *) originHandler->GetBuffer();
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
        buffers[i] = (T1 *) handlers[i]->GetBuffer();
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