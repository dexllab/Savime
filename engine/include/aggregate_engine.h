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
#ifndef AGGREGATE_ENGINE_H
#define AGGREGATE_ENGINE_H

#include "../core/include/abstract_storage_manager.h"
#include "aggregate_config.h"

struct IsNull {
    bool operator()(const AggregateBufferPtr &aggBuffer) {
      return aggBuffer == nullptr;
    }
};

class AbstractAggregateEngine {
public:
    virtual void Run(AggregateBufferPtr aggBuffer,
                     AggregateBufferPtr aggAuxBuffer, int64_t size) = 0;
    virtual void Reduce(AggregateBufferPtr buffer, AggregateBufferPtr auxBuffer,
                        vector<AggregateBufferPtr> outputBuffers,
                        vector<AggregateBufferPtr> outputAuxBuffers,
                        int64_t bufferSize) = 0;
    virtual void Finalize(AggregateBufferPtr buffer, AggregateBufferPtr auxBuffer,
                          int64_t bufferSize) = 0;
};

typedef std::shared_ptr<AbstractAggregateEngine> AbstractAggregateEnginePtr;

template <class T> class AggregateEngine : public AbstractAggregateEngine {
    AggregateConfigurationPtr _aggConfig;
    AggregateFunctionPtr _function;
    int64_t _subtarLen;
    int32_t _numCores;
    int32_t _minWork;

public:
    AggregateEngine(AggregateConfigurationPtr aggConfig,
                    AggregateFunctionPtr function, int64_t subtarLen,
                    int64_t numCores, int64_t minWork) {
      _aggConfig = aggConfig;
      _function = function;
      _subtarLen = subtarLen;
      _numCores = static_cast<int32_t>(numCores);
      _minWork = static_cast<int32_t>(minWork);
    }

    inline void CalcAvg(SavimeBufferPtr<T> buffer,
                        vector<AggregateBufferPtr> outputBuffers,
                        vector<AggregateBufferPtr> outputAuxBuffers, int64_t subtarLen) {

      SET_THREADS_ALIGNED(subtarLen, _minWork, _numCores,
                          EXPECTED_BITS_PER_BLOCK);

#pragma omp parallel
      for (int64_t i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {

        SubTARPosition linearPos =
          _aggConfig->GetLinearPosition(outputBuffers[omp_get_thread_num()], i);
        auto v = (*buffer)[i];
        (*outputBuffers[omp_get_thread_num()])[linearPos] += (*buffer)[i];
        outputBuffers[omp_get_thread_num()]->SetBit(linearPos);
        (*outputAuxBuffers[omp_get_thread_num()])[linearPos]++;
      }
    }

    inline void ReduceAvg(const AggregateBufferPtr &buffer, const AggregateBufferPtr &auxBuffer,
                          vector<AggregateBufferPtr> outputBuffers,
                          vector<AggregateBufferPtr> outputAuxBuffers,
                          int64_t bufferSize) {


      if (_aggConfig->mode == HASHED) {

        for (int32_t b = 0; b < outputBuffers.size(); b++) {

          if (outputBuffers[b] == nullptr)
            continue;

          for (auto entry : *(outputBuffers[b]->getIndexesMap())) {
            auto linearIndex = entry.first;
            auto indexes = entry.second;
            (*buffer)[indexes] += (*(outputBuffers[b]))[linearIndex];
          }

          for (auto entry : *(outputBuffers[b]->getIndexesMap())) {
            auto linearIndex = entry.first;
            auto indexes = entry.second;
            (*auxBuffer)[indexes] += (*(outputAuxBuffers[b]))[linearIndex];
          }
        }

      } else {

        SET_THREADS_ALIGNED(bufferSize, _minWork, _numCores,
                            EXPECTED_BITS_PER_BLOCK);


#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          double positionSum = 0;
          double positionCount = 0;
          bool bit = false;
          for (int32_t b = 0; b < outputBuffers.size(); b++) {
            positionSum += (*(outputBuffers[b]))[i];
            positionCount += (*(outputAuxBuffers[b]))[i];
            bit |= outputBuffers[b]->GetBit(i);
          }
          (*buffer)[i] += positionSum;
          (*auxBuffer)[i] += positionCount;
          if (bit)
            buffer->SetBit(i);
        }
      }
    }

    inline void FinalizeAvg(const AggregateBufferPtr &buffer, const AggregateBufferPtr &auxBuffer,
                            int64_t bufferSize) {

      if (_aggConfig->mode == HASHED) {

        for (auto entry : *(buffer->getIndexesMap())) {
          auto linearIndex = entry.first;
          if ((*auxBuffer)[linearIndex] > 0.0)
            (*buffer)[linearIndex] /= (*auxBuffer)[linearIndex];
          else
            (*buffer)[linearIndex] = 0;
        }

      } else {

        SET_THREADS_ALIGNED(bufferSize, _minWork, _numCores,
                            EXPECTED_BITS_PER_BLOCK);

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          if ((*auxBuffer)[i] > 0)
            (*buffer)[i] = (*buffer)[i] / (*auxBuffer)[i];
        }
      }
    }

    inline void CalcSum(SavimeBufferPtr<T> buffer,
                        vector<AggregateBufferPtr> outputBuffers, int64_t subtarLen) {

      SET_THREADS_ALIGNED(subtarLen, _minWork, _numCores,
                          EXPECTED_BITS_PER_BLOCK);

#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          SubTARPosition linearPos =
            _aggConfig->GetLinearPosition(outputBuffers[omp_get_thread_num()], i);
          (*outputBuffers[omp_get_thread_num()])[linearPos] += (*buffer)[i];
          outputBuffers[omp_get_thread_num()]->SetBit(linearPos);
        }
    }

    inline void ReduceSum(const AggregateBufferPtr &buffer,
                          vector<AggregateBufferPtr> outputBuffers, int64_t bufferSize) {

      if (_aggConfig->mode == HASHED) {

        for (int32_t b = 0; b < outputBuffers.size(); b++) {
          for (auto entry : *(outputBuffers[b]->getIndexesMap())) {
            auto linearIndex = entry.first;
            auto indexes = entry.second;
            (*buffer)[indexes] += (*outputBuffers[b])[linearIndex];
          }
        }

      } else {
        SET_THREADS_ALIGNED(bufferSize, _minWork, _numCores,
                            EXPECTED_BITS_PER_BLOCK);
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          double positionSum = 0;
          bool bit = false;
          for (int32_t b = 0; b < outputBuffers.size(); b++) {
            positionSum += (*outputBuffers[b])[i];
            bit |= outputBuffers[b]->GetBit(i);
          }
          (*buffer)[i] += positionSum;
          if (bit)
            buffer->SetBit(i);
        }
      }
    }

    inline void CalcMin(SavimeBufferPtr<T> buffer,
                        vector<AggregateBufferPtr> outputBuffers, int64_t subtarLen) {

      SET_THREADS_ALIGNED(subtarLen, _minWork, _numCores,
                          EXPECTED_BITS_PER_BLOCK);

      if (_aggConfig->mode == BUFFERED) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          SubTARPosition linearPos = _aggConfig->GetLinearPosition(
            outputBuffers[omp_get_thread_num()], i);
          (*outputBuffers[omp_get_thread_num()])[linearPos] =
            std::numeric_limits<double>::max();
        }
      }

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        SubTARPosition linearPos =
          _aggConfig->GetLinearPosition(outputBuffers[omp_get_thread_num()], i);
        if ((*buffer)[i] < (*outputBuffers[omp_get_thread_num()])[linearPos]) {
          (*outputBuffers[omp_get_thread_num()])[linearPos] = (*buffer)[i];
        }
        outputBuffers[omp_get_thread_num()]->SetBit(linearPos);
      }
    }

    inline void ReduceMin(const AggregateBufferPtr &buffer,
                          vector<AggregateBufferPtr> outputBuffers, int64_t bufferSize) {

      if (_aggConfig->mode == HASHED) {

        for (int32_t b = 0; b < outputBuffers.size(); b++) {
          for (auto entry : *(outputBuffers[b]->getIndexesMap())) {
            auto linearIndex = entry.first;
            auto indexes = entry.second;
            double v = (*outputBuffers[b])[linearIndex];
            double bv = (*buffer)[indexes];
            if (v < bv)
              (*buffer)[indexes] = v;
          }
        }

      } else {
        SET_THREADS_ALIGNED(bufferSize, _minWork, _numCores,
                            EXPECTED_BITS_PER_BLOCK);
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          double min = std::numeric_limits<double>::max();
          bool bit = false;
          for (int32_t b = 0; b < outputBuffers.size(); b++) {
            double bvalue = (*outputBuffers[b])[i];
            bit |= outputBuffers[b]->GetBit(i);
            if (bvalue < min)
              min = bvalue;
          }
          if (min < (*buffer)[i])
            (*buffer)[i] = min;

          if (bit)
            buffer->SetBit(i);
        }
      }
    }

    inline void CalcMax(SavimeBufferPtr<T> buffer,
                        vector<AggregateBufferPtr> outputBuffers, int64_t subtarLen) {

      SET_THREADS_ALIGNED(subtarLen, _minWork, _numCores,
                          EXPECTED_BITS_PER_BLOCK);

      if (_aggConfig->mode == BUFFERED) {
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          SubTARPosition linearPos = _aggConfig->GetLinearPosition(
            outputBuffers[omp_get_thread_num()], i);
          (*outputBuffers[omp_get_thread_num()])[linearPos] =
            std::numeric_limits<double>::min();
        }
      }

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        SubTARPosition linearPos =
          _aggConfig->GetLinearPosition(outputBuffers[omp_get_thread_num()], i);
        ;
        if ((*buffer)[i] > (*outputBuffers[omp_get_thread_num()])[linearPos]) {
          (*outputBuffers[omp_get_thread_num()])[linearPos] = (*buffer)[i];
        }
        outputBuffers[omp_get_thread_num()]->SetBit(linearPos);
      }
    }

    inline void ReduceMax(const AggregateBufferPtr &buffer,
                          vector<AggregateBufferPtr> outputBuffers, int64_t bufferSize) {

      if (_aggConfig->mode == HASHED) {

        for (int32_t b = 0; b < outputBuffers.size(); b++) {
          for (auto entry : *(outputBuffers[b]->getIndexesMap())) {
            auto linearIndex = entry.first;
            auto indexes = entry.second;
            double v = (*outputBuffers[b])[linearIndex];
            if (v > (*buffer)[indexes])
              (*buffer)[indexes] = v;
          }
        }

      } else {

        SET_THREADS_ALIGNED(bufferSize, _minWork, _numCores,
                            EXPECTED_BITS_PER_BLOCK);
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          double max = std::numeric_limits<double>::min();
          bool bit = false;
          for (int32_t b = 0; b < outputBuffers.size(); b++) {
            double bvalue = (*outputBuffers[b])[i];
            if (bvalue > max)
              max = bvalue;
            bit |= outputBuffers[b]->GetBit(i);
          }
          if (max > (*buffer)[i])
            (*buffer)[i] = max;

          if (bit)
            buffer->SetBit(i);
        }
      }
    }

    inline void CalcCount(SavimeBufferPtr<T> buffer,
                          vector<AggregateBufferPtr> outputBuffers, int64_t subtarLen) {

      SET_THREADS_ALIGNED(subtarLen, _minWork, _numCores,
                          EXPECTED_BITS_PER_BLOCK);

#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
        SubTARPosition linearPos =
          _aggConfig->GetLinearPosition(outputBuffers[omp_get_thread_num()], i);
        (*outputBuffers[omp_get_thread_num()])[linearPos]++;
        outputBuffers[omp_get_thread_num()]->SetBit(linearPos);
      }
    }

    inline void ReduceCount(const AggregateBufferPtr &buffer,
                            vector<AggregateBufferPtr> outputBuffers,
                            int64_t bufferSize) {

      if (_aggConfig->mode == HASHED) {

        for (int32_t b = 0; b < outputBuffers.size(); b++) {
          for (auto entry : *(outputBuffers[b]->getIndexesMap())) {
            auto linearIndex = entry.first;
            auto indexes = entry.second;
            (*buffer)[indexes] += (*outputBuffers[b])[linearIndex];
          }
        }

      } else {

        SET_THREADS_ALIGNED(bufferSize, _minWork, _numCores,
                            EXPECTED_BITS_PER_BLOCK);
#pragma omp parallel
        for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
          double positionSum = 0;
          bool bit = false;
          for (int32_t b = 0; b < outputBuffers.size(); b++) {
            positionSum += (*outputBuffers[b])[i];
            bit |= outputBuffers[b]->GetBit(i);
          }
          (*buffer)[i] += positionSum;

          if (bit)
            buffer->SetBit(i);
        }
      }
    }

    void Run(AggregateBufferPtr outputBuffer, AggregateBufferPtr auxOutputBuffer,
             int64_t size) override {

      /*Creating buffers*/
      vector<AggregateBufferPtr> partialBuffers(_numCores);
      vector<AggregateBufferPtr> partialAuxBuffers(_numCores);

      T *inputDataBuffer =
        (T *) _aggConfig->inputHandlers[_function->paramName]->GetBuffer();
      auto type =
        _aggConfig->inputHandlers[_function->paramName]->GetDataSet()->GetType();

      auto inputData = BUILD_VECTOR<T>(inputDataBuffer, type);

      if(_numCores > 1) {

        if (_aggConfig->mode == HASHED) {

          for (int32_t i = 0; i < _numCores; i++) {
            AggregateBufferPtr partialBuffer =
              AggregateBufferPtr(new MapAggregateBuffer(
                _aggConfig->dimensions, _function->GetStartValue()));
            partialBuffers[i] = partialBuffer;
          }

          if (_function->RequiresAuxDataset()) {
            for (int32_t i = 0; i < _numCores; i++) {
              AggregateBufferPtr partialAuxBuffer =
                AggregateBufferPtr(new MapAggregateBuffer(
                  _aggConfig->dimensions, _function->GetStartValue()));
              partialAuxBuffers[i] = partialAuxBuffer;
            }
          }

        } else {
          for (int32_t i = 0; i < _numCores; i++) {
            AggregateBufferPtr partialBuffer =
              AggregateBufferPtr(new VectorAggregateBuffer(
                _aggConfig->dimensions, size, _function->GetStartValue()));
            partialBuffers[i] = partialBuffer;
          }

          if (_function->RequiresAuxDataset()) {
            for (int32_t i = 0; i < _numCores; i++) {
              AggregateBufferPtr partialAuxBuffer = AggregateBufferPtr(
                new VectorAggregateBuffer(_aggConfig->dimensions, size, 0));
              partialAuxBuffers[i] = partialAuxBuffer;
            }
          }
        }

        if (_function->function == (AVG_FUNCTION)) {
          CalcAvg(inputData, partialBuffers, partialAuxBuffers, _subtarLen);
          ReduceAvg(outputBuffer, auxOutputBuffer, partialBuffers,
                    partialAuxBuffers, size);
        } else if (_function->function == (SUM_FUNCTION)) {
          CalcSum(inputData, partialBuffers, _subtarLen);
          ReduceSum(outputBuffer, partialBuffers, size);
        } else if (_function->function == (MIN_FUNCTION)) {
          CalcMin(inputData, partialBuffers, _subtarLen);
          ReduceMin(outputBuffer, partialBuffers, size);
        } else if (_function->function == (MAX_FUNCTION)) {
          CalcMax(inputData, partialBuffers, _subtarLen);
          ReduceMax(outputBuffer, partialBuffers, size);
        } else if (_function->function == (COUNT_FUNCTION)) {
          CalcCount(inputData, partialBuffers, _subtarLen);
          ReduceCount(outputBuffer, partialBuffers, size);
        }

      } else {

        printf("diff here %ld address\n", (int64_t )outputBuffer.get());

        if (_function->function == (AVG_FUNCTION)) {
          CalcAvg(inputData, {outputBuffer}, {auxOutputBuffer}, _subtarLen);
        } else if (_function->function == (SUM_FUNCTION)) {
          CalcSum(inputData, {outputBuffer}, _subtarLen);
        } else if (_function->function == (MIN_FUNCTION)) {
          CalcMin(inputData, {outputBuffer}, _subtarLen);
        } else if (_function->function == (MAX_FUNCTION)) {
          CalcMax(inputData, {outputBuffer}, _subtarLen);
        } else if (_function->function == (COUNT_FUNCTION)) {
          CalcCount(inputData, {outputBuffer}, _subtarLen);
        }
      }
    }

    void Reduce(AggregateBufferPtr aggBuffer, AggregateBufferPtr aggAuxBuffer,
                vector<AggregateBufferPtr> outputBuffers,
                vector<AggregateBufferPtr> outputAuxBuffers, int64_t size) override {

      outputBuffers.erase(
        std::remove_if(outputBuffers.begin(), outputBuffers.end(), IsNull()),
        outputBuffers.end());
      outputAuxBuffers.erase(std::remove_if(outputAuxBuffers.begin(),
                                            outputAuxBuffers.end(), IsNull()),
                             outputAuxBuffers.end());

      if (outputBuffers.empty())
        return;

      if (_function->function == (AVG_FUNCTION)) {
        ReduceAvg(aggBuffer, aggAuxBuffer, outputBuffers, outputAuxBuffers, size);
      } else if (_function->function == (SUM_FUNCTION)) {
        ReduceSum(aggBuffer, outputBuffers, size);
      } else if (_function->function == (MIN_FUNCTION)) {
        ReduceMin(aggBuffer, outputBuffers, size);
      } else if (_function->function == (MAX_FUNCTION)) {
        ReduceMax(aggBuffer, outputBuffers, size);
      } else if (_function->function == (COUNT_FUNCTION)) {
        ReduceCount(aggBuffer, outputBuffers, size);
      }
    }

    void Finalize(AggregateBufferPtr buffer, AggregateBufferPtr auxBuffer,
                  int64_t bufferSize) override {
      if (_function->function == (AVG_FUNCTION)) {
        FinalizeAvg(buffer, auxBuffer, bufferSize);
      }
    }
};

inline AbstractAggregateEnginePtr buildAggregateEngine(
  AggregateConfigurationPtr aggConfig, AggregateFunctionPtr func,
  DataType type, int64_t subtarLen, int64_t numCores, int64_t workPerThread) {

  AbstractAggregateEnginePtr engine;

#ifdef FULL_TYPE_SUPPORT
  if (type == INT8) {
    engine = std::make_shared<AggregateEngine<int8_t>>(
        aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == INT16) {
    engine = std::make_shared<AggregateEngine<int16_t>>(
        aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == UINT8) {
    engine = std::make_shared<AggregateEngine<uint8_t>>(
        aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == UINT16) {
    engine = std::make_shared<AggregateEngine<uint16_t>>(
        aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == UINT32) {
    engine = std::make_shared<AggregateEngine<uint32_t>>(
        aggConfig, func, subtarLen, numCores, workPerThread);
  }
  } else if (type == UINT64) {
    engine = std::make_shared<AggregateEngine<uint6_t4>>(
        aggConfig, func, subtarLen, numCores, workPerThread);
  }
#endif
  if (type == INT32) {
    engine = std::make_shared<AggregateEngine<int32_t>>(
      aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == INT64) {
    engine = std::make_shared<AggregateEngine<int64_t>>(
      aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == FLOAT) {
    engine = std::make_shared<AggregateEngine<float>>(
      aggConfig, func, subtarLen, numCores, workPerThread);
  } else if (type == DOUBLE) {
    engine = std::make_shared<AggregateEngine<double>>(
      aggConfig, func, subtarLen, numCores, workPerThread);
  } else {
    throw runtime_error(type.toString() +" is not supported for aggregations.");
  }

  return engine;
}

#endif /* AGGREGATE_ENGINE_H */