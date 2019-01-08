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
#include <stdio.h>
#include <algorithm>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>
#include <time.h>
#include <omp.h>
#include <chrono>
#include "mock_default_storage_manager.h"

std::unordered_map<string, void *> MockDefaultDatasetHandler::_buffers;
std::mutex _buffers_lock;

MockDefaultDatasetHandler::MockDefaultDatasetHandler(
    DatasetPtr ds, StorageManagerPtr storageManager, int64_t hugeTblThreshold,
    int64_t hugeTblSize)
    : DatasetHandler(ds) {


  _storageManager = storageManager;
  _huge_pages_size = hugeTblSize;
  _buffer_offset = 0;
  _buffer = nullptr;

  if (ds == nullptr) {
    throw std::runtime_error("Invalid dataset for handler creation.");
  }

  _buffers_lock.lock();
  _entry_length = TYPE_SIZE(ds->GetType());
  _buffer = _buffers[ds->GetLocation()];
  _buffers_lock.unlock();

  if (_buffer == nullptr) {
    _mapping_length = ds->GetLength();
    _buffer = malloc(_mapping_length+100);
    _buffers_lock.lock();
    _buffers[ds->GetLocation()] = _buffer;
    _buffers_lock.unlock();
    if (_buffer == nullptr)
      throw std::runtime_error("Could not malloc buffer for dataset");
  }

  _open = true;
}

int32_t MockDefaultDatasetHandler::GetValueLength() { return _entry_length; }

void MockDefaultDatasetHandler::Remap() {}

void MockDefaultDatasetHandler::Append(void *value) {}

void *MockDefaultDatasetHandler::Next() {
  if (_buffer_offset < _mapping_length) {
    char *v = &((char *)_buffer)[_buffer_offset];
    _buffer_offset += _entry_length;
    return (void *)v;
  } else if (_buffer_offset < _ds->GetLength()) {
    Remap();
    char *v = &((char *)_buffer)[_buffer_offset];
    _buffer_offset += _entry_length;
    return (void *)v;
  }

  return nullptr;
}

bool MockDefaultDatasetHandler::HasNext() {
  return _buffer_offset < _ds->GetLength();
}

void MockDefaultDatasetHandler::InsertAt(void *value, RealIndex offset) {
  offset = offset * _entry_length;
  memcpy(&((char *)_buffer)[offset], value, _entry_length);
}

void MockDefaultDatasetHandler::CursorAt(RealIndex index) {
  _buffer_offset = index * _entry_length;
}

void *MockDefaultDatasetHandler::GetBuffer() { return _buffer; }

void *MockDefaultDatasetHandler::GetBufferAt(RealIndex index) {
  int64_t buffer_offset = index * _entry_length;
  if (_ds->GetLength() > buffer_offset) {
    if (_ds->GetLength() > _mapping_length)
      Remap();

    return &((char *)_buffer)[index * _entry_length];
  } else {
    return nullptr;
  }
}

void MockDefaultDatasetHandler::TruncateAt(RealIndex index) {}

void MockDefaultDatasetHandler::Close() { _open = false; }

DatasetPtr MockDefaultDatasetHandler::GetDataSet() { return _ds; }

MockDefaultDatasetHandler::~MockDefaultDatasetHandler() {

}

//-----------------------------------------------------------------------------
// Storage Manager Members
DatasetPtr MockDefaultStorageManager::Create(DataType type,
                                             savime_size_t entries) {
  try {

    if (entries <= 0) {
      throw runtime_error("Attempt to create a dataset with invalid size.");
    }

    int error, typeSize;
    typeSize = TYPE_SIZE(type);
    auto name = generateUniqueFileName(MOCK_FILE_PREFIX, 1);
    auto size = entries * typeSize;

    // Checking size
    DatasetPtr ds = make_shared<Dataset>(UNSAVED_ID, name, size, type);
    ds->Sorted() = false;
    ds->HasIndexes() = false;
    ds->AddListener(GetOwn());
    ds->GetLocation() = name;
    return ds;

  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return nullptr;
  }
}

DatasetPtr MockDefaultStorageManager::Create(DataType type, double init,
                                         double spacing, double end,
                                         int64_t rep) {
  DatasetPtr newDataset;

  DimensionPtr dummyDimension =
      make_shared<Dimension>(UNSAVED_ID, "", type, init, end, spacing);

  DimSpecPtr dummySpecs = make_shared<DimensionSpecification>(
      UNSAVED_ID, dummyDimension, 0, dummyDimension->GetLength() - 1, 1, rep);

  MaterializeDim(dummySpecs, dummySpecs->GetFilledLength()*rep, newDataset);

  return newDataset;
}

DatasetPtr MockDefaultStorageManager::Create(DataType type,
                                         vector<string> literals) {
#define BLANK '\0'

  DatasetPtr newDataset;

  if (type != CHAR)
    newDataset = Create(type, literals.size() / type.vectorLength());
  else
    newDataset = Create(type, literals.size());

  DatasetHandlerPtr handler = GetHandler(newDataset);

  switch (type.type()) {
  case CHAR: {
    int8_t *buffer = (int8_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      string literal = literals[i];
      literal.erase(std::remove(literal.begin(), literal.end(), '"'),
                    literal.end());

      int64_t len =
          std::min((int64_t)literal.size(), (int64_t)type.vectorLength());
      for (int64_t j = 0; j < len; j++) {
        buffer[i * type.vectorLength() + j] = (int8_t)literal.at(j);
      }

      /*Adding padding*/
      for (int64_t j = len; j < type.vectorLength(); j++) {
        buffer[i * type.vectorLength() + j] = (int8_t)BLANK;
      }
    }
    break;
  }
#ifdef FULL_TYPE_SUPPORT
    case INT8: {
    int8_t *buffer = (int8_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (int8_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
  case INT16: {
    int16_t *buffer = (int16_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (int16_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
#endif
  case INT32: {
    int32_t *buffer = (int32_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (int32_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
  case INT64: {
    int64_t *buffer = (int64_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (int64_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
#ifdef FULL_TYPE_SUPPORT
    case UINT8: {
    uint8_t *buffer = (uint8_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (uint8_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
  case UINT16: {
    uint16_t *buffer = (uint16_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (uint16_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
  case UINT32: {
    uint32_t *buffer = (uint32_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (uint32_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
  case UINT64: {
    uint64_t *buffer = (uint64_t *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (uint64_t)strtol(literals[i].c_str(), NULL, 10);
    }
    break;
  }
#endif
  case FLOAT: {
    float *buffer = (float *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (float)strtod(literals[i].c_str(), NULL);
    }
    break;
  }
  case DOUBLE: {
    double *buffer = (double *)handler->GetBuffer();
    for (int32_t i = 0; i < literals.size(); i++) {
      buffer[i] = (double)strtod(literals[i].c_str(), NULL);
    }
    break;
  }
  }

  handler->Close();
  return newDataset;
}

DatasetHandlerPtr MockDefaultStorageManager::GetHandler(DatasetPtr dataset) {
  int64_t hugeTblThreshold =
      _configurationManager->GetLongValue(HUGE_TBL_THRESHOLD);

  int64_t hugeTblSize = _configurationManager->GetLongValue(HUGE_TBL_SIZE);

  DatasetHandlerPtr handler = DatasetHandlerPtr(new MockDefaultDatasetHandler(
      dataset, GetOwn(), hugeTblThreshold, hugeTblSize));
  return handler;
}

void MockDefaultStorageManager::FromBitMaskToPosition(DatasetPtr &dataset,
                                                      bool keepBitmask) {

  int numCores = _configurationManager->GetIntValue(MAX_THREADS);
  int workPerThread = _configurationManager->GetIntValue(WORK_PER_THREAD);
  int64_t startPositionPerCore[numCores];
  int64_t finalPositionPerCore[numCores];

  if (dataset->BitMask() == nullptr)
    throw std::runtime_error("Dataset does not contain a bitmask.");

  int32_t numThreads = SetWorkloadPerThread(dataset->BitMask()->num_blocks(),
                                            workPerThread, startPositionPerCore,
                                            finalPositionPerCore, numCores);
  int64_t sizePerChunk[numCores];
  int64_t totalLen = 0, offsetPerChunk[numCores];

  for (int32_t i = 0; i < numThreads; i++) {
    sizePerChunk[i] = dataset->BitMask()->count_parallel(
        startPositionPerCore[i], finalPositionPerCore[i], numCores,
        workPerThread);

    startPositionPerCore[i] *= dataset->GetBitsPerBlock();
    finalPositionPerCore[i] *= dataset->GetBitsPerBlock();
    finalPositionPerCore[i] =
        std::max(finalPositionPerCore[i], (int64_t)dataset->BitMask()->size());
  }

  // calculating offset per chunk
  for (int32_t i = 0; i < numThreads; i++) {
    offsetPerChunk[i] = totalLen;
    totalLen += sizePerChunk[i];
  }

  // if no bit is set return empty dataset
  if (totalLen == 0) {
    dataset = make_shared<Dataset>(0);
    return;
  }

  // Dataset must be created
  auto auxDataSet = this->Create(DataType(SUBTAR_POSITION, 1), totalLen);
  if (auxDataSet == nullptr)
    throw std::runtime_error("Could not create a dataset.");

  // Setting parameter dataset with newly created dataset to hold positions
  // dataset->Redefine(dataset->GetId(), dataset->GetName(),
  //                  auxDataSet->GetLocation(), auxDataSet->GetType());

  // To avoid file removal by destructor
  // auxDataSet->ClearListeners();

  DatasetHandlerPtr handler = this->GetHandler(auxDataSet);
  SubTARPosition *buffer = (SubTARPosition *)handler->GetBuffer();

// creating index set from bitmap
#pragma omp parallel
  {
    int64_t i = 0, index;
    if (startPositionPerCore[omp_get_thread_num()] == 0)
      index = dataset->BitMask()->find_first();
    else
      index = dataset->BitMask()->find_next(
          startPositionPerCore[omp_get_thread_num()]);

    while (index <= finalPositionPerCore[omp_get_thread_num()] &&
        index != dataset->BitMask()->npos) {
      buffer[offsetPerChunk[omp_get_thread_num()] + i] = index;
      index = dataset->BitMask()->find_next(index);
      i++;
    }
  }

  dataset = auxDataSet;

  dataset->HasIndexes() = true;
  dataset->Sorted() = true;
  handler->Close();

  if (!keepBitmask)
    dataset->BitMask()->clear();
}


void MockDefaultStorageManager::DisposeObject(MetadataObject *object) {
  if (auto dataset = dynamic_cast<Dataset *>(object)) {
    _buffers_lock.lock();
    auto pos = MockDefaultDatasetHandler::_buffers.find(dataset->GetLocation());
    if (pos != MockDefaultDatasetHandler::_buffers.end()) {
      free(pos->second);
      MockDefaultDatasetHandler::_buffers.erase(dataset->GetLocation());
   }
    _buffers_lock.unlock();
  }
}