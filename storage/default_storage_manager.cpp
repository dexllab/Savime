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
#include "../core/include/util.h"
#include "../core/include/dynamic_bitset.h"
#include "template_builder.h"
#include "default_storage_manager.h"


using namespace std;
using namespace std::chrono;

#define PRINT_TIME_INFO(FUNCTION)                                              \
  GET_T2();                                                                    \
  _systemLogger->LogEvent(_moduleName, string(FUNCTION) + " took " +           \
                                           to_string(GET_DURATION()) +         \
                                           " ms.");

#define BUF2STR(OUT, BUFFER, IDX, SIZE)                                        \
   {                                                                           \
     char strbuffer[SIZE];                                                     \
     memcpy(strbuffer, &(*BUFFER)[IDX], SIZE);                                 \
     OUT = strbuffer;                                                          \
   }

std::mutex TemplateBuilder::templateBuilderMutex;

DefaultDatasetHandler::DefaultDatasetHandler(DatasetPtr ds,
                                             StorageManagerPtr storageManager,
                                             int64_t hugeTblThreshold,
                                             int64_t hugeTblSize)
  : DatasetHandler(ds) {

  _storageManager = storageManager;
  _huge_pages_size = hugeTblSize;
  _buffer_offset = 0;
  _buffer = nullptr;

  if (ds == nullptr) {
    throw std::runtime_error("Invalid dataset for handler creation.");
  }

  _entry_length = TYPE_SIZE(ds->GetType());
  _fd = open(ds->GetLocation().c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);

  if (_fd == -1) {

    throw std::runtime_error("Could not open dataset file: " +
      ds->GetLocation() + " Error: " +
      std::string(strerror(errno)));
  }

  if (_buffer == nullptr) {
    _mapping_length =
      ((ds->GetLength() / _huge_pages_size) + 1) * _huge_pages_size;
    _buffer =
      mmap(0, _mapping_length, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
    _huge_pages = 0;
  }

  if (_buffer == MAP_FAILED || _buffer == nullptr) {
    close(_fd);
    throw std::runtime_error("Could not map dataset file: " +
      ds->GetLocation() + " Error: " +
      std::string(strerror(errno)));
  }
  _open = true;
}

int32_t DefaultDatasetHandler::GetValueLength() { return _entry_length; }

void DefaultDatasetHandler::Remap() {
  munmap(_buffer, _mapping_length);
  _mapping_length =
    ((_ds->GetLength() / _huge_pages_size) + 1) * _huge_pages_size;
  _buffer =
    mmap(0, _mapping_length, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);

  if (_buffer == MAP_FAILED) {
    close(_fd);
    throw std::runtime_error("Could not map dataset file: " +
      _ds->GetLocation() + " Error: " +
      std::string(strerror(errno)));
  }
}

void DefaultDatasetHandler::Append(void *value) {
  if (_buffer_offset > _ds->GetLength()) {
    int written = write(_fd, value, _entry_length);

    if (written == -1) {
      throw std::runtime_error("Could not write to dataset file: " +
        _ds->GetLocation() + " Error: " +
        std::string(strerror(errno)));
    }

    _ds->Resize(_ds->GetLength() + _entry_length);
    _buffer_offset += _entry_length;

    if (_storageManager->RegisterDatasetExpasion(_entry_length) ==
      SAVIME_FAILURE) {
      throw std::runtime_error("Could not append to dataset file, "
                               "max storage size reached, consider "
                               "increasing max storage size.");
    }
  } else {
    memcpy(&(((char *)_buffer)[_buffer_offset]), (char *)value, _entry_length);
    _buffer_offset += _entry_length;
  }
}

void *DefaultDatasetHandler::Next() {
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

bool DefaultDatasetHandler::HasNext() {
  return _buffer_offset < _ds->GetLength();
}

void DefaultDatasetHandler::InsertAt(void *value, RealIndex offset) {
  offset = offset * _entry_length;
  memcpy(&((char *)_buffer)[offset], value, _entry_length);
}

void DefaultDatasetHandler::CursorAt(RealIndex index) {
  _buffer_offset = index * _entry_length;
}

void *DefaultDatasetHandler::GetBuffer() {
  if (_ds->GetLength() > _mapping_length)
    Remap();

  return _buffer;
}

void *DefaultDatasetHandler::GetBufferAt(RealIndex index) {
  int64_t buffer_offset = index * _entry_length;
  if (_ds->GetLength() > buffer_offset) {
    if (_ds->GetLength() > _mapping_length)
      Remap();

    return &((char *)_buffer)[index * _entry_length];
  } else {
    return nullptr;
  }
}

void DefaultDatasetHandler::TruncateAt(RealIndex index) {
  int64_t reduction;

  if (_ds->GetLength() > index) {
    if (ftruncate(_fd, index) == -1) {
      throw std::runtime_error("Could not truncate dataset file: " +
        _ds->GetLocation() + " Error: " +
        std::string(strerror(errno)));
    }

    reduction = _ds->GetLength() - index;
    _ds->Resize(index);
    _storageManager->RegisterDatasetTruncation(reduction);
    Remap();
  }
}

void DefaultDatasetHandler::Close() {
  munmap(_buffer, _mapping_length);
  close(_fd);
  _open = false;
}

DatasetPtr DefaultDatasetHandler::GetDataSet() { return _ds; }

DefaultDatasetHandler::~DefaultDatasetHandler() {
  if(_open) {
    munmap(_buffer, _mapping_length);
    close(_fd);
  }
}

//-----------------------------------------------------------------------------
// Storage Manager Members

std::string DefaultStorageManager::GenerateUniqueFileName() {
  int32_t numDirs = _configurationManager->GetIntValue(SUBDIRS_NUM);
  if (_useSecStorage) {
    std::string path = _configurationManager->GetStringValue(SEC_STORAGE_DIR);
    return generateUniqueFileName(path, numDirs);
  } else {
    std::string path = _configurationManager->GetStringValue(SHM_STORAGE_DIR);
    return generateUniqueFileName(path, numDirs);
  }
}

DatasetPtr DefaultStorageManager::Create(DataType type, savime_size_t entries) {
  try {

    if (entries <= 0) {
      throw runtime_error("Attempt to create a dataset with invalid size.");
    }

    int typeSize = TYPE_SIZE(type);
    auto location = GenerateUniqueFileName();
    auto size = entries * typeSize;

    // Checking size
    int64_t max = _configurationManager->GetLongValue(MAX_STORAGE_SIZE);
    if ((_usedStorageSize + size) > max)
      throw std::runtime_error("Could not create dataset: size would "
                               "exceed max storage size.");

    int fd = open(location.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
    if (fd == -1) {
      throw std::runtime_error("Could not open dataset file: " + location +
        " Error: " + std::string(strerror(errno)));
    }

    ftruncate(fd, size);
    //if((error = ftruncate(fd, ds->length))!= 0)
    //{
    // ignoring errors during truncation in hugetblfs
    // if(error != EINTR)
    //    throw std::runtime_error("Could not allocate dataset file:
    //    "+ds->location+" Error: "+std::string(strerror(error)));
    //}

    DatasetPtr ds = make_shared<Dataset>(UNSAVED_ID, "", location, type);
    ds->Sorted() = false;

    _mutex.lock();
    _usedStorageSize += size;
    _mutex.unlock();

    ds->AddListener(_this);
    close(fd);
    return ds;

  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return nullptr;
  }
}

DatasetPtr DefaultStorageManager::Create(DataType type, double init,
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

DatasetPtr DefaultStorageManager::Create(DataType type,
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

SavimeResult DefaultStorageManager::Save(DatasetPtr dataset) {
  int64_t max = _configurationManager->GetLongValue(MAX_STORAGE_SIZE);

  _mutex.lock();
  if ((_usedStorageSize + dataset->GetLength()) > max) {
    _mutex.unlock();
    return SAVIME_FAILURE;
  }

  _usedStorageSize += dataset->GetLength();
  _mutex.unlock();
  dataset->AddListener(_this);

  return SAVIME_SUCCESS;
}

DatasetHandlerPtr DefaultStorageManager::GetHandler(DatasetPtr dataset) {
  int64_t hugeTblThreshold =
    _configurationManager->GetLongValue(HUGE_TBL_THRESHOLD);

  int64_t hugeTblSize = _configurationManager->GetLongValue(HUGE_TBL_SIZE);

  DatasetHandlerPtr handler = DatasetHandlerPtr(
    new DefaultDatasetHandler(dataset, _this, hugeTblThreshold, hugeTblSize));
  return handler;
}

SavimeResult DefaultStorageManager::Drop(DatasetPtr dataset) {
  return SAVIME_SUCCESS;
}

void DefaultStorageManager::SetUseSecStorage(bool use) { _useSecStorage = use; }

SavimeResult
DefaultStorageManager::RegisterDatasetExpasion(savime_size_t size) {
  int64_t max = _configurationManager->GetLongValue(MAX_STORAGE_SIZE);

  _mutex.lock();
  if (_usedStorageSize + size > max) {
    _mutex.unlock();
    return SAVIME_FAILURE;
  }
  _usedStorageSize += size;
  _mutex.unlock();

  return SAVIME_SUCCESS;
}

SavimeResult
DefaultStorageManager::RegisterDatasetTruncation(savime_size_t size) {
  _mutex.lock();
  if (_usedStorageSize < size) {
    _mutex.unlock();
    return SAVIME_FAILURE;
  }
  _usedStorageSize -= size;
  _mutex.unlock();

  return SAVIME_SUCCESS;
}

bool DefaultStorageManager::CheckSorted(DatasetPtr dataset) {
  try {
#ifdef TIME
    GET_T1();
#endif

    if(dataset == nullptr)
      throw runtime_error(
          "Attempt to call CheckSorted with invalid dataset.");

    DataType type = dataset->GetType();
    if(type.isVector())
      throw runtime_error(
          "Attempt to call CheckSorted with vector typed dataset.");

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type, type, type);

     return st->CheckSorted(dataset);

#ifdef TIME
    PRINT_TIME_INFO("CheckSorted")
#endif
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    throw e;
  }
}

RealIndex DefaultStorageManager::Logical2Real(DimensionPtr dimension,
                                              Literal logicalIndex) {
  RealIndex realIndex = 0;
  try {

    if(dimension == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid dimension.");

    if(logicalIndex.type == NO_TYPE)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid logical index.");

    DataType type1 = dimension->GetType();
    DataType type2 = logicalIndex.type;
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    realIndex = st->Logical2Real(dimension, logicalIndex);
    return realIndex;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    throw e;
  }
}

IndexPair DefaultStorageManager::Logical2ApproxReal(DimensionPtr dimension,
                                                    Literal logicalIndex) {
  IndexPair indexPair;
  try {

    if(dimension == nullptr)
      throw runtime_error(
          "Attempt to call Logical2ApproxReal with invalid dimension.");

    if(logicalIndex.type == NO_TYPE)
      throw runtime_error(
          "Attempt to call Logical2ApproxReal with invalid logical index.");

    DataType type1 = dimension->GetType();
    DataType type2 = logicalIndex.type;

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    indexPair = st->Logical2ApproxReal(dimension, logicalIndex);
    return indexPair;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    throw e;
  }
}

SavimeResult DefaultStorageManager::Logical2Real(DimensionPtr dimension,
                                                 DimSpecPtr dimSpecs,
                                                 DatasetPtr logicalIndexes,
                                                 DatasetPtr &destinyDataset) {
  try {
    SavimeResult result;

#ifdef TIME
    GET_T1();
#endif

    if(dimension == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid dimension.");

    if(dimSpecs == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid dimension specification.");

    if(logicalIndexes == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid logical indexes dataset.");

    DataType type = dimension->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type, type, type);

    result =
      st->Logical2Real(dimension, dimSpecs, logicalIndexes, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Logical2Real")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::UnsafeLogical2Real(
  DimensionPtr dimension, DimSpecPtr dimSpecs, DatasetPtr logicalIndexes,
  DatasetPtr &destinyDataset) {
  try {

    if(dimension == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid dimension.");

    if(dimSpecs == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid dimension specification.");

    if(logicalIndexes == nullptr)
      throw runtime_error(
          "Attempt to call Logical2Real with invalid logical indexes dataset.");

    SavimeResult result;

#ifdef TIME
    GET_T1();
#endif

    DataType type1 = logicalIndexes->GetType();
    DataType type2 = dimension->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    result = st->UnsafeLogical2Real(dimension, dimSpecs, logicalIndexes,
                                    destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("UnsafeLogical2Real")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

Literal DefaultStorageManager::Real2Logical(DimensionPtr dimension,
                                            RealIndex realIndex) {
  Literal logicalIndex;
  logicalIndex.type = dimension->GetType();

  try {

    if(dimension == nullptr)
      throw runtime_error(
          "Attempt to call Real2Logical with invalid dimension.");

    DataType type = dimension->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type, type, type);

    logicalIndex = st->Real2Logical(dimension, realIndex);

    return logicalIndex;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    throw e;
  }
}

SavimeResult DefaultStorageManager::Real2Logical(DimensionPtr dimension,
                                                 DimSpecPtr dimSpecs,
                                                 DatasetPtr realIndexes,
                                                 DatasetPtr &destinyDataset) {
  try {
    SavimeResult result;

#ifdef TIME
    GET_T1();
#endif

    if(dimension == nullptr)
      throw runtime_error(
          "Attempt to call Real2Logical with invalid dimension.");

    if(dimSpecs == nullptr)
      throw runtime_error(
          "Attempt to call Real2Logical with invalid dimension specification.");

    if(realIndexes == nullptr || (realIndexes->GetType() != REAL_INDEX
        && realIndexes->GetType() != INT64))
      throw runtime_error(
          "Attempt to call Real2Logical with invalid logical indexes dataset.");

    DataType type = dimension->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type, type, type);

    result = st->Real2Logical(dimension, dimSpecs, realIndexes, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Real2logical")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult
DefaultStorageManager::IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2,
                                           DimensionPtr &destinyDim) {
  SavimeResult result;

  try {
#ifdef TIME
    GET_T1();
#endif

    if(dim1 == nullptr)
      throw runtime_error(
          "Attempt to call IntersectDimensions with invalid dimension.");

    if(dim2 == nullptr)
      throw runtime_error(
          "Attempt to call IntersectDimensions with invalid dimension.");

    DataType type1 = dim1->GetType();
    DataType type2 = dim2->GetType();

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    result = st->IntersectDimensions(dim1, dim2, destinyDim);

#ifdef TIME
    PRINT_TIME_INFO("Intersect")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Copy(DatasetPtr originDataset,
                                         RealIndex lowerBound,
                                         RealIndex upperBound,
                                         RealIndex offsetInDestiny,
                                         savime_size_t spacingInDestiny,
                                         DatasetPtr destinyDataset) {
  SavimeResult result;

  try {
#ifdef TIME
    GET_T1();
#endif

    if(originDataset == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid origin dataset.");

    if(destinyDataset == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid destiny dataset.");

    DataType type1 = originDataset->GetType();
    DataType type2 = destinyDataset->GetType();

    if((lowerBound > upperBound)
        || lowerBound >= originDataset->GetLength()
        || upperBound >= originDataset->GetLength() )
      throw runtime_error(
          "Attempt to call Copy with invalid lower and upper bounds.");

    savime_size_t sizeInDestiny = offsetInDestiny +
                                  (upperBound - lowerBound+1)*spacingInDestiny;

    if(sizeInDestiny >= destinyDataset->GetLength())
      throw runtime_error(
          "Attempt to call Copy with invalid offsetInDestiny "
          "and spacingInDestiny.");

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    result = st->Copy(originDataset, lowerBound, upperBound, offsetInDestiny,
                      spacingInDestiny, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Copy")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Copy(DatasetPtr originDataset,
                                         Mapping mapping,
                                         DatasetPtr destinyDataset,
                                         int64_t &copied) {
  SavimeResult result;

  try {
#ifdef TIME
    GET_T1();
#endif

    if(originDataset == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid origin dataset.");

    if(mapping == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid mapping.");

    if(destinyDataset == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid destiny dataset.");

    if(originDataset->GetEntryCount() != mapping->size())
      throw runtime_error(
          "Attempt to call Copy with inconformant origin dataset and mapping.");

    DataType type1 = originDataset->GetType();
    DataType type2 = destinyDataset->GetType();

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    result = st->Copy(originDataset, mapping, destinyDataset, copied);

#ifdef TIME
    PRINT_TIME_INFO("Copy")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Copy(DatasetPtr originDataset,
                                         DatasetPtr mapping,
                                         DatasetPtr destinyDataset,
                                         int64_t &copied) {
  SavimeResult result;

  try {
#ifdef TIME
    GET_T1();
#endif

    if(originDataset == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid origin dataset.");

    if(mapping == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid mapping dataset.");

    if(destinyDataset == nullptr)
      throw runtime_error(
          "Attempt to call Copy with invalid destiny dataset.");

    if(originDataset->GetEntryCount() != mapping->GetEntryCount())
      throw runtime_error(
          "Attempt to call Copy with inconformant origin dataset and mapping.");

    DataType type1 = originDataset->GetType();
    DataType type2 = destinyDataset->GetType();

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    result = st->Copy(originDataset, mapping, destinyDataset, copied);

#ifdef TIME
    PRINT_TIME_INFO("Copy")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Filter(DatasetPtr originDataset,
                                           DatasetPtr filterDataSet,
                                           DatasetPtr &destinyDataset) {
  SavimeResult result;

  try {
#ifdef TIME
    GET_T1();
#endif

    if(originDataset == nullptr)
      throw runtime_error(
          "Attempt to call Filter with invalid origin dataset.");

    if(filterDataSet == nullptr)
      throw runtime_error(
          "Attempt to call Filter with invalid filter dataset.");

    DataType type = originDataset->GetType();

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type, type, type);

    result = st->Filter(originDataset, filterDataSet, originDataset->GetType(),
                        destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Filter")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::And(DatasetPtr operand1,
                                        DatasetPtr operand2,
                                        DatasetPtr &destinyDataset) {
  try {
#ifdef TIME
    GET_T1();
#endif

    if(operand1 == nullptr || operand1->BitMask() == nullptr)
      throw runtime_error(
          "Attempt to call And with operand1 dataset.");

    if(operand2 == nullptr || operand2->BitMask() == nullptr)
      throw runtime_error(
          "Attempt to call And with operand2 dataset.");


    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t startPositionPerCore[numCores];
    int64_t finalPositionPerCore[numCores];

    SetWorkloadPerThread(operand1->BitMask()->size(), minWorkPerThread,
                         startPositionPerCore, finalPositionPerCore,
                         numCores);

    destinyDataset = make_shared<Dataset>(operand1->BitMask()->size());
    destinyDataset->AddListener(_this);
    destinyDataset->HasIndexes() = false;
    destinyDataset->Sorted() = false;

    boost::dynamic_bitset<>::and_parallel(
      *(destinyDataset->BitMask()), *(operand1->BitMask()),
      *(operand2->BitMask()), numCores, minWorkPerThread);


#ifdef TIME
    PRINT_TIME_INFO("And")
#endif

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Or(DatasetPtr operand1, DatasetPtr operand2,
                                       DatasetPtr &destinyDataset) {
  try {
#ifdef TIME
    GET_T1();
#endif

    if(operand1 == nullptr || operand1->BitMask() == nullptr)
      throw runtime_error(
          "Attempt to call Or with operand1 dataset.");

    if(operand2 == nullptr || operand2->BitMask() == nullptr)
      throw runtime_error(
          "Attempt to call Or with operand2 dataset.");

    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t startPositionPerCore[numCores];
    int64_t finalPositionPerCore[numCores];


    SetWorkloadPerThread(operand1->BitMask()->size(), minWorkPerThread,
                         startPositionPerCore, finalPositionPerCore,
                         numCores);

    destinyDataset = make_shared<Dataset>(operand1->BitMask()->size());
    destinyDataset->HasIndexes() = false;
    destinyDataset->Sorted() = false;
    destinyDataset->AddListener(_this);

    boost::dynamic_bitset<>::or_parallel(
      *(destinyDataset->BitMask()), *(operand1->BitMask()),
      *(operand2->BitMask()), numCores, minWorkPerThread);

#ifdef TIME
    PRINT_TIME_INFO("Or")
#endif

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Not(DatasetPtr operand1,
                                        DatasetPtr &destinyDataset) {
  try {
#ifdef TIME
    GET_T1();
#endif

    if(operand1 == nullptr || operand1->BitMask() == nullptr)
      throw runtime_error(
          "Attempt to call Not with operand1 dataset.");

    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
    int64_t startPositionPerCore[numCores];
    int64_t finalPositionPerCore[numCores];

    SetWorkloadPerThread(operand1->BitMask()->size(), minWorkPerThread,
                         startPositionPerCore, finalPositionPerCore,
                         numCores);

    destinyDataset = make_shared<Dataset>(operand1->BitMask()->size());
    destinyDataset->HasIndexes() = false;
    destinyDataset->Sorted() = false;
    destinyDataset->AddListener(_this);

    boost::dynamic_bitset<>::not_parallel(*(destinyDataset->BitMask()),
                                          *(operand1->BitMask()), numCores,
                                          minWorkPerThread);

#ifdef TIME
    PRINT_TIME_INFO("Not")
#endif

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::ComparisonStr(string op, DatasetPtr operand1,
                                               DatasetPtr operand2,
                                               DatasetPtr &destinyDataset) {

  int numCores = _configurationManager->GetIntValue(MAX_THREADS);
  int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
  int64_t entryCount = operand1->GetEntryCount() <= operand2->GetEntryCount()
                       ? operand1->GetEntryCount()
                       : operand2->GetEntryCount();

  DatasetHandlerPtr op1Handler = GetHandler(operand1);
  DatasetHandlerPtr op2Handler = GetHandler(operand2);

  auto op1Buffer =
      BUILD_VECTOR<char>(op1Handler->GetBuffer(), operand1->GetType());

  auto op2Buffer =
      BUILD_VECTOR<char>(op2Handler->GetBuffer(), operand2->GetType());


  destinyDataset = make_shared<Dataset>(entryCount);
  destinyDataset->AddListener(_this);
  destinyDataset->HasIndexes() = false;
  destinyDataset->Sorted() = false;

  SET_THREADS_ALIGNED(entryCount, minWorkPerThread, numCores,
                      (int32_t)destinyDataset->GetBitsPerBlock());

  string lhs, rhs;
  int32_t lhsSize = operand1->GetType().vectorLength();
  int32_t rhsSize = operand2->GetType().vectorLength();

  if (op == _EQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = lhs == rhs;
    }
  } else if (op == _NEQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = lhs != rhs;
    }
  } else if (op == _LE) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = lhs < rhs;
    }
  } else if (op == _GE) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = lhs > rhs;
    }
  } else if (op == _LEQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = lhs <= rhs;
    }
  } else if (op == _GEQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = lhs >= rhs;
    }
  } else if (op == _LIKE) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      BUF2STR(rhs, op2Buffer, i, rhsSize);
      (*destinyDataset->BitMask())[i] = like(lhs, rhs);
    }
  } else {
    throw std::runtime_error("Invalid comparison operation.");
  }

  op1Handler->Close();
  op2Handler->Close();

  return SAVIME_SUCCESS;
}

SavimeResult DefaultStorageManager::Comparison(string op, DatasetPtr operand1,
                                               DatasetPtr operand2,
                                               DatasetPtr &destinyDataset) {
  SavimeResult result;

  try {

    if(operand1 == nullptr)
      throw runtime_error(
          "Attempt to call Comparison with invalid operand1 dataset.");

    if(operand2 == nullptr)
      throw runtime_error(
          "Attempt to call Comparison with invalid operand2 dataset.");

#ifdef TIME
    GET_T1();
#endif

    DataType type1 = operand1->GetType();
    DataType type2 = operand2->GetType();

    if(type1.isNumeric() && type2.isNumeric()) {
      AbstractStorageManagerPtr st = TemplateBuilder::Build(
          _this, _configurationManager, _systemLogger, type1, type2, type1);

      result = st->Comparison(op, operand1, operand2, destinyDataset);
    } else {
      result = ComparisonStr(op, operand1, operand2, destinyDataset);
    }

#ifdef TIME
    PRINT_TIME_INFO("Comparison")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::ComparisonDim(string op,
                                                  DimSpecPtr dimSpecs,
                                                  savime_size_t totalLength,
                                                  DatasetPtr operand2,
                                                  DatasetPtr &destinyDataset) {
  DatasetPtr materializeDimDataset;

  if (MaterializeDim(dimSpecs, totalLength, materializeDimDataset) ==
    SAVIME_SUCCESS) {
#ifdef TIME
    GET_T1();
#endif

    SavimeResult result =
      Comparison(op, materializeDimDataset, operand2, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("ComparisonDim")
#endif

    return result;
  } else {
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::ComparisonStr(std::string op,
                                               DatasetPtr operand1,
                                               Literal operand2,
                                               DatasetPtr &destinyDataset) {
  int numCores = _configurationManager->GetIntValue(MAX_THREADS);
  int32_t minWorkPerThread =
      _configurationManager->GetIntValue(WORK_PER_THREAD);
  int64_t entryCount = operand1->GetEntryCount();
  DatasetHandlerPtr op1Handler = GetHandler(operand1);

  auto op1Buffer =
      BUILD_VECTOR<char>(op1Handler->GetBuffer(), operand1->GetType());

  destinyDataset = make_shared<Dataset>(entryCount);
  destinyDataset->AddListener(_this);
  destinyDataset->HasIndexes() = false;
  destinyDataset->Sorted() = false;

  SET_THREADS_ALIGNED(entryCount, minWorkPerThread, numCores,
                      (int32_t)destinyDataset->GetBitsPerBlock());

  string lhs, rhs = operand2.str;
  int32_t lhsSize = operand1->GetType().vectorLength();

  if (op == _EQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = lhs == rhs;
    }
  } else if (op == _NEQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = lhs != rhs;
    }
  } else if (op == _LE) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = lhs < rhs;
    }
  } else if (op == _GE) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = lhs > rhs;
    }
  } else if (op == _LEQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = lhs <= rhs;
    }
  } else if (op == _GEQ) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = lhs >= rhs;
    }
  } else if (op == _LIKE) {
#pragma omp parallel
    for (SubTARPosition i = THREAD_FIRST(); i < THREAD_LAST(); ++i) {
      BUF2STR(lhs, op1Buffer, i, lhsSize);
      (*destinyDataset->BitMask())[i] = like(lhs, rhs);
    }
  } else {
    throw std::runtime_error("Invalid comparison operation.");
  }

  op1Handler->Close();

  return SAVIME_SUCCESS;
}

SavimeResult DefaultStorageManager::Comparison(std::string op,
                                               DatasetPtr operand1,
                                               Literal operand2,
                                               DatasetPtr &destinyDataset) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    if (operand1 == nullptr)
      throw runtime_error(
          "Attempt to call Comparison with invalid operand1 dataset.");

    if (operand2.type == NO_TYPE)
      throw runtime_error(
          "Attempt to call Comparison with invalid operand2 literal type.");

    DataType type1 = operand1->GetType();
    DataType type2 = operand2.type;

    if (type1.isNumeric() && type2.isNumeric()) {
      AbstractStorageManagerPtr st = TemplateBuilder::Build(
        _this, _configurationManager, _systemLogger, type1, type2, type1);

      result = st->Comparison(op, operand1, operand2, destinyDataset);
    } else {
      result = ComparisonStr(op, operand1, operand2, destinyDataset);
    }

#ifdef TIME
    PRINT_TIME_INFO("Comparison")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::ComparisonDim(string op,
                                                  DimSpecPtr dimSpecs,
                                                  savime_size_t totalLength,
                                                  Literal operand2,
                                                  DatasetPtr &destinyDataset) {
  SavimeResult result;
  auto operand1 = dimSpecs->GetDimension();

  try {
#ifdef TIME
    GET_T1();
#endif

    if (dimSpecs == nullptr)
      throw runtime_error(
          "Attempt to call ComparisonDim with invalid dimension specification.");

    if (operand2.type == NO_TYPE)
      throw runtime_error(
          "Attempt to call ComparisonDim with invalid operand2 literal type.");

    DataType type1 = operand1->GetType();
    DataType type2 = operand2.type;

    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type2, type1);

    result =
      st->ComparisonDim(op, dimSpecs, operand2, totalLength, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("ComparisonDim")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::SubsetDims(vector<DimSpecPtr> dimSpecs,
                                               vector<RealIndex> lowerBounds,
                                               vector<RealIndex> upperBounds,
                                               DatasetPtr &destinyDataset) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    if (dimSpecs.empty())
      throw runtime_error(
          "Attempt to call SubsetDims with empty dimSpecs list.");

    if (lowerBounds.size() != upperBounds.size()
        || dimSpecs.size() != lowerBounds.size())
      throw runtime_error(
          "Attempt to call SubsetDims with invalid bounds.");

    DataType type(DOUBLE, 1);
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type, type, type);

    result = st->SubsetDims(dimSpecs, lowerBounds, upperBounds, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("SubsetDims")
#endif

  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
  return result;
}

SavimeResult DefaultStorageManager::Apply(string op, DatasetPtr operand1,
                                          DatasetPtr operand2,
                                          DatasetPtr &destinyDataset) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    if (operand1 == nullptr)
      throw runtime_error(
          "Attempt to call Apply with invalid operand1.");

    if (operand2 == nullptr)
      throw runtime_error(
          "Attempt to call Apply with invalid operand2.");


    DataType type1 = operand1->GetType();
    DataType type2 = operand2->GetType();
    DataType resultType = SelectType(type1, type2, op);

    AbstractStorageManagerPtr st = TemplateBuilder::BuildApply(
      _this, _configurationManager, _systemLogger, type1, type2, resultType);

    result = st->Apply(op, operand1, operand2, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Apply")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Apply(string op, DatasetPtr operand1,
                                          Literal operand2,
                                          DatasetPtr &destinyDataset) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    if (operand1 == nullptr)
      throw runtime_error(
          "Attempt to call Apply with invalid operand1.");

    if (operand2.type == NO_TYPE)
      throw runtime_error(
          "Attempt to call Apply with invalid operand2.");

    DataType type1 = operand1->GetType();
    DataType type2 = operand2.type;
    DataType resultType = SelectType(operand1->GetType(), operand2.type, op);

    AbstractStorageManagerPtr st = TemplateBuilder::BuildApply(
      _this, _configurationManager, _systemLogger, type1, type2, resultType);

    result = st->Apply(op, operand1, operand2, type2, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Apply")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::MaterializeDim(DimSpecPtr dimSpecs,
                                                   savime_size_t totalLength,
                                                   DatasetPtr &destinyDataset) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    if (dimSpecs == nullptr)
      throw runtime_error(
          "Attempt to call MaterializeDim with invalid dimSpecs.");

    DataType type1 = dimSpecs->GetDimension()->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type1, type1);

    result = st->MaterializeDim(dimSpecs, totalLength, type1, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("MaterializeDim")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::PartiatMaterializeDim(
  DatasetPtr filter, DimSpecPtr dimSpecs, savime_size_t totalLength,
  DatasetPtr &destinyDataset, DatasetPtr &destinyRealDataset) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    if (filter == nullptr)
      throw runtime_error(
          "Attempt to call PartiatMaterializeDim with invalid filter.");

    if (dimSpecs == nullptr)
      throw runtime_error(
          "Attempt to call PartiatMaterializeDim with invalid dimSpecs.");

    DataType type1 = dimSpecs->GetDimension()->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type1, type1);

    result = st->PartiatMaterializeDim(filter, dimSpecs, totalLength, type1,
                                       destinyDataset, destinyRealDataset);

#ifdef TIME
    PRINT_TIME_INFO("PartiatMaterializeDim")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Stretch(DatasetPtr origin,
                                            savime_size_t entryCount,
                                            savime_size_t recordsRepetitions,
                                            savime_size_t datasetRepetitions,
                                            DatasetPtr &destinyDataset) {

  SavimeResult result;
  try {

#ifdef TIME
    GET_T1();
#endif

    if (origin == nullptr)
      throw runtime_error(
          "Attempt to call Stretch with invalid DatasetPtr.");

    if (entryCount > origin->GetEntryCount())
      throw runtime_error(
          "Attempt to call Stretch with invalid entryCount.");


    DataType type1 = origin->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type1, type1);

    result = st->Stretch(origin, entryCount, recordsRepetitions,
                         datasetRepetitions, type1, destinyDataset);

#ifdef TIME
    PRINT_TIME_INFO("Stretch")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Match(DatasetPtr ds1, DatasetPtr ds2,
                                          DatasetPtr &ds1Mapping,
                                          DatasetPtr &ds2Mapping) {
  SavimeResult result;
  try {

#ifdef TIME
    GET_T1();
#endif

    if (ds1 == nullptr)
      throw runtime_error(
          "Attempt to call Match with invalid ds1.");

    if (ds2 == nullptr)
      throw runtime_error(
          "Attempt to call Match with invalid ds2.");

    /*Smaller dataset is the one on the right.*/
    if (ds1->GetEntryCount() > ds2->GetEntryCount()) {

      DataType type1 = ds1->GetType();
      DataType type2 = ds2->GetType();
      AbstractStorageManagerPtr st = TemplateBuilder::Build(
        _this, _configurationManager, _systemLogger, type1, type2, type1);

      result = st->Match(ds1, ds2, ds1Mapping, ds2Mapping);

    } else {
      DataType type1 = ds2->GetType();
      DataType type2 = ds1->GetType();
      AbstractStorageManagerPtr st = TemplateBuilder::Build(
        _this, _configurationManager, _systemLogger, type1, type2, type1);

      result = st->Match(ds2, ds1, ds2Mapping, ds1Mapping);
    }

#ifdef TIME
    PRINT_TIME_INFO("Match")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::MatchDim(DimSpecPtr dim1, int64_t totalLen1,
                                             DimSpecPtr dim2, int64_t totalLen2,
                                             DatasetPtr &ds1Mapping,
                                             DatasetPtr &ds2Mapping) {
  SavimeResult result;
  try {

#ifdef TIME
    GET_T1();
#endif

    if (dim1 == nullptr)
      throw runtime_error(
          "Attempt to call MatchDim with invalid dim1.");

    if (dim2 == nullptr)
      throw runtime_error(
          "Attempt to call MatchDim with invalid dim2.");

    /*Smaller dataset is the one on the right.*/
    // if (dim1->GetLength() > dim2->GetLength()) {
    if (1) {

      DataType type1 = dim1->GetDimension()->GetType();
      DataType type2 = dim2->GetDimension()->GetType();
      AbstractStorageManagerPtr st = TemplateBuilder::Build(
        _this, _configurationManager, _systemLogger, type1, type2, type1);

      result = st->MatchDim(dim1, totalLen1, dim2, totalLen2, ds1Mapping,
                            ds2Mapping);

    } else {
      DataType type1 = dim2->GetDimension()->GetType();
      DataType type2 = dim1->GetDimension()->GetType();
      AbstractStorageManagerPtr st = TemplateBuilder::Build(
        _this, _configurationManager, _systemLogger, type1, type2, type1);

      result = st->MatchDim(dim2, totalLen2, dim1, totalLen1, ds2Mapping,
                            ds1Mapping);
    }

#ifdef TIME
    PRINT_TIME_INFO("MatchDim")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultStorageManager::Split(DatasetPtr origin,
                                          savime_size_t totalLength,
                                          savime_size_t parts,
                                          vector<DatasetPtr> &brokenDatasets) {
  SavimeResult result;
  try {
#ifdef TIME
    GET_T1();
#endif

    DataType type1 = origin->GetType();
    AbstractStorageManagerPtr st = TemplateBuilder::Build(
      _this, _configurationManager, _systemLogger, type1, type1, type1);

    result = st->Split(origin, totalLength, parts, brokenDatasets);

#ifdef TIME
    PRINT_TIME_INFO("Split")
#endif

    return result;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

void DefaultStorageManager::FromBitMaskToPosition(DatasetPtr &dataset,
                                                  bool keepBitmask) {
#ifdef TIME
  GET_T1();
#endif

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
  dataset->Redefine(dataset->GetId(), dataset->GetName(),
                    auxDataSet->GetLocation(), auxDataSet->GetType());

  // To avoid file removal by destructor
  auxDataSet->ClearListeners();

  DatasetHandlerPtr handler = this->GetHandler(dataset);
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

  dataset->HasIndexes() = true;
  dataset->Sorted() = true;
  handler->Close();

  if (!keepBitmask)
    dataset->BitMask()->clear();

#ifdef TIME
  PRINT_TIME_INFO("FromBitMaskToIndex")
#endif
}

void DefaultStorageManager::DisposeObject(MetadataObject *object) {
  if (Dataset *dataset = dynamic_cast<Dataset *>(object)) {
    if (dataset->GetLocation().empty())
      return;

#ifdef TIME
    GET_T1();
#endif

    int64_t fileSize = FILE_SIZE(dataset->GetLocation().c_str());
    if (remove(dataset->GetLocation().c_str()) == 0) {

#ifdef TIME
      GET_T2();
      _systemLogger->LogEvent(_moduleName, "Remove dataset took " +
        std::to_string(GET_DURATION()) +
        " ms.");
#endif

#ifdef DEBUG
      _systemLogger->LogEvent(this->_moduleName,
                              "Removing dataset " + dataset->GetName() + "  " +
                                dataset->GetLocation() + " " +
                                std::to_string(dataset->GetLength()));
#endif

      RegisterDatasetTruncation(fileSize);
    } else {
      _systemLogger->LogEvent(this->_moduleName,
                              "Could not remove dataset " +
                                dataset->GetLocation() + ": " +
                                std::string(strerror(errno)));
    }
  }
}

string DefaultStorageManager::GetObjectInfo(MetadataObjectPtr object,
                                            string infoType) {
  return "";
}