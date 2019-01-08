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
#ifndef DEFAULT_TEMPLATE_APPLY_H
#define DEFAULT_TEMPLATE_APPLY_H

#include "../core/include/abstract_storage_manager.h"
#include "../core/include/storage_manager.h"


template <class T1, class T2, class T3>
class TemplateApplyStorageManager : public AbstractStorageManager {
  StorageManagerPtr _storageManager;
  ConfigurationManagerPtr _configurationManager;
  SystemLoggerPtr _systemLogger;

public:
  TemplateApplyStorageManager(StorageManagerPtr storageManager,
                              ConfigurationManagerPtr configurationManager,
                              SystemLoggerPtr systemLogger) {
    _storageManager = storageManager;
    _configurationManager = configurationManager;
    _systemLogger = systemLogger;
  }

  RealIndex Logical2Real(DimensionPtr dimension, Literal logicalIndex) {
    return SAVIME_FAILURE;
  }

  IndexPair Logical2ApproxReal(DimensionPtr dimension, Literal _logicalIndex) {
    throw runtime_error("Unsupported Logical2ApproxReal operation.");
  }

  SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr logicalIndexes,
                            DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult UnsafeLogical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                                  DatasetPtr logicalIndexes,
                                  DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  Literal Real2Logical(DimensionPtr dimension, RealIndex realIndex) {
    return SAVIME_FAILURE;
  }

  SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr realIndexes,
                            DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2,
                                   DimensionPtr &destinyDim) {
    return SAVIME_FAILURE;
  }

  bool CheckSorted(DatasetPtr dataset) { return SAVIME_FAILURE; }

  SavimeResult Copy(DatasetPtr originDataset, SubTARPosition lowerBound,
                    SubTARPosition upperBound, SubTARPosition offsetInDestiny,
                    savime_size_t spacingInDestiny, DatasetPtr destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult Copy(DatasetPtr originDataset, Mapping mapping,
                    DatasetPtr destinyDataset, int64_t &copied) {
    return SAVIME_FAILURE;
  }

  SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping,
                    DatasetPtr destinyDataset, int64_t &copied) {
    return SAVIME_FAILURE;
  }

  SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet,
                      DataType type, DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult Comparison(string op, DatasetPtr operand1, DatasetPtr operand2,
                          DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult Comparison(string op, DatasetPtr operand1, Literal _operand2,
                          DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs,
                          vector<RealIndex> lowerBounds,
                          vector<RealIndex> upperBounds,
                          DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult ComparisonOrderedDim(string op, DimSpecPtr dimSpecs,
                                    Literal _operand2, int64_t totalLength,
                                    DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs, Literal _operand2,
                             int64_t totalLength, DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult Apply(string op, DatasetPtr operand1, DatasetPtr operand2,
                     DatasetPtr &destinyDataset) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
        _configurationManager->GetIntValue(WORK_PER_THREAD);
    
    int64_t entryCount = operand1->GetEntryCount() <= operand2->GetEntryCount()
                             ? operand1->GetEntryCount()
                             : operand2->GetEntryCount();

    SET_THREADS(entryCount, minWorkPerThread, numCores);

    DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
    DatasetHandlerPtr op2Handler = _storageManager->GetHandler(operand2);

    destinyDataset = _storageManager->Create(
        DataType(SelectType(operand1->GetType(), operand2->GetType(), op), 1),
        operand1->GetEntryCount());
    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset. ");

    DatasetHandlerPtr destinyHandler =
        _storageManager->GetHandler(destinyDataset);

    auto op1Buffer =
        BUILD_VECTOR<T1>(op1Handler->GetBuffer(), operand1->GetType());

    auto op2Buffer =
        BUILD_VECTOR<T2>(op2Handler->GetBuffer(), operand2->GetType());

    T3 *destinyBuffer = (T3 *)destinyHandler->GetBuffer();

    if (op.c_str()[0] == _ADDITION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] + (*op2Buffer)[i];
    } else if (op.c_str()[0] == _SUBTRACTION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] - (*op2Buffer)[i];
    } else if (op.c_str()[0] == _MULTIPLICATION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] * (*op2Buffer)[i];
    } else if (op.c_str()[0] == _DIVISION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] / (*op2Buffer)[i];
    } else if (op.c_str()[0] == _MODULUS[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = fmod((*op1Buffer)[i], (*op2Buffer)[i]);
    } else if (!op.compare(_POWER)) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = pow((*op1Buffer)[i], (*op2Buffer)[i]);
    } else {
      throw std::runtime_error("Invalid apply operation.");
    }

    op1Handler->Close();
    op2Handler->Close();
    destinyHandler->Close();
    //delete op1Buffer;
    //delete op2Buffer;
    return SAVIME_SUCCESS;
  }

  SavimeResult Apply(string op, DatasetPtr operand1, Literal _operand2,
                     DataType type, DatasetPtr &destinyDataset) {
    int numCores = _configurationManager->GetIntValue(MAX_THREADS);
    int32_t minWorkPerThread =
        _configurationManager->GetIntValue(WORK_PER_THREAD); 
    SET_THREADS(operand1->GetEntryCount(), minWorkPerThread, numCores);
  
    T2 operand2;
    GET_LITERAL(operand2, _operand2, T2);

    DatasetHandlerPtr op1Handler = _storageManager->GetHandler(operand1);
    destinyDataset = _storageManager->Create(
        DataType(SelectType(operand1->GetType(), type, op), 1),
        operand1->GetEntryCount());

    if (destinyDataset == nullptr)
      throw std::runtime_error("Could not create dataset.");

    DatasetHandlerPtr destinyHandler =
        _storageManager->GetHandler(destinyDataset);

    // T1* op1Buffer = (T1*) op1Handler->GetBuffer();
    auto op1Buffer =
        BUILD_VECTOR<T1>(op1Handler->GetBuffer(), operand1->GetType());

    T3 *destinyBuffer = (T3 *)destinyHandler->GetBuffer();

    if (op.c_str()[0] == _ADDITION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] + operand2;
    } else if (op.c_str()[0] == _SUBTRACTION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] - operand2;
    } else if (op.c_str()[0] == _MULTIPLICATION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] * operand2;
    } else if (op.c_str()[0] == _DIVISION[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = (*op1Buffer)[i] / operand2;
    } else if (op.c_str()[0] == _MODULUS[0]) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = fmod((*op1Buffer)[i], operand2);
    } else if (!op.compare("cos")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = cos((*op1Buffer)[i]);
    } else if (!op.compare("sin")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = sin((*op1Buffer)[i]);
    } else if (!op.compare("tan")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = tan((*op1Buffer)[i]);
    } else if (!op.compare("acos")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = acos((*op1Buffer)[i]);
    } else if (!op.compare("asin")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = asin((*op1Buffer)[i]);
    } else if (!op.compare("atan")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = atan((*op1Buffer)[i]);
    } else if (!op.compare("cosh")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = cosh((*op1Buffer)[i]);
    } else if (!op.compare("sinh")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = sinh((*op1Buffer)[i]);
    } else if (!op.compare("tanh")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = tanh((*op1Buffer)[i]);
    } else if (!op.compare("acosh")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = acosh((*op1Buffer)[i]);
    } else if (!op.compare("asinh")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = asinh((*op1Buffer)[i]);
    } else if (!op.compare("atanh")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = atanh((*op1Buffer)[i]);
    } else if (!op.compare("exp")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = exp((*op1Buffer)[i]);
    } else if (!op.compare("log")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = log((*op1Buffer)[i]);
    } else if (!op.compare("log10")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = log10((*op1Buffer)[i]);
    } else if (!op.compare("pow")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = pow((*op1Buffer)[i], operand2);
    } else if (!op.compare("sqrt")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = sqrt((*op1Buffer)[i]);
    } else if (!op.compare("ceil")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = ceil((*op1Buffer)[i]);
    } else if (!op.compare("floor")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = floor((*op1Buffer)[i]);
    } else if (!op.compare("round")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = round((*op1Buffer)[i]);
    } else if (!op.compare("abs")) {
#pragma omp parallel
      for (SubTARPosition i = THREAD_FIRST();i < THREAD_LAST(); ++i)
        destinyBuffer[i] = fabs((*op1Buffer)[i]);
    } else {
      throw std::runtime_error("Invalid apply operation.");
    }

    op1Handler->Close();
    destinyHandler->Close();
    return SAVIME_SUCCESS;
  }

  SavimeResult MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength,
                              DataType type, DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult PartiatMaterializeDim(DatasetPtr filter, DimSpecPtr dimSpecs,
                                     savime_size_t totalLength, DataType type,
                                     DatasetPtr &destinyLogicalDataset,
                                     DatasetPtr &destinyRealDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult Match(DatasetPtr ds1, DatasetPtr ds2, DatasetPtr &ds1Mapping,
                     DatasetPtr &ds2Mapping) {
    return SAVIME_FAILURE;
  }

  SavimeResult MatchDim(DimSpecPtr dim1, int64_t totalLen1,
                        DimSpecPtr dim2, int64_t totalLen2,
                        DatasetPtr &ds1Mapping,
                        DatasetPtr &ds2Mapping) {
    return SAVIME_FAILURE;
  }

  SavimeResult Stretch(DatasetPtr origin, int64_t entryCount,
                       int64_t recordsRepetitions, int64_t datasetRepetitions,
                       DataType type, DatasetPtr &destinyDataset) {
    return SAVIME_FAILURE;
  }

  SavimeResult Split(DatasetPtr origin, int64_t totalLength, int64_t parts,
                     vector<DatasetPtr> &brokenDatasets) {
    return SAVIME_FAILURE;
  }
};

#endif /* DEFAULT_TEMPLATE_H */

