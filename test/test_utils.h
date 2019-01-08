#include <utility>

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
#ifndef SAVIME_TEST_UTILS_H
#define SAVIME_TEST_UTILS_H

#include <sstream>
#include <iomanip>
#include "catch2/catch.hpp"
#include "core/include/metadata.h"
#include "core/include/storage_manager.h"

#define COMP_TOL  0.00000001

#define CHECK_BIT(BITMASK, POS) ((BITMASK) & (1<<(POS)))
#define B(POS) (1<<POS)


void InitBitmask(DatasetPtr dataset, vector<bool> bits);

template <class T1, class T2> class TemplateDatasetComparator {
  StorageManagerPtr _storageManager;

public:
  TemplateDatasetComparator(StorageManagerPtr storageManager) {
    _storageManager = storageManager;
  }

  void Log(T1 * buf1, T2 * buf2, savime_size_t size) {
    std::stringstream ss;
    ss << string("Different datasets:\n");

    for (SubTARPosition i = 0; i < size; i++) {
      ss << std::setprecision(12) << i << "| \t" << buf1[i]  << "|\t"
                                 << buf2[i] << "\n";
    }

    WARN("" << ss.str());
  }

  bool Compare(DatasetPtr ds1, DatasetPtr ds2) {

    bool equal = true;
    if (ds1 == nullptr || ds2 == nullptr)
      throw runtime_error(
          "Attempt to call Compare in DatasetComparator with invalid datasets");

    if (ds1->GetEntryCount() != ds2->GetEntryCount())
      return false;

    savime_size_t size = ds1->GetEntryCount();

    DatasetHandlerPtr handler1 = _storageManager->GetHandler(ds1);
    DatasetHandlerPtr handler2 = _storageManager->GetHandler(ds2);
    T1 *buf1 = (T1 *)handler1->GetBuffer();
    T2 *buf2 = (T2 *)handler2->GetBuffer();

    for (SubTARPosition i = 0; i < size; i++) {
      if (abs(buf1[i] - buf2[i]) > COMP_TOL) {
        equal = false;
        break;
      }
    }

    handler1->Close();
    handler2->Close();

    if(!equal) {
      Log(buf1, buf2, size);
    }

    return equal;
  }
};


class DatasetComparator {
  StorageManagerPtr _storageManager;
public:

  explicit DatasetComparator(StorageManagerPtr storageManager) {
    _storageManager = std::move(storageManager);
  }

  bool Compare(DatasetPtr ds1, DatasetPtr ds2);
  bool CompareBitmask(DatasetPtr ds1, DatasetPtr ds2);
  void LogBitmask(DatasetPtr ds1, DatasetPtr ds2);
};


class MetadataComparator {
  StorageManagerPtr _storageManager;
  DatasetComparator _datasetComparator;
public:

  MetadataComparator(StorageManagerPtr storageManager)
    : _datasetComparator(storageManager) {
    _storageManager = storageManager;
  }

  bool Compare(DimensionPtr dim1, DimensionPtr dim2, int64_t mask);
  bool Compare(AttributePtr att1, AttributePtr att2, int64_t mask);
  bool Compare(DimSpecPtr dimSpec1, DimSpecPtr dimSpec2, int64_t mask);
  bool Compare(SubtarPtr subtar1, SubtarPtr subtar2, int64_t mask);
  bool Compare(TypePtr type1, TypePtr type2, int64_t mask);
  bool Compare(TARPtr tar1, TARPtr tar2, int64_t mask);
  bool Compare(TARSPtr tars1, TARSPtr tars2, int64_t mask);
};


class QueryStrucuturesComparator {
  StorageManagerPtr _storageManager;
  DatasetComparator _datasetComparator;
  MetadataComparator _metadataComparator;
public:

  QueryStrucuturesComparator(StorageManagerPtr storageManager,
                             DatasetComparator datasetComparator,
                             MetadataComparator metadataComparator)
      : _datasetComparator(storageManager), _metadataComparator(storageManager){
  _storageManager = storageManager;
  }

  bool Compare(ParameterPtr p1, ParameterPtr p2);
  bool Compare(OperationPtr op1, OperationPtr op2);
  bool Compare(QueryPlanPtr plan1, QueryPlanPtr plan2, int64_t mask);
};


#endif //SAVIME_TEST_UTILS_H
