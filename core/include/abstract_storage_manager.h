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

#ifndef ABSTRACT_TEMPLATE_H
#define ABSTRACT_TEMPLATE_H

#include <storage_manager.h>
#include <metadata.h>
#include <vector>
using namespace std;

class AbstractStorageManager {

public:
  virtual RealIndex
  Logical2Real(DimensionPtr dimension, Literal logicalIndex) = 0;

  virtual IndexPair Logical2ApproxReal(DimensionPtr dimension,
                                       Literal logicalIndex) = 0;

  virtual SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                                    DatasetPtr logicalIndexes,
                                    DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult UnsafeLogical2Real(DimensionPtr dimension,
                                          DimSpecPtr dimSpecs,
                                          DatasetPtr logicalIndexes,
                                          DatasetPtr &destinyDataset) = 0;

  virtual Literal Real2Logical(DimensionPtr dimension, RealIndex realIndex) = 0;

  virtual SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs,
                                    DatasetPtr realIndexes,
                                    DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2,
                                           DimensionPtr &destinyDim) = 0;

  virtual bool CheckSorted(DatasetPtr dataset) = 0;

  virtual SavimeResult Copy(DatasetPtr originDataset, SubTARPosition lowerBound,
                            SubTARPosition upperBound,
                            SubTARPosition offsetInDestiny,
                            savime_size_t spacingInDestiny,
                            DatasetPtr destinyDataset) = 0;

  virtual SavimeResult Copy(DatasetPtr originDataset, Mapping mapping,
                            DatasetPtr destinyDataset, int64_t &copied) = 0;

  virtual SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping,
                            DatasetPtr destinyDataset, int64_t &copied) = 0;

  virtual SavimeResult Filter(DatasetPtr originDataset,
                              DatasetPtr filterDataSet, DataType type,
                              DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult Comparison(std::string op, DatasetPtr operand1,
                                  DatasetPtr operand2,
                                  DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult Comparison(std::string op, DatasetPtr operand1,
                                  Literal _operand2,
                                  DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs,
                                  vector<RealIndex> lowerBounds,
                                  vector<RealIndex> upperBounds,
                                  DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult ComparisonOrderedDim(std::string op, DimSpecPtr dimSpecs,
                                            Literal operand2,
                                            int64_t totalLength,
                                            DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult ComparisonDim(std::string op, DimSpecPtr dimSpecs,
                                     Literal operand2, int64_t totalLength,
                                     DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult Apply(std::string op, DatasetPtr operand1,
                             DatasetPtr operand2,
                             DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult Apply(std::string op, DatasetPtr operand1,
                             Literal operand2, DataType type,
                             DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult MaterializeDim(DimSpecPtr dimSpecs, int64_t totalLength,
                                      DataType type,
                                      DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult
  PartiatMaterializeDim(DatasetPtr filter, DimSpecPtr dimSpecs,
                        savime_size_t totalLength, DataType type,
                        DatasetPtr &destinyLogicalDataset,
                        DatasetPtr &destinyRealDataset) = 0;

  virtual SavimeResult Stretch(DatasetPtr origin, int64_t entryCount,
                               int64_t recordsRepetitions,
                               int64_t datasetRepetitions, DataType type,
                               DatasetPtr &destinyDataset) = 0;

  virtual SavimeResult Match(DatasetPtr ds1, DatasetPtr ds2,
                             DatasetPtr &ds1Mapping,
                             DatasetPtr &ds2Mapping) = 0;

  virtual SavimeResult MatchDim(DimSpecPtr dim1, int64_t totalLen1,
                                DimSpecPtr dim2, int64_t totalLen2,
                                DatasetPtr &ds1Mapping,
                                DatasetPtr &ds2Mapping) = 0;

  virtual SavimeResult Split(DatasetPtr origin, int64_t totalLength,
                             int64_t parts,
                             vector<DatasetPtr> &brokenDatasets) = 0;
};
typedef std::shared_ptr<AbstractStorageManager>
  AbstractStorageManagerPtr;

template<class T> class SavimeBuffer {
protected:
  T
    *_base;
  DataType
    _type;

public:
  SavimeBuffer(T *base, DataType type) {
    _base =
      base;
    _type =
      type;
  }

  virtual inline T &operator[](RealIndex idx) { return _base[idx]; }

  virtual void copyTuple(RealIndex dst, void *src) {}
};

template<typename T> class VectorBuffer : public SavimeBuffer<T> {
  size_t
    _tupleLen =
    0;

public:
  VectorBuffer(T *base, DataType type) : SavimeBuffer<T>(base, type) {
    _tupleLen =
      sizeof(T) * this->_type.vectorLength();
  }

  inline T &operator[](RealIndex idx) {
    return this
      ->_base[idx * this->_type.vectorLength() + this->_type.selected()];
  }

  void copyTuple(RealIndex dst, void *src) {
    memcpy(&this->_base[dst * this->_type.vectorLength()], (char *) src,
           _tupleLen);
  }
};

template<class T>
inline shared_ptr<SavimeBuffer<T>> BUILD_VECTOR(void *buffer, DataType type) {
  shared_ptr<SavimeBuffer<T>>
    ptr;

  if (type.isVector()) {
    ptr = std::make_shared<VectorBuffer<T>>((T *) buffer, type);
  } else {
    ptr = std::make_shared<SavimeBuffer<T>>((T *) buffer, type);
  }

  return ptr;
}

template<class T>
using SavimeBufferPtr = std::shared_ptr<SavimeBuffer<T>>;

#endif /* ABSTRACT_TEMPLATE_H */

