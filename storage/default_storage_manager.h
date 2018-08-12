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
#ifndef DEFAULT_STORAGE_MANAGER_H
#define DEFAULT_STORAGE_MANAGER_H

#include "../core/include/storage_manager.h"

#define END_OF_REGISTERS -1


class DefaultDatasetHandler : public DatasetHandler {
protected:
  bool _open;
  int32_t _fd;
  int32_t _entry_length;
  int64_t _buffer_offset;
  int64_t _mapping_length;
  int64_t _huge_pages;
  int64_t _huge_pages_size;
  StorageManagerPtr _storageManager;
  void *_buffer;

  void Remap();

public:
  DefaultDatasetHandler(DatasetPtr ds, StorageManagerPtr storageManager,
                        int64_t hugeTblThreshold, int64_t hugeTblSize);

  int32_t GetValueLength();
  DatasetPtr GetDataSet();
  void Append(void *value);
  void *Next();
  bool HasNext();
  void InsertAt(void *value, RealIndex offset);
  void CursorAt(RealIndex index);
  void *GetBuffer();
  void *GetBufferAt(RealIndex offset);
  void TruncateAt(RealIndex offset);
  void Close();
  ~DefaultDatasetHandler();
};

class DefaultStorageManager : public StorageManager,
                              public MetadataObjectListener {
  mutex _mutex;
  int64_t _usedStorageSize;
  bool _useSecStorage;
  shared_ptr<DefaultStorageManager> _this;
  string GenerateUniqueFileName();

public:
  DefaultStorageManager(ConfigurationManagerPtr configurationManager,
                        SystemLoggerPtr systemLogger)
      : StorageManager(configurationManager, systemLogger) {}

  void SetThisPtr(std::shared_ptr<DefaultStorageManager> thisPtr) {
    _usedStorageSize = 0;
    _useSecStorage = false;
    _this = thisPtr;
  }

  std::shared_ptr<DefaultStorageManager> GetOwn() { return _this; }

  /*Dataset management*/
  DatasetPtr Create(DataType type, savime_size_t size);
  DatasetPtr Create(DataType type, double init, double spacing, 
                     double end, int64_t rep);
  DatasetPtr Create(DataType type, vector<string> literals);
  SavimeResult Save(DatasetPtr dataset);
  DatasetHandlerPtr GetHandler(DatasetPtr dataset);
  SavimeResult Drop(DatasetPtr dataset);
  void SetUseSecStorage(bool use);

  /*Misc*/
  bool CheckSorted(DatasetPtr dataset);

  RealIndex Logical2Real(DimensionPtr dimension, Literal logicalIndex);

  IndexPair Logical2ApproxReal(DimensionPtr dimension, Literal logicalIndex);

  SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr logicalIndexes,
                            DatasetPtr &destinyDataset);

  SavimeResult UnsafeLogical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                                  DatasetPtr logicalIndexes,
                                  DatasetPtr &destinyDataset);

  Literal Real2Logical(DimensionPtr dimension, RealIndex realIndex);

  SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr realIndexes, DatasetPtr &destinyDataset);

  SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2,
                                   DimensionPtr &destinyDim);

  /*Copy*/
  SavimeResult Copy(DatasetPtr originDataset, RealIndex lowerBound,
                    RealIndex upperBound, RealIndex offsetInDestiny,
                    savime_size_t spacingInDestiny, DatasetPtr destinyDataset);

  SavimeResult Copy(DatasetPtr originDataset, Mapping mapping,
                    DatasetPtr destinyDataset, int64_t &copied);

  SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping,
                    DatasetPtr destinyDataset, int64_t &copied);

  /*Filter*/
  SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet,
                      DatasetPtr &destinyDataset);

  /*Logical*/
  SavimeResult And(DatasetPtr operand1, DatasetPtr operand2,
                   DatasetPtr &destinyDataset);

  SavimeResult Or(DatasetPtr operand1, DatasetPtr operand2,
                  DatasetPtr &destinyDataset);

  SavimeResult Not(DatasetPtr operand1, DatasetPtr &destinyDataset);

  /*Relational*/
  SavimeResult Comparison(string op, DatasetPtr operand1, DatasetPtr operand2,
                          DatasetPtr &destinyDataset);

  SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs,
                             savime_size_t totalLength, DatasetPtr operand2,
                             DatasetPtr &destinyDataset);

  SavimeResult Comparison(string op, DatasetPtr operand1, Literal operand2,
                          DatasetPtr &destinyDataset);

  SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs,
                             savime_size_t totalLength, Literal operand2,
                             DatasetPtr &destinyDataset);

  /*Subset*/
  SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs,
                          vector<RealIndex> lowerBounds,
                          vector<RealIndex> upperBounds,
                          DatasetPtr &destinyDataset);

  /*Apply*/
  SavimeResult Apply(string op, DatasetPtr operand1, DatasetPtr operand2,
                     DatasetPtr &destinyDataset);

  SavimeResult Apply(string op, DatasetPtr operand1, Literal operand2,
                     DatasetPtr &destinyDataset);

  /*Materialization*/
  SavimeResult MaterializeDim(DimSpecPtr dimSpecs, savime_size_t totalLength,
                              DatasetPtr &destinyDataset);

  SavimeResult PartiatMaterializeDim(DatasetPtr filter, DimSpecPtr dimSpecs,
                                     savime_size_t totalLength,
                                     DatasetPtr &destinyDataset,
                                     DatasetPtr &destinyRealDataset);

  /*Stretch*/
  SavimeResult Stretch(DatasetPtr origin, savime_size_t entryCount,
                       savime_size_t recordsRepetitions,
                       savime_size_t datasetRepetitions,
                       DatasetPtr &destinyDataset);

  /*Match*/
  SavimeResult Match(DatasetPtr ds1, DatasetPtr ds2, DatasetPtr &ds1Mapping,
                             DatasetPtr &ds2Mapping);

  /*MatchDim*/
  SavimeResult MatchDim(DimSpecPtr dim1, int64_t totalLen1, DimSpecPtr dim2,
                         int64_t totalLen2, DatasetPtr &ds1Mapping,
                         DatasetPtr &ds2Mapping);

  /*Split*/
  SavimeResult Split(DatasetPtr origin, savime_size_t totalLength,
                     savime_size_t parts, vector<DatasetPtr> &brokenDatasets);

  /*FromBitMaskToIndex*/
  void FromBitMaskToPosition(DatasetPtr &dataset, bool keepBitmask);

  /*Utilities*/
  SavimeResult RegisterDatasetExpasion(savime_size_t size);

  SavimeResult RegisterDatasetTruncation(savime_size_t size);

  void DisposeObject(MetadataObject *object);

  string GetObjectInfo(MetadataObjectPtr object, string infoType);
};


#endif /* DEFAULT_STORAGE_MANAGER_H */

