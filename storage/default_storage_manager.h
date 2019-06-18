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

  int32_t GetValueLength() override;
  DatasetPtr GetDataSet() override;
  void Append(void *value) override;
  void *Next() override;
  bool HasNext() override;
  void InsertAt(void *value, RealIndex offset) override;
  void CursorAt(RealIndex index) override;
  void *GetBuffer() override;
  void *GetBufferAt(RealIndex offset) override;
  void TruncateAt(RealIndex offset) override;
  void Close() override;
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
  DatasetPtr Create(DataType type, savime_size_t size) override;
  DatasetPtr Create(DataType type, double init, double spacing, 
                     double end, int64_t rep) override;
  DatasetPtr Create(DataType type, vector<string> literals) override;
  SavimeResult Save(DatasetPtr dataset) override;
  DatasetHandlerPtr GetHandler(DatasetPtr dataset) override;
  SavimeResult Drop(DatasetPtr dataset) override;
  void SetUseSecStorage(bool use) override;

  /*Misc*/
  bool CheckSorted(DatasetPtr dataset) override;

  RealIndex Logical2Real(DimensionPtr dimension, Literal logicalIndex) override;

  IndexPair Logical2ApproxReal(DimensionPtr dimension, Literal logicalIndex) override;

  SavimeResult Logical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr logicalIndexes,
                            DatasetPtr &destinyDataset) override;

  SavimeResult UnsafeLogical2Real(DimensionPtr dimension, DimSpecPtr dimSpecs,
                                  DatasetPtr logicalIndexes,
                                  DatasetPtr &destinyDataset) override;

  Literal Real2Logical(DimensionPtr dimension, RealIndex realIndex) override;

  SavimeResult Real2Logical(DimensionPtr dimension, DimSpecPtr dimSpecs,
                            DatasetPtr realIndexes, DatasetPtr &destinyDataset) override;

  SavimeResult IntersectDimensions(DimensionPtr dim1, DimensionPtr dim2,
                                   DimensionPtr &destinyDim) override;

  /*Copy*/
  SavimeResult Copy(DatasetPtr originDataset, SubTARPosition lowerBound,
                    SubTARPosition upperBound, SubTARPosition offsetInDestiny,
                    savime_size_t spacingInDestiny, DatasetPtr destinyDataset) override;

  SavimeResult Copy(DatasetPtr originDataset, Mapping mapping,
                    DatasetPtr destinyDataset, int64_t &copied) override;

  SavimeResult Copy(DatasetPtr originDataset, DatasetPtr mapping,
                    DatasetPtr destinyDataset, int64_t &copied) override;

  /*Filter*/
  SavimeResult Filter(DatasetPtr originDataset, DatasetPtr filterDataSet,
                      DatasetPtr &destinyDataset) override;

  /*Logical*/
  SavimeResult And(DatasetPtr operand1, DatasetPtr operand2,
                   DatasetPtr &destinyDataset) override;

  SavimeResult Or(DatasetPtr operand1, DatasetPtr operand2,
                  DatasetPtr &destinyDataset) override;

  SavimeResult Not(DatasetPtr operand1, DatasetPtr &destinyDataset) override;

  /*Relational*/
  SavimeResult ComparisonStr(string op, DatasetPtr operand1, DatasetPtr operand2,
                          DatasetPtr &destinyDataset);

  SavimeResult Comparison(string op, DatasetPtr operand1, DatasetPtr operand2,
                          DatasetPtr &destinyDataset) override;

  SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs,
                             savime_size_t totalLength, DatasetPtr operand2,
                             DatasetPtr &destinyDataset) override;

  SavimeResult ComparisonStr(string op, DatasetPtr operand1, Literal operand2,
                          DatasetPtr &destinyDataset);

  SavimeResult Comparison(string op, DatasetPtr operand1, Literal operand2,
                          DatasetPtr &destinyDataset) override;

  SavimeResult ComparisonDim(string op, DimSpecPtr dimSpecs,
                             savime_size_t totalLength, Literal operand2,
                             DatasetPtr &destinyDataset) override;

  /*Subset*/
  SavimeResult SubsetDims(vector<DimSpecPtr> dimSpecs,
                          vector<RealIndex> lowerBounds,
                          vector<RealIndex> upperBounds,
                          DatasetPtr &destinyDataset) override;

  /*Apply*/
  SavimeResult Apply(string op, DatasetPtr operand1, DatasetPtr operand2,
                     DatasetPtr &destinyDataset) override;

  SavimeResult Apply(string op, DatasetPtr operand1, Literal operand2,
                     DatasetPtr &destinyDataset) override;

  /*Materialization*/
  SavimeResult MaterializeDim(DimSpecPtr dimSpecs, savime_size_t totalLength,
                              DatasetPtr &destinyDataset) override;

  SavimeResult PartiatMaterializeDim(DatasetPtr filter, DimSpecPtr dimSpecs,
                                     savime_size_t totalLength,
                                     DatasetPtr &destinyDataset,
                                     DatasetPtr &destinyRealDataset) override;

  /*Stretch*/
  SavimeResult Stretch(DatasetPtr origin, savime_size_t entryCount,
                       savime_size_t recordsRepetitions,
                       savime_size_t datasetRepetitions,
                       DatasetPtr &destinyDataset) override;

  /*Match*/
  SavimeResult Match(DatasetPtr ds1, DatasetPtr ds2, DatasetPtr &ds1Mapping,
                             DatasetPtr &ds2Mapping) override;

  /*MatchDim*/
  SavimeResult MatchDim(DimSpecPtr dim1, int64_t totalLen1, DimSpecPtr dim2,
                         int64_t totalLen2, DatasetPtr &ds1Mapping,
                         DatasetPtr &ds2Mapping) override;

  /*Split*/
  SavimeResult Split(DatasetPtr origin, savime_size_t totalLength,
                     savime_size_t parts, vector<DatasetPtr> &brokenDatasets) override;

  SavimeResult Reorient(DatasetPtr originDataset, vector<DimSpecPtr> dimSpecs, savime_size_t totalLength,
                        int32_t newMajor, int64_t partitionSize, DatasetPtr& destinyDataset) override;

    /*FromBitMaskToIndex*/
  void FromBitMaskToPosition(DatasetPtr &dataset, bool keepBitmask) override;

  /*Utilities*/
  SavimeResult RegisterDatasetExpasion(savime_size_t size) override;

  SavimeResult RegisterDatasetTruncation(savime_size_t size) override;

  void DisposeObject(MetadataObject *object) override;

  string GetObjectInfo(MetadataObjectPtr object, string infoType) override;
};


#endif /* DEFAULT_STORAGE_MANAGER_H */

