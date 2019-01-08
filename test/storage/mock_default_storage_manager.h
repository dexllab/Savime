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
#ifndef SAVIME_MOCK_DEFAULT_STORAGE_MANAGER_H
#define SAVIME_MOCK_DEFAULT_STORAGE_MANAGER_H

#define MOCK_FILE_BASE_LOCATION "/tmp/savime-mocXXXXXX"

#include <unordered_map>
#include "../../core/include/metadata.h"
#include "../../storage/default_storage_manager.h"

class MockDefaultDatasetHandler : public DatasetHandler {
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

  static std::unordered_map<string, void*> _buffers;
  MockDefaultDatasetHandler(DatasetPtr ds, StorageManagerPtr storageManager,
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
  ~MockDefaultDatasetHandler();
};


class MockDefaultStorageManager : public DefaultStorageManager {
  string _mockFileLocation;
public:
  MockDefaultStorageManager(ConfigurationManagerPtr configurationManager,
  SystemLoggerPtr systemLogger)
  : DefaultStorageManager(configurationManager, systemLogger) {}

  DatasetPtr Create(DataType type, savime_size_t entries);
  DatasetPtr Create(DataType type, double init, double spacing,
                    double end, int64_t rep);
  DatasetPtr Create(DataType type, vector<string> literals);
  DatasetHandlerPtr GetHandler(DatasetPtr dataset);
  void FromBitMaskToPosition(DatasetPtr &dataset, bool keepBitmask);
  void DisposeObject(MetadataObject *object);
};



#endif //SAVIME_MOCK_DEFAULT_STORAGE_MANAGER_H
