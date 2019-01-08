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
#ifndef DEFAULT_CATALOG_MANAGER_H
#define DEFAULT_CATALOG_MANAGER_H

#include <algorithm>
#include <unordered_map>
#include <mutex>
#include "../core/include/metadata.h"

using namespace std;

class DefaultMetadataManager : public MetadataManager,
                               public MetadataObjectListener {
  int32_t _id = 1;
  unordered_map<int32_t, TARSPtr> _tars;
  unordered_map<string, TARPtr> _tarName;
  unordered_map<int32_t, TARPtr> _tar;
  unordered_map<int32_t, SubtarPtr> _subtar;
  unordered_map<int32_t, TypePtr> _type;
  unordered_map<int32_t, DatasetPtr> _dataset;
  unordered_map<string, DatasetPtr> _datasetName;
  list<string> _backup;
  recursive_mutex _mutex;

public:
  DefaultMetadataManager(ConfigurationManagerPtr configurationManager,
                         SystemLoggerPtr systemLogger);
  SavimeResult SaveTARS(TARSPtr tars) override;
  TARSPtr GetTARS(int32_t id) override;
  SavimeResult RemoveTARS(TARSPtr tars) override;
  SavimeResult SaveTAR(TARSPtr tars, TARPtr tar) override;
  TARPtr GetTARByName(TARSPtr tars, std::string tarName) override;
  list<TARPtr> GetTARs(TARSPtr tars) override;
  SavimeResult RemoveTar(TARSPtr tars, TARPtr tar) override;
  SavimeResult SaveSubtar(TARPtr tar, SubtarPtr subtar) override;
  list<SubtarPtr> GetSubtars(std::string tarName) override;
  list<SubtarPtr> GetSubtars(TARPtr tar) override;
  SavimeResult RemoveSubtar(TARPtr tar, SubtarPtr subtar) override;
  SavimeResult SaveType(TARSPtr tars, TypePtr type) override;
  TypePtr GetType(int32_t typeId);
  list<TypePtr> GetTypes(TARSPtr tars) override;
  TypePtr GetTypeByName(TARSPtr tars, string typeName) override;
  SavimeResult RemoveType(TARSPtr tars, TypePtr type) override;
  SavimeResult SaveDataSet(TARSPtr tars, DatasetPtr dataset) override;
  DatasetPtr GetDataSet(int32_t id);
  DatasetPtr GetDataSetByName(string dsName) override;
  SavimeResult RemoveDataSet(TARSPtr tars, DatasetPtr dataset) override;
  bool ValidateIdentifier(string identifier, string objectType) override;
  void RegisterQuery(string query) override;
  list<string> GetQueries() override;
  void DisposeObject(MetadataObject *object) override;
  string GetObjectInfo(MetadataObjectPtr object, string infoType) override;
};

#endif /* DEFAULT_CATALOG_MANAGER_H */
