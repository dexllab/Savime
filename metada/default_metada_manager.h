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
  SavimeResult SaveTARS(TARSPtr tars);
  TARSPtr GetTARS(int32_t id);
  SavimeResult RemoveTARS(TARSPtr tars);
  SavimeResult SaveTAR(TARSPtr tars, TARPtr tar);
  TARPtr GetTARByName(TARSPtr tars, std::string tarName);
  list<TARPtr> GetTARs(TARSPtr tars);
  SavimeResult RemoveTar(TARSPtr tars, TARPtr tar);
  SavimeResult SaveSubtar(TARPtr tar, SubtarPtr subtar);
  list<SubtarPtr> GetSubtars(std::string tarName);
  list<SubtarPtr> GetSubtars(TARPtr tar);
  SavimeResult RemoveSubtar(TARPtr tar, SubtarPtr subtar);
  SavimeResult SaveType(TARSPtr tars, TypePtr type);
  TypePtr GetType(int32_t typeId);
  list<TypePtr> GetTypes(TARSPtr tars);
  TypePtr GetTypeByName(TARSPtr tars, string typeName);
  SavimeResult RemoveType(TARSPtr tars, TypePtr type);
  SavimeResult SaveDataSet(TARSPtr tars, DatasetPtr dataset);
  DatasetPtr GetDataSet(int32_t id);
  DatasetPtr GetDataSetByName(string dsName);
  SavimeResult RemoveDataSet(TARSPtr tars, DatasetPtr dataset);
  bool ValidateIdentifier(string identifier, string objectType);
  void RegisterQuery(string query);
  list<string> GetQueries();
  void DisposeObject(MetadataObject *object);
  string GetObjectInfo(MetadataObjectPtr object, string infoType);
};

#endif /* DEFAULT_CATALOG_MANAGER_H */
