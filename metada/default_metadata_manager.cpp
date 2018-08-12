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
#include "../core/include/util.h"
#include "../core/include/parser.h"
#include "default_metada_manager.h" 

DefaultMetadataManager::DefaultMetadataManager(
    ConfigurationManagerPtr configurationManager, SystemLoggerPtr systemLogger)
    : MetadataManager(configurationManager, systemLogger) {
  try {
    TARSPtr defaultTARS = TARSPtr(new TARS());
    defaultTARS->id = UNSAVED_ID;
    defaultTARS->name = "default";
    SaveTARS(defaultTARS);
    _configurationManager->SetIntValue(DEFAULT_TARS, defaultTARS->id);
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
  }
}

SavimeResult DefaultMetadataManager::SaveTARS(TARSPtr tars) {
  try {
    _mutex.lock();
    if (tars->id == UNSAVED_ID) {
      tars->id = _id++;
    }

    _tars[tars->id] = tars;
    _mutex.unlock();

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

TARSPtr DefaultMetadataManager::GetTARS(int32_t id) {
  TARSPtr tars = NULL;

  try {
    _mutex.lock();
    if (_tars.find(id) != _tars.end()) {
      tars = _tars[id];
    }
    _mutex.unlock();
    return tars;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return tars;
  }
}

SavimeResult DefaultMetadataManager::RemoveTARS(TARSPtr tars) {
  try {
    _mutex.lock();
    if (_tars.find(tars->id) != _tars.end()) {
      _tars.erase(tars->id);
    }

    _mutex.unlock();
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultMetadataManager::SaveTAR(TARSPtr tars, TARPtr tar) {
  try {
    _mutex.lock();
    if (tar->GetId() == UNSAVED_ID) {
      tar->AlterTAR(_id++, tar->GetName());
    }

    _tar[tar->GetId()] = tar;
    _tarName[tar->GetName()] = tar;

    tars->tars.push_back(tar);

    _mutex.unlock();
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

std::list<TARPtr> DefaultMetadataManager::GetTARs(TARSPtr tars) {
  return tars->tars;
}

TARPtr DefaultMetadataManager::GetTARByName(TARSPtr tars, std::string tarName) {
  TARPtr tar = NULL;

  _mutex.lock();
  if (_tarName.find(tarName) != _tarName.end()) {
    tar = _tarName[tarName];
  }
  _mutex.unlock();

  return tar;
}

SavimeResult DefaultMetadataManager::RemoveTar(TARSPtr tars, TARPtr tar) {
  try {
    _mutex.lock();
    if (_tar.find(tar->GetId()) != _tar.end()) {
      for (SubtarPtr s : tar->GetSubtars()) {
        _subtar.erase(s->GetId());
      }

      _tar.erase(tar->GetId());
      _tarName.erase(tar->GetName());
      tars->tars.remove(tar);
      tar->GetSubtars().clear();
      tar->AlterType(NULL);
      // delete tar;
    }
    _mutex.unlock();

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultMetadataManager::SaveSubtar(TARPtr tar, SubtarPtr subtar) {
  try {
    _mutex.lock();
    if (subtar->GetId() == UNSAVED_ID) {
      subtar->SetId(_id++);
    }

    _subtar[subtar->GetId()] = subtar;
    subtar->SetTAR(tar);
    tar->AddSubtar(subtar);

    _mutex.unlock();
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

std::list<SubtarPtr> DefaultMetadataManager::GetSubtars(std::string tarName) {
  std::list<SubtarPtr> list;

  _mutex.lock();
  if (_tarName.find(tarName) != _tarName.end()) {
    auto tar = _tarName[tarName];
    std::list<SubtarPtr> list;
    std::copy(tar->GetSubtars().begin(), tar->GetSubtars().end(),
              std::back_inserter(list));
  }
  _mutex.unlock();

  return list;
}

std::list<SubtarPtr> DefaultMetadataManager::GetSubtars(TARPtr tar) {
  std::list<SubtarPtr> list;
  _mutex.lock();
  std::copy(tar->GetSubtars().begin(), tar->GetSubtars().end(),
            std::back_inserter(list));
  _mutex.unlock();
  return list;
}

SavimeResult DefaultMetadataManager::RemoveSubtar(TARPtr tar,
                                                  SubtarPtr subtar) {
  try {
    _mutex.lock();
    if (_subtar.find(subtar->GetId()) != _subtar.end()) {
      _subtar.erase(subtar->GetId());
      auto subtars = tar->GetSubtars();
      subtars.erase(std::remove(subtars.begin(), subtars.end(), subtar),
                    subtars.end());
    }
    _mutex.unlock();

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultMetadataManager::SaveDataSet(TARSPtr tars,
                                                 DatasetPtr dataset) {
  try {
    _mutex.lock();
    if (dataset->GetId() == UNSAVED_ID) {
      dataset->GetId() = _id++;
    }

    _dataset[dataset->GetId()] = dataset;
    _datasetName[dataset->GetName()] = dataset;

    tars->id_datasets.push_back(dataset->GetId());
    _mutex.unlock();

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

DatasetPtr DefaultMetadataManager::GetDataSet(int32_t id) {
  try {
    _mutex.lock();
    if (_dataset.find(id) != _dataset.end()) {
      _mutex.unlock();
      return _dataset[id];
    }
    _mutex.unlock();
    return NULL;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return NULL;
  }
}

DatasetPtr DefaultMetadataManager::GetDataSetByName(std::string dsName) {
  try {
    _mutex.lock();
    if (_datasetName.find(dsName) != _datasetName.end()) {
      _mutex.unlock();
      return _datasetName[dsName];
    }
    _mutex.unlock();
    return NULL;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return NULL;
  }
}

SavimeResult DefaultMetadataManager::RemoveDataSet(TARSPtr tars,
                                                   DatasetPtr dataset) {
#define PRT_COUNT_NOREF 4

  try {
    _mutex.lock();
    if (_dataset.find(dataset->GetId()) != _dataset.end()) {
      if (dataset.use_count() != PRT_COUNT_NOREF) {
        _mutex.unlock();
        return SAVIME_FAILURE;
      }
      _dataset.erase(dataset->GetId());
      _datasetName.erase(dataset->GetName());
      tars->id_datasets.remove(dataset->GetId());

      // Removing file
      remove(dataset->GetLocation().c_str());
    }

    _mutex.unlock();
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

SavimeResult DefaultMetadataManager::SaveType(TARSPtr tars, TypePtr type) {
  try {
    _mutex.lock();
    if (type->id == UNSAVED_ID) {
      type->id = _id++;
    }

    _type[type->id] = type;
    tars->types.push_back(type);
    _mutex.unlock();

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

TypePtr DefaultMetadataManager::GetType(int32_t typeId) {
  try {
    _mutex.lock();
    if (_type.find(typeId) != _type.end()) {
      _mutex.unlock();
      return _type[typeId];
    }

    _mutex.unlock();
    return NULL;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return NULL;
  }
}

TypePtr DefaultMetadataManager::GetTypeByName(TARSPtr tars,
                                              std::string typeName) {
  TypePtr searchedType = NULL;

  _mutex.lock();
  for (auto type : tars->types) {
    if (!type->name.compare(typeName)) {
      // searchedType = new Type;
      searchedType = type;
    }
  }

  _mutex.unlock();
  return searchedType;
}

std::list<TypePtr> DefaultMetadataManager::GetTypes(TARSPtr tars) {
  return tars->types;
}

SavimeResult DefaultMetadataManager::RemoveType(TARSPtr tars, TypePtr type) {
#define PRT_COUNT_NOREF 4
  try {
    _mutex.lock();

    if (type.use_count() != PRT_COUNT_NOREF) {
      _mutex.unlock();
      return SAVIME_FAILURE;
    }

    if (_type.find(type->id) != _type.end()) {
      _type.erase(type->id);
      tars->types.remove(type);
      // delete type;
    }

    _mutex.unlock();
    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    _mutex.unlock();
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

void DefaultMetadataManager::RegisterQuery(string query) {
  _mutex.unlock();
  _backup.push_back(query);
  _mutex.unlock();
}

list<string> DefaultMetadataManager::GetQueries() {
  list<string> fullBackup;

#define CREATE_DATASET_DEF(d) d->GetName() + _COLON + d->GetType().toString()
#define CREATE_DATASET(d)                                                      \
  "create_dataset(\"" + CREATE_DATASET_DEF(d) + "\",\"" + d->GetLocation() + "\");"

  for (auto entry : _dataset) {
    DatasetPtr ds = entry.second;
    auto command = string(CREATE_DATASET(ds));
    fullBackup.push_back(command);
  }

  std::copy(_backup.begin(), _backup.end(), std::back_inserter(fullBackup));
  return fullBackup;
}

bool DefaultMetadataManager::ValidateIdentifier(std::string identifier,
                                                std::string objectType) {
  return validadeIdentifier(identifier);
}

void DefaultMetadataManager::DisposeObject(MetadataObject *object) {}

string DefaultMetadataManager::GetObjectInfo(MetadataObjectPtr object,
                                             string infoType) {}