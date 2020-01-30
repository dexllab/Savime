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
#ifndef DDL_OPERATORS_H
#define DDL_OPERATORS_H

#include "../core/include/engine.h"
#include "../core/include/util.h"
#include "../core/include/query_data_manager.h"
#include "../core/include/storage_manager.h"
#include "../core/include/symbols.h"
#include "dml_operators.h"
#include "create_tar.h"
#include "load_subtar.h"
#include <algorithm>
#include <vector>
#include <regex>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


class CreateTARS : public EngineOperator {
public:
    CreateTARS(OperationPtr operation, ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class CreateTAR : public EngineOperator {
public:
    CreateTAR(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
               EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class CreateType : public EngineOperator {
public:
    CreateType(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
               EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class CreateDataset : public EngineOperator {
public:
    CreateDataset(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
               EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class LoadSubtar : public EngineOperator {
public:
    LoadSubtar(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                  QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
                  EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class DropTARS : public EngineOperator {
public:
    DropTARS(OperationPtr operation, ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
               EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class DropTAR : public EngineOperator {
public:
    DropTAR(OperationPtr operation, ConfigurationManagerPtr configurationManager,
             QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
             EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class DropType : public EngineOperator {
public:
    DropType(OperationPtr operation, ConfigurationManagerPtr configurationManager,
             QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
             EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class DropDataset : public EngineOperator {
public:
    DropDataset(OperationPtr operation, ConfigurationManagerPtr configurationManager,
      QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
    EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class Save : public EngineOperator {
public:
    Save(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
                EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class Show : public EngineOperator {
public:
    Show(OperationPtr operation, ConfigurationManagerPtr configurationManager,
         QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
         EnginePtr engine);

    SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
    SavimeResult Run() override;
};

class RegisterModel : public EngineOperator {
 public:
  RegisterModel(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
                EnginePtr engine);

  SavimeResult GenerateSubtar(SubTARIndex subtarIndex) override { return SAVIME_FAILURE; }
  SavimeResult Run() override;
 private:
  unordered_map<string, unordered_map<string, string>> ParseModelsFile(string filePath);
};

#endif /* DDL_OPERATORS_H */

