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
#ifndef DML_OPERATORS_H
#define DML_OPERATORS_H

#include <unordered_map>
#include "../core/include/engine.h"
#include "../core/include/util.h"
#include "../core/include/query_data_manager.h"
#include "../core/../core/include/storage_manager.h"

#define ERROR_MSG(F, O)                                                        \
  "Error during " + std::string(F) + " execution in " + std::string(O) +       \
      " operator. Check the log file for more info."

#define FIRST_SUBTAR 0

#define DECLARE_TARS(INPUT, OUTPUT)\
    ParameterPtr inputTarParam = operation->GetParametersByName(INPUT_TAR);\
    TARPtr INPUT = inputTarParam->tar;\
    assert(INPUT != nullptr);\
    TARPtr OUTPUT = operation->GetResultingTAR();\
    assert(OUTPUT != nullptr);

#define DECLARE_GENERATOR(GENERATOR, TAR_NAME)\
   auto GENERATOR = (std::dynamic_pointer_cast<DefaultEngine>(engine))\
                         ->GetGenerators()[TAR_NAME]

#define SET_GENERATOR(GENERATOR, TAR_NAME)\
        GENERATOR = (std::dynamic_pointer_cast<DefaultEngine>(engine))\
                         ->GetGenerators()[TAR_NAME]

#define GET_INT_CONFIG_VAL(VAR, KEY)\
   int32_t VAR = configurationManager->GetIntValue(KEY)

#define GET_BOOL_CONFIG_VAL(VAR, KEY)\
   int32_t VAR = configurationManager->GetBooleanValue(KEY)

#define SET_SUBTARS_THREADS(NUM)\
    omp_set_num_threads(NUM);\
    omp_set_nested(true);

#define SUB_THREADS_FIRST()\
    omp_get_thread_num()

#define SUB_THREADS_LAST()\
    omp_get_thread_num()

#define SET_ERROR(EXCEPTION, QUERY_DATA_MANAGER)\
   string error = QUERY_DATA_MANAGER->GetErrorResponse();\
    QUERY_DATA_MANAGER->SetErrorResponseText(EXCEPTION.what() + \
                                                string("\n") + error);

typedef std::mutex DMLMutex;
typedef std::unordered_map<int32_t, SubtarPtr> SubtarMap;
typedef std::vector<int32_t> SubtarIndexList;


/*----------------------------OPERATORS PROTOTYPES----------------------------*/
int scan(SubTARIndex subtarIndex, OperationPtr operation,
         ConfigurationManagerPtr configurationManager,
         QueryDataManagerPtr queryDataManager,
         MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
         EnginePtr engine);

int select(SubTARIndex subtarIndex, OperationPtr operation,
           ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

int filter(SubTARIndex subtarIndex, OperationPtr operation,
           ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

int subset(SubTARIndex subtarIndex, OperationPtr operation,
           ConfigurationManagerPtr configurationManager,
           QueryDataManagerPtr queryDataManager,
           MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
           EnginePtr engine);

int logical(SubTARIndex subtarIndex, OperationPtr operation,
            ConfigurationManagerPtr configurationManager,
            QueryDataManagerPtr queryDataManager,
            MetadataManagerPtr metadataManager,
            StorageManagerPtr storageManager, EnginePtr engine);

int comparison(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine);

int arithmetic(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine);

int cross_join(SubTARIndex subtarIndex, OperationPtr operation,
               ConfigurationManagerPtr configurationManager,
               QueryDataManagerPtr queryDataManager,
               MetadataManagerPtr metadataManager,
               StorageManagerPtr storageManager, EnginePtr engine);

int equijoin(SubTARIndex subtarIndex, OperationPtr operation,
             ConfigurationManagerPtr configurationManager,
             QueryDataManagerPtr queryDataManager,
             MetadataManagerPtr metadataManager,
             StorageManagerPtr storageManager, EnginePtr engine);

int dimjoin(SubTARIndex subtarIndex, OperationPtr operation,
            ConfigurationManagerPtr configurationManager,
            QueryDataManagerPtr queryDataManager,
            MetadataManagerPtr metadataManager,
            StorageManagerPtr storageManager, EnginePtr engine);

int slice(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine);

int aggregate(SubTARIndex subtarIndex, OperationPtr operation,
              ConfigurationManagerPtr configurationManager,
              QueryDataManagerPtr queryDataManager,
              MetadataManagerPtr metadataManager,
              StorageManagerPtr storageManager, EnginePtr engine);

int split(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine);

int user_defined(SubTARIndex subtarIndex, OperationPtr operation,
                 ConfigurationManagerPtr configurationManager,
                 QueryDataManagerPtr queryDataManager,
                 MetadataManagerPtr metadataManager,
                 StorageManagerPtr storageManager, EnginePtr engine);

#endif /* DML_OPERATORS_H */

