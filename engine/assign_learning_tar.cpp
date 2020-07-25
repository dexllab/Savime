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
*    ANDERSON C. SILVA				JUNE 2020
*/

#include <configuration/default_config_manager.h>
#include <configuration/model_configuration_manager.h>
#include "include/ml_operators.h"
#include <fstream>

AssignLearningTAR::AssignLearningTAR(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                             QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                             StorageManagerPtr storageManager, EnginePtr engine) :
        EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult AssignLearningTAR::Run() {
    string learningQuery = _operation->GetParametersByName("learning_query")->literal_str;
    string modelName = _operation->GetParametersByName("model_name")->literal_str;

    string modelConfigDirectory =  "/tmp/";
    auto filename = modelConfigDirectory + modelName;
    ModelConfigurationManager *modelConfigurationManager = new ModelConfigurationManager();
    modelConfigurationManager->LoadConfigFile(filename);
    modelConfigurationManager->SetStringValue("learning_query", learningQuery);
    modelConfigurationManager->SaveConfigFile(filename);
    delete(modelConfigurationManager);
    return SAVIME_SUCCESS;
}