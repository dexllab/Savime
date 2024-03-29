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
*    ANDERSON C. SILVA				JANUARY 2020
*/

#include <configuration/default_config_manager.h>
#include <configuration/model_configuration_manager.h>
#include <jsoncpp/json/value.h>
#include "include/ml_operators.h"
#include <fstream>
#include <jsoncpp/json/json.h>

RegisterModel::RegisterModel(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult RegisterModel::Run() {
  string modelName = _operation->GetParametersByName("model_name")->literal_str;
  string attributeList = _operation->GetParametersByName("attribute_string")->literal_str;
  string inputDimensionString = _operation->GetParametersByName("input_dimension_string")->literal_str;
  string outputDimensionString = _operation->GetParametersByName("output_dimension_string")->literal_str;

  attributeList = remove_str(attributeList, ' ');
  inputDimensionString  = remove_str(inputDimensionString, ' ');
  outputDimensionString = remove_str(outputDimensionString, ' ');

  ModelConfigurationManager *modelConfigurationManager = new ModelConfigurationManager();
  modelConfigurationManager->SetStringValue("model_name", "\"" + modelName + "\"");
  modelConfigurationManager->SetStringValue("attribute_list",  attributeList  );
  modelConfigurationManager->SetStringValue("input_dimension_specifications", inputDimensionString);
  modelConfigurationManager->SetStringValue("output_dimension_specifications", outputDimensionString);

  string modelConfigDirectory =  "/tmp";
  auto numberOfDimensions = split(inputDimensionString, '|').size();
  modelConfigurationManager->SetLongValue("number_of_dimensions", numberOfDimensions);
  modelConfigurationManager->SaveConfigFile(modelConfigDirectory + "/" + modelName);
  auto list = _operation->GetParameters();

  auto model_path = modelConfigDirectory + "/models.config";
  auto modelServerConfiguration = this->ParseModelsFile(model_path);

  delete(modelConfigurationManager);
  return SAVIME_SUCCESS;
}

unordered_map<string, unordered_map<string, string>> RegisterModel::ParseModelsFile(string filePath){
  ifstream file(filePath);
  unordered_map<string, unordered_map<string, string>> modelServerConfiguration;
  unordered_map<string, string> modelConfig ;

  string line;
  ifstream myfile(filePath);
  if (myfile.is_open())
  {
    while ( getline (myfile,line) )
    {
      cout << line << '\n';
    }
    myfile.close();
  }

  string s;
  getline(file, s, '{');
  getline(file, s, '{');
  while(trim(s).compare("config") == 0) {
    for (int i = 0; i < 3; ++i) {
      string key, value;
      getline(file, key, ':');
      if(trim(key).compare("model_platform") == 0){
        getline(file, value, '}');
        modelConfig[trim(key)] = trim(value);
        getline(file, value, ',');
      }else{
        getline(file, value, ',');
        modelConfig[trim(key)] = trim(value);
      }

    }
    modelServerConfiguration[modelConfig["name"]] = modelConfig;
    if(!getline(file, s, '{')){
        break;
    };
  }
  return modelServerConfiguration;
}
