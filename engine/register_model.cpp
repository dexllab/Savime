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
#include "include/ddl_operators.h"
#include <fstream>
#include <jsoncpp/json/json.h>

RegisterModel::RegisterModel(OperationPtr operation, ConfigurationManagerPtr configurationManager,
                       QueryDataManagerPtr queryDataManager, MetadataManagerPtr metadataManager,
                       StorageManagerPtr storageManager, EnginePtr engine) :
  EngineOperator(operation, configurationManager, queryDataManager, metadataManager, storageManager, engine){}

SavimeResult RegisterModel::Run() {
  string modelName = _operation->GetParametersByName("model_name")->literal_str;
  string tarName = _operation->GetParametersByName("tar_name")->literal_str;
  string targetAttribute = _operation->GetParametersByName("target_attribute")->literal_str;
  string dimensionString = _operation->GetParametersByName("dimension_string")->literal_str;
  string modelPath = _operation->GetParametersByName("model_path")->literal_str;

  dimensionString.erase(std::remove_if(dimensionString.begin(), dimensionString.end(), ::isspace), dimensionString.end());
  ModelConfigurationManager *modelConfigurationManager = new ModelConfigurationManager();
  modelConfigurationManager->SetStringValue("model_name", "\"" + modelName + "\"");
  modelConfigurationManager->SetStringValue("tar_name", "\"" + tarName + "\"");
  modelConfigurationManager->SetStringValue("target_attribute", "\"" + targetAttribute + "\"");
  modelConfigurationManager->SetStringValue("dimension_specifications", dimensionString);
  modelConfigurationManager->SetStringValue("model_path", modelPath);

  auto configurationManager = new DefaultConfigurationManager();
  configurationManager->LoadConfigFile("/home/anderson/Programacao/Savime/Savime/etc/savime.config");
  string modelConfigDirectory = configurationManager->GetStringValue("mdl_cfg_dir") + "/models.config";
  delete(configurationManager);

  modelConfigurationManager->LoadConfigFile(modelConfigDirectory);

  auto numberOfDimensions = split(dimensionString, ',').size();
  modelConfigurationManager->SetLongValue("number_of_dimensions", numberOfDimensions);

  modelConfigurationManager->SaveConfigFile(modelConfigDirectory + "/" + modelName);
  auto list = _operation->GetParameters();

  auto model_path = modelConfigDirectory + "/models.config";
  auto modelServerConfiguration = this->ParseModelsFile(model_path);

  modelServerConfiguration.erase(modelName);
  unordered_map<string, string> modelConfig ;
  modelConfig["name"] = "\"" + modelName + "\"";
  modelConfig["base_path"] = modelPath;
  modelConfig["model_platform"] = "\"tensorflow\"";

  modelServerConfiguration[modelName] = modelConfig;
  this->registerModelConfigurationInFile(modelServerConfiguration, model_path);
  delete(configurationManager);
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
void RegisterModel::registerModelConfigurationInFile(unordered_map<string, unordered_map<string, string>>
                                                     modelServerConfiguration, string filePath) {
  ofstream file(filePath);
  file << "model_config_list {";
  auto ct = modelServerConfiguration.size();
  for (auto entry : modelServerConfiguration) {
      file << "config {";
      file << "name: " + entry.second["name"] + ", ";
      file << "base_path: " + entry.second["base_path"] + ", ";
      file << "model_platform: " + entry.second["model_platform"];
      file << "}";
      ct--;
      if(ct != 0){
          file << ",";
      }
  }
  file << "}";
  file.close();
}


