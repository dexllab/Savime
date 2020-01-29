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

#include <utility>
#include "engine/misc/include/prediction_model.h"


PredictionModel::PredictionModel(string modelName) {
   this->_modelConfigurationManager = new DefaultConfigurationManager();
   this->loadModelConfig(std::move(modelName));
}

void PredictionModel::loadModelConfig(string modelName) {
   auto *configurationManager = new DefaultConfigurationManager();
   //configurationManager->LoadConfigFile("/home/anderson/Programacao/Savime/Savime/etc/savime.config");
   //this->_modelConfigurationManager->LoadConfigFile(configurationManager->GetStringValue("mdl_cfg_dir") + "/" + modelName);
   this->_modelConfigurationManager->LoadConfigFile("/tmp");
}

void PredictionModel::checkInputDimensions(SubtarPtr subtar){
   string dimString = _modelConfigurationManager->GetStringValue("dimension_specifications");

   auto dimSpecs = split(dimString, '|');
   for(auto entry : dimSpecs){
      auto s = split(entry, '-');
      long int dimLength = subtar->GetDimensionSpecificationFor(s[0])->GetUpperBound() -
          subtar->GetDimensionSpecificationFor(s[0])->GetLowerBound()+1;
      if(dimLength != std::stoi(s[1]))
      {
         string errorMsg = "Unexpected dimension length for " + s[0]+ "." +
             "Expected: " + s[1] + " Found: " + std::to_string(dimLength);
         throw std::runtime_error(errorMsg );
      }
   }
}

PredictionModel::~PredictionModel() = default;
