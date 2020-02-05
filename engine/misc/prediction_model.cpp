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
#include "include/json.h"

PredictionModel::PredictionModel(string modelName) {
   this->_modelConfigurationManager = new DefaultConfigurationManager();
   this->loadModelConfig(std::move(modelName));
}

void PredictionModel::loadModelConfig(string modelName) {
   this->_modelConfigurationManager->LoadConfigFile("/tmp/" + modelName);
}

string PredictionModel::getDimensionalString(){
    return _modelConfigurationManager->GetStringValue("dimension_specifications");
}

int PredictionModel::getNumberOfInputDimensions(){
    int numberOfInputDimensions = int(split(this->getDimensionalString(), '|').size());
    return numberOfInputDimensions;
}

string PredictionModel::getTargetAttributeName(){
    return this->_modelConfigurationManager->GetStringValue("target_attribute");
}

void PredictionModel::checkInputDimensions(SubtarPtr subtar){
   string dimString = _modelConfigurationManager->GetStringValue("dimension_specifications");

   auto dimSpecs = split(dimString, '|');
   for (auto entry : dimSpecs) {
         auto s = split(entry, '-');
         if(s.empty()) {
            throw std::runtime_error("Could not parse dimension string: \n" + dimString);
         }
         long int dimLength = subtar->GetDimensionSpecificationFor(s[0])->GetUpperBound() -
             subtar->GetDimensionSpecificationFor(s[0])->GetLowerBound() + 1;
         if (dimLength != std::stoi(s[1])) {
            string errorMsg = "Unexpected dimension length for " + s[0] + "." +
                "Expected: " + s[1] + " Found: " + std::to_string(dimLength);
            throw std::runtime_error(errorMsg);
         }
   }

}

PredictionModel::~PredictionModel() = default;
