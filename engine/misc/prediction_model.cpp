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
   this->loadModelConfig(modelName);
   this->_modelName = modelName;
}

string PredictionModel::getModelName() {
    return this->_modelName;
}

void PredictionModel::loadModelConfig(string modelName) {
   this->_modelConfigurationManager->LoadConfigFile("/tmp/" + modelName);
}

string PredictionModel::getInputDimensionString(){
    return _modelConfigurationManager->GetStringValue("input_dimension_specifications");
}

string PredictionModel::getOutputDimensionString(){
    return this->_modelConfigurationManager->GetStringValue("output_dimension_specifications");
}

int PredictionModel::getNumberOfInputDimensions(){
    int numberOfInputDimensions = int(split(this->getInputDimensionString(), '|').size());
    return numberOfInputDimensions;
}

string PredictionModel::getAttributeString(){
    return this->_modelConfigurationManager->GetStringValue("attribute_list");
}

vector<string> PredictionModel::getAttributeList(){
    string attrList = this->getAttributeString();
    return split(attrList, ',');
}

void PredictionModel::checkInputDimensions(SubtarPtr subtar){
   string dimString = _modelConfigurationManager->GetStringValue("input_dimension_specifications");

   auto dimSpecs = split(dimString, '|');
   for (auto entry : dimSpecs) {
         auto s = split(entry, '-');
         if(s[1].compare("*") == 0)
             continue;
         auto subtarDimSpec = subtar->GetDimensionSpecificationFor(s[0]);
         if(subtarDimSpec == NULL){
             throw std::runtime_error(s[0] + ": Dimension Specification is NULL\n");
         }
         else if(subtarDimSpec->GetSpecsType() == TOTAL)
         {
             throw std::runtime_error(s[0] + ": Dimension Specification cannot be of type TOTAL.\n"
                                             "Use operator SUBSET for filters.");
         }
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

list<DimensionPtr> PredictionModel::getOutputDimensionList() {
    auto dimString = this->getOutputDimensionString();
    list<DimensionPtr> dimensionList;
    auto dimensionSpecifications = split(this->getOutputDimensionString(), '|');
    for(auto dimSpecification : dimensionSpecifications){
        auto dimVector = split(dimSpecification, '-');
        DimensionPtr dimension =                             //Lower, upper bound and spacing
            make_shared<Dimension>(UNSAVED_ID, dimVector[0], INT32, 0, stoi(dimVector[1]), 1); //stoi(dimVector[1]) , 1);
        dimensionList.emplace_back(dimension);
    }
    return dimensionList;
}

list<pair<string, savime_size_t>> PredictionModel::getOutputAttributeList() {
    std::list<pair<string, savime_size_t>> outputAttributeList;
    pair<string, savime_size_t> outputAttribute;
    outputAttribute.first = "op_result";
    outputAttribute.second = 1;
    outputAttributeList.emplace_back(outputAttribute);
    return outputAttributeList;
}

PredictionModel::~PredictionModel() = default;
