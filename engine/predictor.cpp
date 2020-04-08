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

#include "predictor.h"
#include "include/dml_operators.h"
#include "engine/misc/include/json.h"
#include <curl/curl.h>
#include <list>
#include <jsoncpp/json/json.h>
#include <jsoncpp/json/reader.h>
#include <json_parser.h>
#include <engine/misc/include/curl.h>
#include <engine/misc/include/prediction_model.h>
#include <utility>

Predictor::Predictor(PredictionModel *predictionModel) {
    this->_predictionModel= predictionModel;
}

void Predictor::checkModelDimensions(SubtarPtr subtar)
{
    try{
        this->_predictionModel->checkInputDimensions(subtar);
    }catch(std::exception &e){
        throw std::runtime_error(ERROR_MSG("operation", "PREDICT") + "\n" + e.what());
    }
}

vector<string> Predictor::getPredictions(SubtarPtr subtar, StorageManagerPtr storageManager, string modelName) {
    modelName.erase(std::remove(modelName.begin(),modelName.end(),'\"'),modelName.end());
    this->checkModelDimensions(subtar);

    std::vector<string> targetAttributeList = this->getTargetAttributeList();
    Json::Value JSonQuery = this->createJsonQuery(subtar, storageManager, targetAttributeList);
    string url_address = "http://localhost:8501/v1/models/" + modelName + ":predict";
    Json::Value JSonPrediction = sendJsonToUrl(JSonQuery, url_address);
    if(JSonPrediction["error"].type() == Json::stringValue)
    {
        throw std::runtime_error("Error while communicating to prediction server: \n TFX: " +
            JSonPrediction["error"].asString());
    }
    vector<string> result = jsonArrayToVector(JSonPrediction["predictions"]);
    return result;
}

int getAttributeLengthFor(SubtarPtr subtar, string attributeName)
{
    auto a = subtar->GetTAR()->GetAttributes();
    for(auto entry : a){
        if(entry->GetName() == attributeName) {
            return entry->GetType().vectorLength();
        }
    }
    return -1;
}

Json::Value Predictor::fillDimensionArray(long int *bufferIndex,
                                          list<DatasetHandlerPtr> *dsHandlerList,
                                          SubtarPtr subtar,
                                          std::vector<string>::iterator *it,
                                          int dimCounter){
    Json::Value dimArray;
    auto dimSpec = split(**it, '-');
    string dimName = dimSpec[0];
    long int dimLength = std::stol(dimSpec[1]);

    if(dimCounter == this->_predictionModel->getNumberOfInputDimensions()){//If it is the last dimension
        vector<string> sourceAttributeList = this->_predictionModel->getAttributeList();
        int attributeLength = getAttributeLengthFor(subtar, sourceAttributeList[0]);
        for (long int i = 0; i < dimLength; ++i) {
            if(attributeLength > 1 or sourceAttributeList.size() > 1) {
                Json::Value attributeListValue;
                for (long int j = 0; j < attributeLength; j++) {
                    for (std::list<DatasetHandlerPtr>::iterator it = dsHandlerList->begin(); it != dsHandlerList->end(); ++it)
                        if((*it)->GetDataSet()->GetType() == INT64)
                        {
                            attributeListValue.append(((int *) (*it)->GetBuffer())[*bufferIndex]);
                        } else if ((*it)->GetDataSet()->GetType() == DOUBLE)
                        {
                            attributeListValue.append(((double *) (*it)->GetBuffer())[*bufferIndex]);
                        }

                        //attributeValue.append(dsHandlerList->[*bufferIndex]);
                    *bufferIndex += 1;
                }
                dimArray.append(attributeListValue);
            } else {
                std::list<DatasetHandlerPtr>::iterator it = dsHandlerList->begin();
                dimArray.append(((double *) (*it)->GetBuffer())[*bufferIndex]);
                *bufferIndex += 1;
            }
        }
        --(*it);
        return dimArray;
    }
    else {
        for (long int i = 0; i < dimLength; i++) {
            dimArray.append(fillDimensionArray(bufferIndex, dsHandlerList, subtar, &(++(*it)), dimCounter+1));
        }
        --(*it);
        return dimArray;
    }
}

DatasetHandlerPtr getDatasetHandler(SubtarPtr subtar, StorageManagerPtr storageManager, string attribute){
    auto datasetList = subtar->GetDataSets();
    DatasetHandlerPtr datasetHandler;
    for (auto ds : datasetList) {
        if (ds.first == attribute) {
            datasetHandler = storageManager->GetHandler(ds.second);
        }
    }
    return datasetHandler;
}

Json::Value Predictor::createJsonQuery(SubtarPtr subtar, StorageManagerPtr storageManager,
                                       vector<string> inputAttribute) {
    auto dimSpecs = subtar->GetDimSpecs();

    list<DatasetHandlerPtr> datasetHandlerList;
    for(vector <string> :: iterator it = inputAttribute. begin(); it != inputAttribute. end(); ++it){
        auto datasetHandler = getDatasetHandler(subtar, storageManager, *it);
        datasetHandlerList.push_back(datasetHandler);
    }

    if(!datasetHandlerList.empty()) {
        Json::Value JSonQuery;

        long int ct = 0;
        string dimString = this->_predictionModel->getInputDimensionString();
        auto dimList = split(dimString, '|');
        auto it = dimList.begin();
        Json::Value dimensionalArray = fillDimensionArray(&ct, &datasetHandlerList, subtar, &it, 1);
        JSonQuery["signature_name"] = "serving_default";
        JSonQuery["instances"] = dimensionalArray;
        writeJsonFile("/tmp/input.json", JSonQuery);
        return JSonQuery;
    }else{
        throw std::runtime_error(ERROR_MSG("Could not get handler for dataset", "PREDICT"));
    }
}
std::vector<string> Predictor::getTargetAttributeList() {
    return this->_predictionModel->getAttributeList();
}

