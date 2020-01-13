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

vector<string> Predictor::getPredictions(SubtarPtr subtar, StorageManagerPtr storageManager,
                                         string modelName, string predictedAttribute){
    Json::Value JSonQuery = createJsonQuery(subtar, storageManager, predictedAttribute);
    string url_address = "http://localhost:8501/v1/models/" + modelName + ":predict";
    Json::Value JSonPrediction = sendJsonToUrl(JSonQuery, url_address);

    /********Only for debug purposes*********/
    writeJsonFile("/home/anderson/Programacao/Savime/TinyModelServer/example/savime_json_output.json", JSonPrediction);

    return jsonArrayToVector(JSonPrediction["predictions"]);
}

Json::Value Predictor::fillDimensionArray(map<string, DimSpecPtr> *dimSpecs, std::map<string, DimSpecPtr>::iterator *it,
                               int *bufferIndex, double* buffer, SubtarPtr subtar){
    Json::Value dimArray;

    if(*it == dimSpecs->end()){//If it is the last dimension
        long int dimLength = 1;

        for (int i = 0; i < dimLength; ++i) {
            dimArray.append(buffer[*bufferIndex]);
            *bufferIndex += 1;
        }
        --(*it);
        return dimArray;
    }
    else {
        auto dimensionString = (*it)->first;
        long int dimLength = (long int) subtar->GetDimensionSpecificationFor(dimensionString)->GetUpperBound() -
            subtar->GetDimensionSpecificationFor(dimensionString)->GetLowerBound()+1;

        for (int i = 0; i < dimLength; i++) {
            dimArray[i] = fillDimensionArray(dimSpecs, &(++(*it)), bufferIndex, buffer, subtar);
        }
        --(*it);
        return dimArray;
    }
}

Json::Value Predictor::createJsonQuery(SubtarPtr subtar, StorageManagerPtr storageManager, string predictedAttribute) {
    auto dimSpecs = subtar->GetDimSpecs();
    auto datasetList = subtar->GetDataSets();
    DatasetHandlerPtr datasetHandler;
    for (auto ds : datasetList) {
      if(ds.first == predictedAttribute){
          datasetHandler = storageManager->GetHandler(ds.second);
      }
    }
    double* buffer = (double*) datasetHandler->GetBuffer();

    Json::Value JSonQuery;

    int ct = 0;
    auto it = dimSpecs.begin();
    Json::Value dimensionalArray = fillDimensionArray(&dimSpecs, &it, &ct, buffer, subtar);

    JSonQuery["signature_name"] = "serving_default";
    JSonQuery["instances"] = dimensionalArray ;

    /********Only for debug purposes*********/
    writeJsonFile("/home/anderson/Programacao/Savime/TinyModelServer/example/savime_json_test.json", JSonQuery);

    return JSonQuery;
}

