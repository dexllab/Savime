//
// Created by anderson on 10/01/20.
//

#ifndef SAVIME_PREDICTOR_H
#define SAVIME_PREDICTOR_H

#include "engine/misc/include/prediction_model.h"
#include "engine/misc/include/json.h"
#include "dml_operators.h"
#include <core/include/system_logger.h>

class Predictor {
 public:
   vector<string> getPredictions(SubtarPtr subtar);
   Predictor(PredictionModel *predictionModel, StorageManagerPtr _storageManager, ConfigurationManagerPtr _configurationManager);
 private:
    PredictionModel *_predictionModel;
    StorageManagerPtr _storageManager;
    ConfigurationManagerPtr _configurationManager;
    Json::Value stepCreateJSonQuery(SubtarPtr subtar, vector<string> inputAttribute);
    Json::Value stepSendJsonToUrl(Json::Value jsonQuery, string modelName);
    Json::Value createJsonQuery(SubtarPtr subtar, StorageManagerPtr storageManager,
                               vector<string> inputAttribute);
    Json::Value fillDimensionArray(long int *bufferIndex, list<DatasetHandlerPtr> *dsHandlerList, SubtarPtr subtar,
                                 std::vector<string>::iterator *it, int dimCounter);
    void checkModelDimensions(SubtarPtr subtar);
    std::vector<string> getTargetAttributeList();

    vector<string> stepConvertJsonToVector(Json::Value &value);
};

#endif //SAVIME_PREDICTOR_H
