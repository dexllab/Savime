//
// Created by anderson on 10/01/20.
//

#ifndef SAVIME_PREDICTOR_H
#define SAVIME_PREDICTOR_H

#include "engine/misc/include/prediction_model.h"
#include "engine/misc/include/json.h"
#include "dml_operators.h"

class Predictor {
 public:
   vector<string> getPredictions(SubtarPtr subtar, StorageManagerPtr storageManager, string modelName);
   Predictor(PredictionModel *predictionModel);
 private:
   PredictionModel *_predictionModel;
   Json::Value createJsonQuery(SubtarPtr subtar, StorageManagerPtr storageManager,
                               vector<string> inputAttribute);
  Json::Value fillDimensionArray(long int *bufferIndex, list<DatasetHandlerPtr> *dsHandlerList, SubtarPtr subtar,
                                 std::vector<string>::iterator *it, int dimCounter);
   void checkModelDimensions(SubtarPtr subtar);
  std::vector<string> getTargetAttributeList();
};

#endif //SAVIME_PREDICTOR_H
