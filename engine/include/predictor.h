//
// Created by anderson on 10/01/20.
//

#ifndef SAVIME_PREDICTOR_H
#define SAVIME_PREDICTOR_H

#include "engine/misc/include/json.h"
#include "dml_operators.h"

class Predictor {
 public:
   vector<string> getPredictions(SubtarPtr subtar, StorageManagerPtr storageManager, string modelName, string predictedAttribute);
 private:
   Json::Value createJsonQuery(SubtarPtr subtar, StorageManagerPtr storageManager, string predictedAttribute);
   Json::Value fillDimensionArray(map<string, DimSpecPtr> *dimSpecs, std::map<string, DimSpecPtr>::iterator *it,
                                    int *bufferIndex, double* buffer, SubtarPtr subtar);
   void checkModelDimensions(string modelName, SubtarPtr subtar);
};

#endif //SAVIME_PREDICTOR_H
