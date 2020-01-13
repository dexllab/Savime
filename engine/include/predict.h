#ifndef SAVIME_PREDICT_H
#define SAVIME_PREDICT_H
#include "../core/include/engine.h"
#include "../core/include/util.h"
#include "../core/include/query_data_manager.h"
#include "../core/../core/include/storage_manager.h"
#include "default_engine.h"
#include <bits/stdc++.h>
#include <string>
#include <iostream>

int predict(SubTARIndex subtarIndex, OperationPtr operation,
          ConfigurationManagerPtr configurationManager,
          QueryDataManagerPtr queryDataManager,
          MetadataManagerPtr metadataManager, StorageManagerPtr storageManager,
          EnginePtr engine);
#endif //SAVIME_PREDICT_H

