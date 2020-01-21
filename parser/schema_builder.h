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
*    HERMANO L. S. LUSTOSA				JANUARY 2018
*/
#ifndef SCHEMA_BUILDER_H
#define SCHEMA_BUILDER_H

#include "../core/include/metadata.h"
#include "../core/include/storage_manager.h"
#include "../core/include/query_data_manager.h"

#define REPLACE_DATAELEMENT_FOR_ROLE(ROLES, OLDNAME, NEWNAME)\
  if(ROLES.find(OLDNAME) != ROLES.end()) {\
    auto role = ROLES[OLDNAME];\
    ROLES.erase(OLDNAME);\
    ROLES[NEWNAME] = role;\
  }

class SchemaBuilder
{
  ConfigurationManagerPtr _configurationManager;
  MetadataManagerPtr _metadataManager;
  StorageManagerPtr _storageManager;

  void SetResultingType(TARPtr inputTAR, TARPtr resultingTAR);
  TARPtr InferSchemaForScanOp(OperationPtr operation);
  TARPtr InferSchemaForSelectOp(OperationPtr operation);
  TARPtr InferSchemaForFilterOp(OperationPtr operation);
  TARPtr InferSchemaForSubsetOp(OperationPtr operation);
  TARPtr InferSchemaForLogicalOp(OperationPtr operation);
  TARPtr InferSchemaForComparisonOp(OperationPtr operation);
  TARPtr InferSchemaForArithmeticOp(OperationPtr operation);
  TARPtr InferSchemaForCrossOp(OperationPtr  operation);
  TARPtr InferSchemaForDimJoinOp(OperationPtr operation);
  TARPtr InferSchemaForAggregationOp(OperationPtr  operation);
  TARPtr InferSchemaForSplitOp(OperationPtr operation);
  TARPtr InferSchemaForPredict(OperationPtr operation);
  TARPtr InferSchemaForUserDefined(OperationPtr operation);

  public :

  SchemaBuilder(ConfigurationManagerPtr configurationManager,
                MetadataManagerPtr metadataManager,
                StorageManagerPtr storageManager)
  {
      _configurationManager = configurationManager;
      _metadataManager = metadataManager;
      _storageManager = storageManager;
  }

  TARPtr  InferSchema(OperationPtr  operation);
};


#endif /* SCHEMA_BUILDER_H */

