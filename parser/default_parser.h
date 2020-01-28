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
#ifndef DEFAULT_PARSER_H
#define DEFAULT_PARSER_H

#include "tree.h"
#include "bison.h"
#include "schema_builder.h"
#include "../core/include/parser.h"
#include "../core/include/storage_manager.h"

extern ParseTreeNode * rootNode;
typedef struct yy_buffer_state * YY_BUFFER_STATE;
YY_BUFFER_STATE yy_scan_string(const char * str, void * scanner);
int yylex_init (void ** scanner);
int yylex_destroy (void * yyscanner );
int yyget_debug (void * yyscanner );

extern int yylex \
               (YYSTYPE * yylval_param ,void * yyscanner);

#define YY_DECL int yylex \
               (YYSTYPE * yylval_param , void * yyscanner)



class DefaultParser : public Parser {
  public:
    MetadataManagerPtr _metadaManager;
    StorageManagerPtr _storageManager;
    std::shared_ptr<SchemaBuilder> _schemaBuilder;
    TARSPtr _currentTARS;

    // UTIL
    TARPtr ParseTAR(ValueExpressionPtr param, string errorMsg,
                    QueryPlanPtr queryPlan, int &idCounter);
    int CreateQueryPlan(ParseTreeNodePtr root,
                        QueryDataManagerPtr queryDataManager);
    TARPtr ParseOperation(QueryExpressionPtr queryExpressionNode,
                          QueryPlanPtr queryPlan, int &idCounter);
    bool ValidateNumericalFunction(QueryExpressionPtr expression,
                                   TARPtr inputTAR);
    bool ValidateBoolFunction(QueryExpressionPtr expression, TARPtr inputTAR);
    OperationPtr ParseDMLOperation(QueryExpressionPtr queryExpressionNode,
                                   QueryPlanPtr queryPlan, int &idCounter);
  private:
    // DDL
    OperationPtr ParseCreateTARS(QueryExpressionPtr queryExpressionNode,
                                 QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseCreateTAR(QueryExpressionPtr queryExpressionNode,
                                QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseCreateType(QueryExpressionPtr queryExpressionNode,
                                 QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseCreateDataset(QueryExpressionPtr queryExpressionNode,
                                    QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseLoad(QueryExpressionPtr queryExpressionNode,
                           QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDelete(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDropTARS(QueryExpressionPtr queryExpressionNode,
                               QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDropTAR(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDropType(QueryExpressionPtr queryExpressionNode,
                               QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDropDataset(QueryExpressionPtr queryExpressionNode,
                                  QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseSave(QueryExpressionPtr queryExpressionNode,
                                  QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseShow(QueryExpressionPtr queryExpressionNode,
                           QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseBatch(QueryExpressionPtr queryExpressionNode,
                            QueryPlanPtr queryPlan, int &idCounter);
    // DML
    OperationPtr ParseLogical(ValueExpressionPtr valueExpression, TARPtr inputTAR,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseComparison(ValueExpressionPtr valueExpression,
                                 TARPtr inputTAR, QueryPlanPtr queryPlan,
                                 int &idCounter);
    OperationPtr ParseArithmetic(ValueExpressionPtr valueExpression,
                                 TARPtr inputTAR, std::string newMember,
                                 QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseScan(QueryExpressionPtr queryExpressionNode,
                           QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseSelect(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseFilter(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseSubset(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDerive(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseCross(QueryExpressionPtr queryExpressionNode,
                            QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseSlice(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseSplit(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseReorient(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseDimJoin(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseEquiJoin(QueryExpressionPtr queryExpressionNode,
                                 QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseAtt2Dim(QueryExpressionPtr queryExpressionNode,
                                 QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseUnion(QueryExpressionPtr queryExpressionNode,
                            QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseTranslate(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseAggregate(QueryExpressionPtr queryExpressionNode,
                                QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParsePredict(QueryExpressionPtr queryExpressionNode,
                                QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseRegisterModel(QueryExpressionPtr queryExpressionNode,
                                      QueryPlanPtr queryPlan, int &idCounter);
    OperationPtr ParseUserDefined(QueryExpressionPtr queryExpressionNode,
                                  QueryPlanPtr queryPlan, int &idCounter);

  public:
    DefaultParser(ConfigurationManagerPtr configurationManager,
                  SystemLoggerPtr systemLogger)
        : Parser(configurationManager, systemLogger) {
      _schemaBuilder == nullptr;
    }

    void SetMetadataManager(MetadataManagerPtr metadaManager);
    void SetStorageManager(StorageManagerPtr storageManager);
    TARPtr InferOutputTARSchema(OperationPtr operation);
    SavimeResult Parse(QueryDataManagerPtr queryDataManager);
};

#endif /* DEFAULT_PARSER_H */

