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
#include <stdexcept>
#include <algorithm>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_map>

#include "default_parser.h"
#include "tree.h"
#include "bison.h"
#include "../core/include/metadata.h"
#include "register_model_parser.h"

enum OperandsType {
  __IDENTIFIER,
  __EXPRESSION,
  __LITERAL,
  ___FUNCTION,
  __NONE
};
const char *select_error = "Invalid parameter for operator SELECT. Expected "
                           "SELECT(tar, schema_element [, ...,  "
                           "schema_element])";
const char *scan_error =
    "Invalid parameter for operator SCAN. Expected SCAN(tar)";
const char *filter_error = "Invalid parameter for operator WHERE. Expected "
                           "WHERE(tar, logical_expression)";
const char *derive_error = "Invalid parameter for operator DERIVE. Expected "
                           "DERIVE(tar, identifier, arithmetic_expression)";
const char *subset_error = "Invalid parameter for operator SUBSET. Expected "
                           "SUBSET(tar, dim_name, lower_bound1, upper_bound [, "
                           "..., dim_nameN, lower_bound1, upper_bound1])";
const char *cross_error =
    "Invalid parameter for operator CROSS. Expected CROSS(left_tar, right_tar)";
const char *dimjoin_error = "Invalid parameter for operator DIMJOIN. Expected "
                            "DIMJOIN(left_tar, right_tar, [left_tar_dim1, "
                            "right_tar_dim2, ...])";
const char *aggregation_error = "Invalid parameter for operator AGGREGATE. "
                                "Expected AGGREGATE(tar, aggregation_function, "
                                "aggregation_function_param, new_attrib_name, "
                                "[aggr_dim1, aggr_dim2, ..., aggr_dimN])";
const char *split_error =
    "Invalid parameter for operator SPLIT. Expected SPLIT(tar, ideal_size)";

const char *reorient_error =
  "Invalid parameter for operator REORIENT. Expected REORIENT(tar, major_dim)";

const char *predict_error = "Invalid parameter for operator PREDICT. Expected "
                             "PREDICT(tar, \"model\", attribute)";

using namespace std;

// UTIL FUNCTIONS
void DefaultParser::SetMetadataManager(MetadataManagerPtr metadaManager) {
  _metadaManager = metadaManager;
}

void DefaultParser::SetStorageManager(StorageManagerPtr storageManager) {
  _storageManager = storageManager;
}

bool DefaultParser::ValidateNumericalFunction(QueryExpressionPtr expression,
                                              TARPtr inputTAR) {
  string functionName = GET_IDENTIFER_BODY(expression);

  if (!_configurationManager->GetBooleanValue(
      NUMERICAL_FUNCTION(functionName)))
    return false;

  int paramsNumber = _configurationManager->GetIntValue(
      NUMERICAL_FUNCTION_PARAMS(functionName));
  auto paramList = expression->_value_expression_list->ParamsToList();

  if (paramList.size() != paramsNumber)
    return false;

  for (auto param : paramList) {
    if (PARSE(param, UnsignedNumericLiteral) ||
        PARSE(param, SignedNumericLiteral))
      continue;

    if (IdentifierChainPtr identifier = PARSE(param, IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier)))
        continue;
    }

    return false;
  }

  return true;
}

bool DefaultParser::ValidateBoolFunction(QueryExpressionPtr expression,
                                         TARPtr inputTAR) {
  string functionName = GET_IDENTIFER_BODY(expression);

  if (!_configurationManager->GetBooleanValue(
      BOOLEAN_FUNCTION(functionName)))
    return false;

  int paramsNumber = _configurationManager->GetIntValue(
      BOOLEAN_FUNCTION_PARAMS(functionName));
  auto paramList = expression->_value_expression_list->ParamsToList();

  if (paramList.size() != paramsNumber)
    return false;

  for (auto param : paramList) {
    if (PARSE(param, UnsignedNumericLiteral) ||
        PARSE(param, SignedNumericLiteral))
      continue;

    if (IdentifierChainPtr identifier = PARSE(param, IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier)))
        continue;
    }

    return false;
  }

  return true;
}

TARPtr DefaultParser::ParseTAR(ValueExpressionPtr param, string errorMsg,
                               QueryPlanPtr queryPlan, int &idCounter) {
  IdentifierChainPtr identifier;
  QueryExpressionPtr queryExpression;
  TARPtr tar;

  if (identifier = PARSE(param, IdentifierChain)) {
    tar = _metadaManager->GetTARByName(_currentTARS,
                                       GET_IDENTIFER_BODY(identifier));
    if (!tar) {
      throw std::runtime_error("Identifier " + GET_IDENTIFER_BODY(identifier) +
          " is not a valid TAR name.");
    }
    return tar;
  } else if (queryExpression = PARSE(param, QueryExpression)) {
    tar = ParseOperation(queryExpression, std::move(queryPlan), idCounter);
    if (tar == nullptr) {
      throw std::runtime_error(errorMsg);
    }
    return tar;
  } else {
    throw std::runtime_error(errorMsg);
  }
}

// DML FUNCTIONS
OperationPtr
DefaultParser::ParseCreateTARS(QueryExpressionPtr queryExpressionNode,
                               QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_CREATE_TARS);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  std::list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (commandString = PARSE(params.front(), CharacterStringLiteral)) {
    operation->AddParam(PARAM(TAL_CREATE_TARS, _COMMAND),
                        commandString->_literalString);

  } else {
    throw std::runtime_error("Invalid parameters for create_tars operator.");
  }

  return operation;
}

OperationPtr
DefaultParser::ParseCreateTAR(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_CREATE_TAR);
  operation->SetResultingTAR(nullptr);
  std::list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() >= 4) {
    for (auto param : params) {
      if (CharacterStringLiteralPtr commandString =
          CharacterStringLiteralPtr(PARSE(param, CharacterStringLiteral)))
        operation->AddParam(PARAM(TAL_CREATE_TAR, _COMMAND),
                            commandString->_literalString);
      else
        throw std::runtime_error("Invalid parameters for create_tar operator.");
    }
  } else {
    throw std::runtime_error("Invalid parameters for create_tar operator.");
  }

  return operation;
}

OperationPtr
DefaultParser::ParseCreateType(QueryExpressionPtr queryExpressionNode,
                               QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_CREATE_TYPE);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 1 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_CREATE_TYPE, _COMMAND),
                        commandString->_literalString);
  } else {
    throw std::runtime_error("Invalid parameters for create_type operator.");
  }

  return operation;
}

OperationPtr
DefaultParser::ParseCreateDataset(QueryExpressionPtr queryExpressionNode,
                                  QueryPlanPtr queryPlan, int &idCounter) {
#define LITERAL "literal"
  OperationPtr operation = std::make_shared<Operation>(TAL_CREATE_DATASET);
  QueryExpressionPtr queryExpression;
  int32_t op_count = 1;
  CharacterStringLiteralPtr stringLiteral;
  UnsignedNumericLiteralPtr unsignedNumericLiteral;
  SignedNumericLiteralPtr signedNumericLiteral;
  CharacterStringLiteralPtr commandString, filler;
  operation->SetResultingTAR(nullptr);

  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 2 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral)) &&
      (filler = PARSE(params.back(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_CREATE_DATASET, _COMMAND),
                        commandString->_literalString);
    operation->AddParam(PARAM(TAL_CREATE_DATASET, _OPERAND, 0),
                        filler->_literalString);

  } else if (params.size() == 2 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral)) &&
      (queryExpression = PARSE(params.back(), QueryExpression))) {
    operation->AddParam(PARAM(TAL_CREATE_DATASET, _COMMAND),
                        commandString->_literalString);
    string identifier = GET_IDENTIFER_BODY(queryExpression);

    if (identifier == LITERAL) {
      operation->AddParam(PARAM(TAL_CREATE_DATASET, _OPERAND, 0),
                          LITERAL_FILLER_MARK);

      list<ValueExpressionPtr> literalParams =
          queryExpression->_value_expression_list->ParamsToList();
      while (!literalParams.empty()) {
        ValueExpressionPtr param = literalParams.front();

        if (stringLiteral = PARSE(param, CharacterStringLiteral)) {
          operation->AddParam(PARAM(TAL_CREATE_DATASET, _OPERAND, op_count++),
                              stringLiteral->_literalString);
        } else if (unsignedNumericLiteral =
                       PARSE(param, UnsignedNumericLiteral)) {
          operation->AddParam(PARAM(TAL_CREATE_DATASET, _OPERAND, op_count++),
                              to_string(unsignedNumericLiteral->_doubleValue));
        } else if (signedNumericLiteral = PARSE(param, SignedNumericLiteral)) {
          operation->AddParam(PARAM(TAL_CREATE_DATASET, _OPERAND, op_count++),
                              to_string(signedNumericLiteral->_doubleValue));
        } else {
          throw std::runtime_error("Invalid params for literal definition.");
        }

        literalParams.pop_front();
      }
    } else {
      throw std::runtime_error("Literal definition expected but " + identifier +
          " found.");
    }
  } else {
    throw std::runtime_error("Invalid parameters for create_dataset operator.");
  }

  return operation;
}

OperationPtr DefaultParser::ParseLoad(QueryExpressionPtr queryExpressionNode,
                                      QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_LOAD_SUBTAR);
  operation->SetResultingTAR(nullptr);
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() >= 3) {
    for (const auto &param : params) {
      if (CharacterStringLiteralPtr commandString =
          CharacterStringLiteralPtr(PARSE(param, CharacterStringLiteral)))
        operation->AddParam(PARAM(TAL_LOAD_SUBTAR, _COMMAND),
                            commandString->_literalString);

      else
        throw std::runtime_error(
            "Invalid parameters for load_subtar operator.");
    }
  } else {
    throw std::runtime_error("Invalid parameters for create_tar operator.");
  }

  return operation;
}

OperationPtr DefaultParser::ParseDelete(QueryExpressionPtr queryExpressionNode,
                                      QueryPlanPtr queryPlan, int &idCounter) {

}

OperationPtr
DefaultParser::ParseDropTARS(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_DROP_TARS);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 1 && (PARSE(params.front(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_DROP_TARS, _COMMAND),
                        commandString->_literalString);
  } else {
    throw std::runtime_error("Invalid parameters for drop_tars operator.");
  }

  return operation;
}

OperationPtr DefaultParser::ParseDropTAR(QueryExpressionPtr queryExpressionNode,
                                         QueryPlanPtr queryPlan,
                                         int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_DROP_TAR);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 1 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_DROP_TAR, _COMMAND),
                        commandString->_literalString);
  } else {
    throw std::runtime_error("Invalid parameters for drop_tar operator.");
  }

  return operation;
}

OperationPtr
DefaultParser::ParseDropType(QueryExpressionPtr queryExpressionNode,
                             QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_DROP_TYPE);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 1 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_DROP_TYPE, _COMMAND),
                        commandString->_literalString);
  } else {
    throw std::runtime_error("Invalid parameters for drop_type operator.");
  }

  return operation;
}

OperationPtr
DefaultParser::ParseDropDataset(QueryExpressionPtr queryExpressionNode,
                                QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_DROP_DATASET);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 1 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_DROP_DATASET, _COMMAND),
                        commandString->_literalString);
  } else {
    throw std::runtime_error("Invalid parameters for create_type operator.");
  }

  return operation;
}

OperationPtr DefaultParser::ParseSave(QueryExpressionPtr queryExpressionNode,
                                      QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_SAVE);
  operation->SetResultingTAR(nullptr);
  CharacterStringLiteralPtr commandString;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.size() == 1 &&
      (commandString = PARSE(params.front(), CharacterStringLiteral))) {
    operation->AddParam(PARAM(TAL_SAVE, _COMMAND),
                        commandString->_literalString);
  } else {
    throw std::runtime_error("Invalid parameters for save operator.");
  }

  return operation;
}

OperationPtr DefaultParser::ParseShow(QueryExpressionPtr queryExpressionNode,
                                      QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_SHOW);
  operation->SetResultingTAR(nullptr);
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.front() != nullptr) {
    throw std::runtime_error("Invalid parameters for show operator.");
  }

  return operation;
}

OperationPtr DefaultParser::ParseBatch(QueryExpressionPtr queryExpressionNode,
                                       QueryPlanPtr queryPlan, int &idCounter) {
  QueryExpressionPtr subexpression;
  list<ValueExpressionPtr> params =
      queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (domain)
  if (params.empty()) {
    throw std::runtime_error("Invalid parameters for batch operator.");
  }

  for (const ValueExpressionPtr &expression : params) {
    if (subexpression = PARSE(expression, QueryExpression)) {
      ParseDMLOperation(subexpression, queryPlan, idCounter);
    } else {
      throw std::runtime_error("Invalid parameters for batch operator.");
    }
  }

  auto lastOp = queryPlan->GetOperations().back();
  queryPlan->GetOperations().pop_back();

  return lastOp;
}

// DDL FUNCTIONS
OperationPtr DefaultParser::ParseLogical(ValueExpressionPtr valueExpression,
                                         TARPtr inputTAR,
                                         QueryPlanPtr queryPlan,
                                         int &idCounter) {
  LogicalConjunctionPtr logicalConjuction;
  LogicalDisjunctionPtr logicalDisjunction;
  BooleanValueExpressionPtr booleanValueExpression;
  ValueExpressionPtr operands[2];
  int opCount = 0;

  OperationPtr operation = std::make_shared<Operation>(TAL_LOGICAL);
  operation->AddParam(PARAM(TAL_LOGICAL, _INPUT_TAR), inputTAR);

  if (logicalConjuction = PARSE(valueExpression, LogicalConjunction)) {
    operation->AddParam(PARAM(TAL_LOGICAL, _OPERATOR), std::string(_OR));
    operands[0] = ValueExpressionPtr(logicalConjuction->_leftOperand);
    operands[1] = ValueExpressionPtr(logicalConjuction->_rightOperand);
    opCount = 2;
  } else if (logicalDisjunction = PARSE(valueExpression, LogicalDisjunction)) {
    operation->AddParam(PARAM(TAL_LOGICAL, _OPERATOR), std::string(_AND));
    operands[0] = ValueExpressionPtr(logicalDisjunction->_leftOperand);
    operands[1] = ValueExpressionPtr(logicalDisjunction->_rightOperand);
    opCount = 2;
  } else if (booleanValueExpression =
                 PARSE(valueExpression, BooleanValueExpression)) {
    operation->AddParam(PARAM(TAL_LOGICAL, _OPERATOR), std::string(_NOT));
    operands[0] =
        ValueExpressionPtr(booleanValueExpression->_notValueExpression);
    opCount = 1;
  } else {
    throw std::runtime_error("Invalid operation.");
  }

  for (int i = 0; i < opCount; i++) {
    if (PARSE(operands[i], IdentifierChain)) {
      throw std::runtime_error(
          "Support to boolean attributes not implemented yet.");
    } else if (PARSE(operands[i], ComparisonPredicate)) {
      OperationPtr comparison =
          ParseComparison(operands[i], inputTAR, queryPlan, idCounter);
      // operation->AddParam("", comparison->GetResultingTAR());
      operation->AddParam(PARAM(TAL_LOGICAL, _OPERAND, i),
                          comparison->GetResultingTAR());

      queryPlan->AddOperation(comparison, idCounter);
    } else if (PARSE(operands[i], LogicalConjunction) ||
        PARSE(operands[i], LogicalDisjunction) ||
        PARSE(operands[i], BooleanValueExpression)) {

      OperationPtr logicalOperation =
          ParseLogical(operands[i], inputTAR, queryPlan, idCounter);

      operation->AddParam(PARAM(TAL_LOGICAL, _OPERAND, i),
                          logicalOperation->GetResultingTAR());

      queryPlan->AddOperation(logicalOperation, idCounter);
    } else {
      throw std::runtime_error("Invalid operators for logic operation.");
    }
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseComparison(ValueExpressionPtr valueExpression,
                                            TARPtr inputTAR,
                                            QueryPlanPtr queryPlan,
                                            int &idCounter) {
  ComparisonPredicatePtr comparisonPredicate;
  OperationPtr operation = std::make_shared<Operation>(TAL_COMPARISON);
  IdentifierChainPtr identifier; std::string symbol;

  // Expression can be either numeric or string
  string strVals[] = {"", ""};
  bool isNumeric[] = {false, false};
  double numVals[] = {0, 0};
  OperandsType operandsType[] = {__LITERAL, __LITERAL};
  ValueExpressionPtr operands[2];

  // Defining type of comparison: >, >=, <, <=, =, <>, like
  if (comparisonPredicate = PARSE(valueExpression, ComparisonPredicate)) {
    symbol = comparisonPredicate->_comparisonOperator->toString();
    operation->AddParam(PARAM(TAL_COMPARISON, _OPERATOR), symbol);
    operands[0] = ValueExpressionPtr(comparisonPredicate->_leftOperand);
    operands[1] = ValueExpressionPtr(comparisonPredicate->_rightOperand);
  } else {
    throw std::runtime_error("Invalid comparison operation.");
  }

  // Defining type of operands (LITERAL, EXPRESSION or IDENTIFIER)
  for (int i = 0; i < 2; i++) {
    if (identifier = PARSE(operands[i], IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
        isNumeric[i] = inputTAR->GetDataElement(GET_IDENTIFER_BODY(identifier))
                               ->IsNumeric();
        operandsType[i] = __IDENTIFIER;
        strVals[i] = identifier->_identifier->_identifierBody;
      } else {
        throw std::runtime_error("Data element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");
      }
    } else if (PARSE(operands[i], SummationNumericExpression) ||
        PARSE(operands[i], SubtractionNumericalExpression) ||
        PARSE(operands[i], ProductNumericalExpression) ||
        PARSE(operands[i], DivisonNumericalExpression) ||
        PARSE(operands[i], ModulusNumericalExpression) ||
        PARSE(operands[i], PowerNumericalExpression)) {
      isNumeric[i] = true;
      operandsType[i] = __EXPRESSION;
    } else if (PARSE(operands[i], UnsignedNumericLiteral)) {
      isNumeric[i] = true;
      operandsType[i] = __LITERAL;
      numVals[i] = PARSE(operands[i], UnsignedNumericLiteral)->_doubleValue;
    } else if (PARSE(operands[i], SignedNumericLiteral)) {
      isNumeric[i] = true;
      operandsType[i] = __LITERAL;
      numVals[i] = PARSE(operands[i], SignedNumericLiteral)->_doubleValue;
    } else if (PARSE(operands[i], CharacterStringLiteral)) {
      isNumeric[i] = false;
      operandsType[i] = __LITERAL;
      strVals[i] = PARSE(operands[i], CharacterStringLiteral)->_literalString;
    } else if (PARSE(operands[i], QueryExpression)) {
      isNumeric[i] = true;
      operandsType[i] = __EXPRESSION;
    } else {
      throw std::runtime_error("Invalid input for comparison operation.");
    }
  }

  // Checking if types are valid
  if (isNumeric[0] != isNumeric[1]) {
    throw std::runtime_error("Invalid input types for comparison operation.");
  }

  if(symbol == _LIKE && isNumeric[0]) {
    throw std::runtime_error("Invalid LIKE operator usage with numeric types.");
  }

  // Continuing parsing according to input operators
  if (operandsType[0] == __LITERAL && operandsType[1] == __LITERAL) {
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR), inputTAR);

    if (isNumeric[0]) {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 0), numVals[0]);
    } else {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 0), strVals[0]);
    }

    if (isNumeric[1]) {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 1), numVals[1]);
    } else {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 1), strVals[1]);
    }
  } else if (operandsType[0] == __LITERAL && operandsType[1] == __EXPRESSION) {

    std::string newMember = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);
    OperationPtr expression =
        ParseArithmetic(operands[1], inputTAR, newMember, queryPlan, idCounter);
    queryPlan->AddOperation(expression, idCounter);
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR),
                        expression->GetResultingTAR());

    if (isNumeric[0]) {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 0), numVals[0]);
    } else {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 0), strVals[0]);
    }

    // param is last attribute added to schema for resulting domain
    // operation->AddParam(IDENTIFIER, newMember);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 1),
                                  newMember);

  } else if (operandsType[0] == __LITERAL && operandsType[1] == __IDENTIFIER) {
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR), inputTAR);

    if (isNumeric[0]) {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 0), numVals[0]);
    } else {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 0), strVals[0]);
    }

    // operation->AddParam(IDENTIFIER, strVals[1]);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 1),
                                  strVals[1]);

  } else if (operandsType[0] == __EXPRESSION && operandsType[1] == __LITERAL) {
    std::string newMember = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);
    OperationPtr expression =
        ParseArithmetic(operands[0], inputTAR, newMember, queryPlan, idCounter);
    queryPlan->AddOperation(expression, idCounter);
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR),
                        expression->GetResultingTAR());

    // param is last attribute added to schema for resulting domain
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 0),
                                  newMember);

    if (isNumeric[1]) {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 1), numVals[1]);
    } else {
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 1), strVals[1]);
    }
  } else if (operandsType[0] == __EXPRESSION &&
      operandsType[1] == __EXPRESSION) {
    std::string newMember1 = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);
    std::string newMember2 = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);

    // pipelining arithmetic expressions
    OperationPtr expression1 = ParseArithmetic(
        operands[0], inputTAR, newMember1, queryPlan, idCounter);
    queryPlan->AddOperation(expression1, idCounter);
    OperationPtr expression2 =
        ParseArithmetic(operands[1], expression1->GetResultingTAR(), newMember2,
                        queryPlan, idCounter);
    queryPlan->AddOperation(expression2, idCounter);
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR),
                        expression2->GetResultingTAR());
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 0),
                                  newMember1);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 1),
                                  newMember2);

  } else if (operandsType[0] == __EXPRESSION &&
      operandsType[1] == __IDENTIFIER) {
    std::string newMember = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);
    OperationPtr expression =
        ParseArithmetic(operands[0], inputTAR, newMember, queryPlan, idCounter);
    queryPlan->AddOperation(expression, idCounter);
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR),
                        expression->GetResultingTAR());

    // param is last attribute added to schema for resulting domain
    // operation->AddParam(IDENTIFIER, newMember);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 0),
                                  newMember);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 1),
                                  strVals[1]);

  } else if (operandsType[0] == __IDENTIFIER && operandsType[1] == __LITERAL) {
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR), inputTAR);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 0),
                                  strVals[0]);

    if (isNumeric[1]) {
      // operation->AddParam(LITERAL, numVals[1]);
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 1), numVals[1]);
    } else {
      // operation->AddParam(LITERAL, strVals[1]);
      operation->AddParam(PARAM(TAL_COMPARISON, _OPERAND, 1), strVals[1]);
    }
  } else if (operandsType[0] == __IDENTIFIER &&
      operandsType[1] == __EXPRESSION) {
    std::string newMember = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);
    OperationPtr expression =
        ParseArithmetic(operands[1], inputTAR, newMember, queryPlan, idCounter);

    queryPlan->AddOperation(expression, idCounter);
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR),
                        expression->GetResultingTAR());

    // param is last attribute added to schema for resulting domain
    // operation->AddParam(IDENTIFIER, strVals[0]);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 0),
                                  strVals[0]);
    // operation->AddParam(IDENTIFIER, newMember);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 1),
                                  newMember);

  } else if (operandsType[0] == __IDENTIFIER &&
      operandsType[1] == __IDENTIFIER) {
    operation->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR), inputTAR);
    // operation->AddParam(IDENTIFIER, strVals[0]);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 0),
                                  strVals[0]);
    // operation->AddParam(IDENTIFIER, strVals[1]);
    operation->AddIdentifierParam(PARAM(TAL_COMPARISON, _OPERAND, 1),
                                  strVals[1]);
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseArithmetic(ValueExpressionPtr valueExpression,
                                            TARPtr inputTAR,
                                            std::string newMember,
                                            QueryPlanPtr queryPlan,
                                            int &idCounter) {
#define MAX_FUNC_PARAMS 100

  OperationPtr operation = std::make_shared<Operation>(TAL_ARITHMETIC);
  std::vector<std::string> strVals(MAX_FUNC_PARAMS);
  std::vector<double> numVals(MAX_FUNC_PARAMS);
  std::vector<OperandsType> operandsType(MAX_FUNC_PARAMS);
  std::vector<ValueExpressionPtr> operands(2);

  if (std::shared_ptr<SummationNumericExpression> expression =
      PARSE(valueExpression, SummationNumericExpression)) {
    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR),
                        std::string(_ADDITION));
    operands[0] = ValueExpressionPtr(expression->_leftOperand);
    operands[1] = ValueExpressionPtr(expression->_rightOperand);

    if (!operands[1]) {
      operands[1] = ValueExpressionPtr(
          new SignedNumericLiteral(expression->_literalNumericalOperand));
    }
  } else if (std::shared_ptr<SubtractionNumericalExpression> expression =
      PARSE(valueExpression, SubtractionNumericalExpression)) {
    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR),
                        std::string(_SUBTRACTION));
    operands[0] = ValueExpressionPtr(expression->_leftOperand);
    operands[1] = ValueExpressionPtr(expression->_rightOperand);
  } else if (std::shared_ptr<ProductNumericalExpression> expression =
      PARSE(valueExpression, ProductNumericalExpression)) {
    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR),
                        std::string(_MULTIPLICATION));
    operands[0] = ValueExpressionPtr(expression->_leftOperand);
    operands[1] = ValueExpressionPtr(expression->_rightOperand);
  } else if (std::shared_ptr<DivisonNumericalExpression> expression =
      PARSE(valueExpression, DivisonNumericalExpression)) {
    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR),
                        std::string(_DIVISION));
    operands[0] = ValueExpressionPtr(expression->_leftOperand);
    operands[1] = ValueExpressionPtr(expression->_rightOperand);
  } else if (std::shared_ptr<ModulusNumericalExpression> expression =
      PARSE(valueExpression, ModulusNumericalExpression)) {
    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR),
                        std::string(_MODULUS));
    operands[0] = ValueExpressionPtr(expression->_leftOperand);
    operands[1] = ValueExpressionPtr(expression->_rightOperand);
  } else if (std::shared_ptr<PowerNumericalExpression> expression =
      PARSE(valueExpression, PowerNumericalExpression)) {
    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR), std::string("pow"));
    operands[0] = ValueExpressionPtr(expression->_leftOperand);
    operands[1] = ValueExpressionPtr(expression->_rightOperand);
  } else if (QueryExpressionPtr expression =
      PARSE(valueExpression, QueryExpression)) {
    if (!ValidateNumericalFunction(expression, inputTAR))
      throw std::runtime_error("Unsupported function: " +
          GET_IDENTIFER_BODY(expression) +
          " with given parameters.");

    operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERATOR),
                        GET_IDENTIFER_BODY(expression));
    operands.clear();

    for (const auto &valueExpression :
        expression->_value_expression_list->ParamsToList()) {
      operands.push_back(valueExpression);
    }
  } else {
    throw std::runtime_error("Invalid arithmetic operation.");
  }

  // Check both side types (numeric or string)?
  // If identifier check if it belongs to tar
  for (int i = 0; i < operands.size(); i++) {
    if (IdentifierChainPtr identifier = PARSE(operands[i], IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
        if (!inputTAR->GetDataElement(GET_IDENTIFER_BODY(identifier))
                     ->IsNumeric()) {
          throw std::runtime_error(
              "Data element: " + GET_IDENTIFER_BODY(identifier) +
                  " is not numeric and cannot be part of numerical expression.");
        }

        operandsType[i] = __IDENTIFIER;
        strVals[i] = identifier->_identifier->_identifierBody;
      } else {
        throw std::runtime_error("Data element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");
      }
    } else if (PARSE(operands[i], SummationNumericExpression) ||
        PARSE(operands[i], SubtractionNumericalExpression) ||
        PARSE(operands[i], ProductNumericalExpression) ||
        PARSE(operands[i], DivisonNumericalExpression) ||
        PARSE(operands[i], ModulusNumericalExpression) ||
        PARSE(operands[i], PowerNumericalExpression)) {
      operandsType[i] = __EXPRESSION;
    } else if (PARSE(operands[i], UnsignedNumericLiteral)) {
      operandsType[i] = __LITERAL;
      numVals[i] = PARSE(operands[i], UnsignedNumericLiteral)->_doubleValue;
    } else if (PARSE(operands[i], SignedNumericLiteral)) {
      operandsType[i] = __LITERAL;
      numVals[i] = PARSE(operands[i], SignedNumericLiteral)->_doubleValue;
    } else if (QueryExpressionPtr expression =
        PARSE(operands[i], QueryExpression)) {
      operandsType[i] = ___FUNCTION;
    } else {
      throw std::runtime_error("Invalid operators for arithmetic operation.");
    }
  }

  TARPtr currentInputTAR = inputTAR;

  for (int i = 0; i < operands.size(); i++) {
    if (operandsType[i] == __LITERAL) {
      // operation->AddParam(OPERAND(i), numVals[i]);
      operation->AddParam(PARAM(TAL_ARITHMETIC, _OPERAND, i), numVals[i]);
    } else if (operandsType[i] == __IDENTIFIER) {
      // operation->AddParam(OPERAND(i), strVals[i]);
      operation->AddIdentifierParam(PARAM(TAL_ARITHMETIC, _OPERAND, i),
                                    strVals[i]);
    } else if (operandsType[i] == __EXPRESSION ||
        operandsType[i] == ___FUNCTION) {
      std::string newMember = DEFAULT_TEMP_MEMBER + std::to_string(idCounter++);
      OperationPtr expression = ParseArithmetic(
          operands[i], currentInputTAR, newMember, queryPlan, idCounter);
      queryPlan->AddOperation(expression, idCounter);
      currentInputTAR = expression->GetResultingTAR();
      // operation->AddParam(OPERAND(i), newMember);
      operation->AddIdentifierParam(PARAM(TAL_ARITHMETIC, _OPERAND, i),
                                    newMember);
    }
  }

  // operation->AddParam(INPUT_TAR, currentInputTAR);
  operation->AddParam(PARAM(TAL_ARITHMETIC, _INPUT_TAR), currentInputTAR);
  // operation->AddParam(NEW_MEMBER, newMember);
  // operation->AddParam( PARAM(TAL_ARITHMETIC, _NEW_MEMBER), newMember);
  operation->AddIdentifierParam(PARAM(TAL_ARITHMETIC, _NEW_MEMBER), newMember);

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseScan(QueryExpressionPtr queryExpressionNode,
                                      QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_SCAN);
  auto params = queryExpressionNode->_value_expression_list->ParamsToList();
  TARPtr inputTAR = ParseTAR(params.front(), scan_error, queryPlan, idCounter);
  operation->AddParam(PARAM(TAL_SCAN, _INPUT_TAR), inputTAR);
  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseSelect(QueryExpressionPtr queryExpressionNode,
                                        QueryPlanPtr queryPlan,
                                        int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_SELECT);
  IdentifierChainPtr identifier; int32_t identifierCount = 0;
  auto params = queryExpressionNode->_value_expression_list->ParamsToList();

  TARPtr inputTAR =
      ParseTAR(params.front(), select_error, queryPlan, idCounter);
  operation->AddParam(PARAM(TAL_SELECT, _INPUT_TAR), inputTAR);
  params.pop_front();

  // Checking following parameters (identifiers)
  for (auto &param : params) {
    if (identifier = PARSE(param, IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
        // operation->AddParam(IDENTIFIER, GET_IDENTIFER_BODY(identifier));
        operation->AddIdentifierParam(PARAM(TAL_SELECT, _IDENTIFIER, identifierCount++),
                                      GET_IDENTIFER_BODY(identifier));

      } else {
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");
      }
    } else {
      throw std::runtime_error(select_error);
    }
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseFilter(QueryExpressionPtr queryExpressionNode,
                                        QueryPlanPtr queryPlan,
                                        int &idCounter) {
#define EXPECTED_FILTER_PARAMS_NUM 2
  OperationPtr operation = std::make_shared<Operation>(TAL_FILTER);
  std::list<ValueExpressionPtr> params;
  params = queryExpressionNode->_value_expression_list->ParamsToList();

  if (params.size() == EXPECTED_FILTER_PARAMS_NUM) {
    TARPtr inputTAR =
        ParseTAR(params.front(), filter_error, queryPlan, idCounter);
    operation->AddParam(PARAM(TAL_FILTER, _INPUT_TAR), inputTAR);

    // Check second parameter (logical expression)
    if (PARSE(params.back(), LogicalConjunction) ||
        PARSE(params.back(), LogicalDisjunction) ||
        PARSE(params.back(), BooleanValueExpression)) {
      OperationPtr logicalOperation =
          ParseLogical(params.back(), inputTAR, queryPlan, idCounter);

      operation->AddParam(PARAM(TAL_FILTER, _AUX_TAR),
                          logicalOperation->GetResultingTAR());
      queryPlan->AddOperation(logicalOperation, idCounter);
    } else if (PARSE(params.back(), ComparisonPredicate)) {
      OperationPtr comparison =
          ParseComparison(params.back(), inputTAR, queryPlan, idCounter);

      operation->AddParam(PARAM(TAL_FILTER, _AUX_TAR),
                          comparison->GetResultingTAR());
      queryPlan->AddOperation(comparison, idCounter);
    } else if (PARSE(params.back(), IdentifierChain)) {
      throw std::runtime_error(
          "Support to boolean attributes not implemented yet.");
    } else if (PARSE(params.back(), QueryExpression)) {
      throw std::runtime_error(
          "Support to boolean functions not implemented yet.");
    } else {
      throw std::runtime_error(filter_error);
    }
  } else {
    throw std::runtime_error(filter_error);
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseSubset(QueryExpressionPtr queryExpressionNode,
                                        QueryPlanPtr queryPlan,
                                        int &idCounter) {
  typedef struct {
    string dimName;
    double bounds[2];
  } Range;
  unordered_map<string, Range> ranges;

  bool hasExplicitDimensions = false;
  int paramCount = 0;
  list<ValueExpressionPtr> params;
  UnsignedNumericLiteralPtr unsignedLiteral;
  SignedNumericLiteralPtr signedLiteral;
  IdentifierChainPtr identifier;
  params = queryExpressionNode->_value_expression_list->ParamsToList();

  // Checking first parameter (tar)
  TARPtr inputTAR =
      ParseTAR(params.front(), subset_error, queryPlan, idCounter);

  for (auto dim : inputTAR->GetDimensions()) {
    if (dim->GetDimensionType() == EXPLICIT)
      hasExplicitDimensions = true;
  }

  params.pop_front();
  if (params.empty() || (params.size() % 3 != 0))
    throw std::runtime_error(subset_error);

  while (!params.empty()) {
    DataElementPtr dataElement;

    Literal logicalIndex;
    logicalIndex.type = DOUBLE;

    Range range;
    DimensionPtr dim;
    auto dimensionNameParam = params.front();
    params.pop_front();

    ValueExpressionPtr boundParams[2];
    boundParams[0] = params.front();
    params.pop_front();
    boundParams[1] = params.front();
    params.pop_front();

    if (identifier = PARSE(dimensionNameParam, IdentifierChain)) {
      string strIdentifier = GET_IDENTIFER_BODY(identifier);
      dataElement = inputTAR->GetDataElement(strIdentifier);

      if (dataElement == nullptr ||
          dataElement->GetType() != DIMENSION_SCHEMA_ELEMENT)
        throw std::runtime_error("Schema element " + strIdentifier +
            " is not a valid dimension.");

      range.dimName = strIdentifier;
      dim = dataElement->GetDimension();
    }

    for (int i = 0; i < 2; i++) {
      if (signedLiteral = PARSE(boundParams[i], SignedNumericLiteral)) {
        range.bounds[i] = signedLiteral->_doubleValue;
      } else if (unsignedLiteral =
                     PARSE(boundParams[i], UnsignedNumericLiteral)) {
        range.bounds[i] = unsignedLiteral->_doubleValue;
      } else {
        throw std::runtime_error(subset_error);
      }

      if (!hasExplicitDimensions &&
          !IN_RANGE(range.bounds[i], dim->GetLowerBound(),
                    dim->GetUpperBound()))
        throw std::runtime_error("Lower and upper bounds for dimension " +
            dim->GetName() + " must be between " +
            to_string(dim->GetLowerBound()) + " and " +
            to_string(dim->GetUpperBound()));
    }

    if (range.bounds[0] > range.bounds[1])
      throw std::runtime_error(
          "Upper bound must be greater than the lower bound.");

    if (ranges.find(range.dimName) != ranges.end())
      throw std::runtime_error("Duplicated range definition for dimension " +
          range.dimName + ".");

    ranges[range.dimName] = range;
  }

  if (hasExplicitDimensions)
    // if(false)
  {
    vector<OperationPtr> comparisonOperations;
    TARPtr andEDFilter = nullptr;

    for (auto entry : ranges) {
      Range range = entry.second;
      OperationPtr compGreaterOrEqual =
        std::make_shared<Operation>(TAL_COMPARISON);

      compGreaterOrEqual->AddParam(PARAM(TAL_COMPARISON, _OPERATOR),
                                   string(_GEQ));
      compGreaterOrEqual->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR), inputTAR);
      compGreaterOrEqual->AddIdentifierParam(PARAM(TAL_COMPARISON, _IDENTIFIER),
                                             range.dimName);
      compGreaterOrEqual->AddParam(PARAM(TAL_COMPARISON, _LITERAL),
                                   range.bounds[0]);
      compGreaterOrEqual->SetResultingTAR(
          _schemaBuilder->InferSchema(compGreaterOrEqual));
      queryPlan->AddOperation(compGreaterOrEqual, idCounter);

      OperationPtr compLessOrEqual =
        std::make_shared<Operation>(TAL_COMPARISON);
      compLessOrEqual->AddParam(PARAM(TAL_COMPARISON, _OPERATOR), string(_LEQ));
      compLessOrEqual->AddParam(PARAM(TAL_COMPARISON, _INPUT_TAR), inputTAR);
      compLessOrEqual->AddIdentifierParam(PARAM(TAL_COMPARISON, _IDENTIFIER),
                                          range.dimName);
      compLessOrEqual->AddParam(PARAM(TAL_COMPARISON, _LITERAL),
                                range.bounds[1]);
      compLessOrEqual->SetResultingTAR(
          _schemaBuilder->InferSchema(compLessOrEqual));
      queryPlan->AddOperation(compLessOrEqual, idCounter);

      comparisonOperations.push_back(compGreaterOrEqual);
      comparisonOperations.push_back(compLessOrEqual);
    }

    andEDFilter = comparisonOperations[0]->GetResultingTAR();
    for (int i = 1; i < comparisonOperations.size(); i++) {
      OperationPtr andOperation = std::make_shared<Operation>(TAL_LOGICAL);
      andOperation->AddParam(PARAM(TAL_LOGICAL, _OPERATOR), string(_AND));
      andOperation->AddParam(PARAM(TAL_LOGICAL, _INPUT_TAR), inputTAR);
      andOperation->AddParam(PARAM(TAL_LOGICAL, _OPERAND, 0), andEDFilter);
      andOperation->AddParam(PARAM(TAL_LOGICAL, _OPERAND, 1),
                             comparisonOperations[i]->GetResultingTAR());
      andOperation->SetResultingTAR(_schemaBuilder->InferSchema(andOperation));
      queryPlan->AddOperation(andOperation, idCounter);
      andEDFilter = andOperation->GetResultingTAR();
    }

    OperationPtr filterOperation = std::make_shared<Operation>(TAL_FILTER);
    filterOperation->AddParam(PARAM(TAL_LOGICAL, _INPUT_TAR), inputTAR);
    filterOperation->AddParam(PARAM(TAL_LOGICAL, _AUX_TAR), andEDFilter);
    filterOperation->SetResultingTAR(
        _schemaBuilder->InferSchema(filterOperation));
    return filterOperation;
  } else // if there are no explicit dimensions, call subset operator with
    // subtars clipping
  {
    OperationPtr operation = std::make_shared<Operation>(TAL_SUBSET);
    operation->AddParam(PARAM(TAL_SUBSET, _INPUT_TAR), inputTAR);

    for (auto entry : ranges) {
      Range range = entry.second;
      operation->AddParam(PARAM(TAL_SUBSET, _DIMENSION, paramCount),
                          range.dimName);
      operation->AddParam(PARAM(TAL_SUBSET, _LOWER_BOUND, paramCount),
                          range.bounds[0]);
      operation->AddParam(PARAM(TAL_SUBSET, _UPPER_BOUND, paramCount),
                          range.bounds[1]);
      paramCount++;
    }

    operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
    return operation;
  }
}

OperationPtr DefaultParser::ParseDerive(QueryExpressionPtr queryExpressionNode,
                                        QueryPlanPtr queryPlan,
                                        int &idCounter) {
#define EXPECTED_DERIVE_PARAMS_NUM 3
  OperationPtr operation;
  std::list<ValueExpressionPtr> params;
  IdentifierChainPtr identifier;
  ValueExpressionPtr valueExpression;

  params = queryExpressionNode->_value_expression_list->ParamsToList();

  if (params.size() == EXPECTED_DERIVE_PARAMS_NUM) {
    TARPtr inputTAR =
        ParseTAR(params.front(), derive_error, queryPlan, idCounter);
    auto paramsFront = params.begin();
    std::advance(paramsFront, 1);

    if (identifier = PARSE(*paramsFront, IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " is already defined.");
      }
    } else {
      throw std::runtime_error(derive_error);
    }

    // Check last parameter numerical expression
    if (valueExpression = PARSE(params.back(), ValueExpression)) {
      operation =
          ParseArithmetic(valueExpression, inputTAR,
                          GET_IDENTIFER_BODY(identifier), queryPlan, idCounter);
    } else {
      throw std::runtime_error(derive_error);
    }
  } else {
    throw std::runtime_error(derive_error);
  }

  return operation;
}

OperationPtr DefaultParser::ParseCross(QueryExpressionPtr queryExpressionNode,
                                       QueryPlanPtr queryPlan, int &idCounter) {
#define EXPECTED_CROSS_PARAMS_NUM 2
  OperationPtr operation = std::make_shared<Operation>(TAL_CROSS);
  list<ValueExpressionPtr> params;
  params = queryExpressionNode->_value_expression_list->ParamsToList();

  if (params.size() == EXPECTED_CROSS_PARAMS_NUM) {
    TARPtr leftTAR =
        ParseTAR(params.front(), cross_error, queryPlan, idCounter);
    TARPtr rightTAR =
        ParseTAR(params.back(), cross_error, queryPlan, idCounter);

    operation->AddParam(PARAM(TAL_CROSS, _OPERAND, 0), leftTAR);
    operation->AddParam(PARAM(TAL_CROSS, _OPERAND, 1), rightTAR);
  } else {
    throw std::runtime_error(cross_error);
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseDimJoin(QueryExpressionPtr queryExpressionNode,
                                         QueryPlanPtr queryPlan,
                                         int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_DIMJOIN);
  IdentifierChainPtr identifier;
  list<ValueExpressionPtr> params;
  int32_t dimsCount = 0;
  params = queryExpressionNode->_value_expression_list->ParamsToList();

  TARPtr leftTAR =
      ParseTAR(params.front(), dimjoin_error, queryPlan, idCounter);
  params.pop_front();
  TARPtr rightTAR =
      ParseTAR(params.front(), dimjoin_error, queryPlan, idCounter);
  params.pop_front();
  operation->AddParam(PARAM(TAL_DIMJOIN, _OPERAND, 0), leftTAR);
  operation->AddParam(PARAM(TAL_DIMJOIN, _OPERAND, 1), rightTAR);

  if (params.empty())
    throw std::runtime_error(dimjoin_error);

  while (!params.empty()) {
    if (identifier = PARSE(params.front(), IdentifierChain)) {
      if (leftTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
        auto dim = leftTAR->GetDataElement(GET_IDENTIFER_BODY(identifier))->GetDimension();
        if(dim == nullptr)
          throw std::runtime_error("Schema element " +
              GET_IDENTIFER_BODY(identifier) +
              " is not a dimension.");

        operation->AddParam(PARAM(TAL_DIMJOIN, _DIMENSION, dimsCount++),
                            GET_IDENTIFER_BODY(identifier));
      } else {
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");
      }
    } else {
      throw std::runtime_error(dimjoin_error);
    }
    params.pop_front();

    if (identifier = PARSE(params.front(), IdentifierChain)) {
      if (rightTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {

        auto dim = rightTAR->GetDataElement(GET_IDENTIFER_BODY(identifier))->GetDimension();
        if(dim == nullptr)
          throw std::runtime_error("Schema element " +
              GET_IDENTIFER_BODY(identifier) +
              " is not a dimension.");

        operation->AddParam(PARAM(TAL_DIMJOIN, _DIMENSION, dimsCount++),
                            GET_IDENTIFER_BODY(identifier));
      } else {
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");
      }
    } else {
      throw std::runtime_error(dimjoin_error);
    }
    params.pop_front();
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseSplit(QueryExpressionPtr queryExpressionNode,
                        QueryPlanPtr queryPlan, int &idCounter){

#define EXPECTED_SPLIT_PARAMS 2

  OperationPtr operation = std::make_shared<Operation>(TAL_SPLIT);
  UnsignedNumericLiteralPtr literal;
  auto params = queryExpressionNode->_value_expression_list->ParamsToList();

  if (params.size() == EXPECTED_SPLIT_PARAMS) {

    TARPtr inputTAR =
      ParseTAR(params.front(), split_error, queryPlan, idCounter);
    operation->AddParam(PARAM(TAL_SPLIT, _INPUT_TAR), inputTAR);
    params.pop_front();


    if(literal = PARSE(params.front(), UnsignedNumericLiteral)){
      int64_t idealSize = (int64_t) literal->_doubleValue;
      operation->AddParam(PARAM(TAL_SPLIT, _OPERAND), idealSize);
    } else {
      throw std::runtime_error(split_error);
    }

  } else {
    throw std::runtime_error(split_error);
  }


  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseReorient(QueryExpressionPtr queryExpressionNode,
                                       QueryPlanPtr queryPlan, int &idCounter){

#define EXPECTED_REORIENT_PARAMS 2

  OperationPtr operation = std::make_shared<Operation>(TAL_REORIENT);
  IdentifierChainPtr identifier;
  auto params = queryExpressionNode->_value_expression_list->ParamsToList();

  if (params.size() == EXPECTED_REORIENT_PARAMS) {

    TARPtr inputTAR =
      ParseTAR(params.front(), split_error, queryPlan, idCounter);
    operation->AddParam(PARAM(TAL_REORIENT, _INPUT_TAR), inputTAR);
    params.pop_front();


    if(identifier = PARSE(params.front(), IdentifierChain)){
      string strIdentifier = GET_IDENTIFER_BODY(identifier);

      if(!inputTAR->HasDataElement(strIdentifier)){
        throw std::runtime_error("Schema element " +
                                   strIdentifier +
                                 " is not a valid member.");
      }

      if(inputTAR->GetDataElement(strIdentifier)->GetDimension() == nullptr){
        throw std::runtime_error("Schema element " +
                                 strIdentifier +
                                 " is not a dimension.");
      }

      operation->AddIdentifierParam(PARAM(TAL_REORIENT, _OPERAND), strIdentifier);

    } else {
      throw std::runtime_error(reorient_error);
    }


  } else {
    throw std::runtime_error(reorient_error);
  }


  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr DefaultParser::ParseEquiJoin(QueryExpressionPtr queryExpressionNode,
                                          QueryPlanPtr queryPlan, int &idCounter) {

}

OperationPtr DefaultParser::ParseSlice(QueryExpressionPtr queryExpressionNode,
                                       QueryPlanPtr queryPlan, int &idCounter) {

}

OperationPtr DefaultParser::ParseAtt2Dim(QueryExpressionPtr queryExpressionNode,
                          QueryPlanPtr queryPlan, int &idCounter) {

}

OperationPtr DefaultParser::ParseUnion(QueryExpressionPtr queryExpressionNode,
                        QueryPlanPtr queryPlan, int &idCounter) {

}

OperationPtr DefaultParser::ParseTranslate(QueryExpressionPtr queryExpressionNode,
                            QueryPlanPtr queryPlan, int &idCounter) {

}

OperationPtr
DefaultParser::ParseAggregate(QueryExpressionPtr queryExpressionNode,
                              QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_AGGREGATE);
  IdentifierChainPtr identifier;
  int32_t opCount = 0, aggr_dim_count = 0;
  list<ValueExpressionPtr> params;
  unordered_map<string, string> dataElementsList;
  params = queryExpressionNode->_value_expression_list->ParamsToList();

  TARPtr inputTAR =
      ParseTAR(params.front(), aggregation_error, std::move(queryPlan), idCounter);
  operation->AddParam(PARAM(TAL_AGGREGATE, _INPUT_TAR), inputTAR);

  params.pop_front();

  for (DataElementPtr de : inputTAR->GetDataElements()) {
    string name = de->GetName();
    dataElementsList[name] = name;
  }

  while (!params.empty()) {

    if (identifier = PARSE(params.front(), IdentifierChain)) {
      auto aggregationFunction = GET_IDENTIFER_BODY(identifier);
      transform(aggregationFunction.begin(), aggregationFunction.end(),
                aggregationFunction.begin(), ::tolower);

      if (!_configurationManager->GetBooleanValue(
          AGGREGATION_FUNCTION(aggregationFunction)) &&
          opCount == 0) {
        throw std::runtime_error("Invalid aggregation function " +
            aggregationFunction + ".");
      } else if (!_configurationManager->GetBooleanValue(
          AGGREGATION_FUNCTION(aggregationFunction))) {
        break;
      }

      // operation->AddParam(OPERAND(opCount++), aggregationFunction);
      operation->AddParam(PARAM(TAL_AGGREGATE, _OPERAND, opCount++),
                          aggregationFunction);

    } else {
      throw std::runtime_error(aggregation_error);
    }
    params.pop_front();

    if (identifier = PARSE(params.front(), IdentifierChain)) {
      if (!inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier)))
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");

      operation->AddParam(PARAM(TAL_AGGREGATE, _OPERAND, opCount++),
                          GET_IDENTIFER_BODY(identifier));
    } else {
      throw std::runtime_error(aggregation_error);
    }
    params.pop_front();

    if (identifier = PARSE(params.front(), IdentifierChain)) {
      if (dataElementsList.find(GET_IDENTIFER_BODY(identifier)) !=
          dataElementsList.end()) {
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " already defined.");
      } else {
        dataElementsList[GET_IDENTIFER_BODY(identifier)] =
            GET_IDENTIFER_BODY(identifier);
      }

      // operation->AddParam(OPERAND(opCount++),
      // GET_IDENTIFER_BODY(identifier));
      operation->AddParam(PARAM(TAL_AGGREGATE, _OPERAND, opCount++),
                          GET_IDENTIFER_BODY(identifier));

    } else {
      throw std::runtime_error(aggregation_error);
    }
    params.pop_front();
  }

  for (const auto &dataElement : params) {
    if (identifier = PARSE(dataElement, IdentifierChain)) {
      if (inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
        // operation->AddParam(DIM(aggr_dim_count++),
        //                    GET_IDENTIFER_BODY(identifier));

        auto aggregateDimension =
            inputTAR->GetDataElement(GET_IDENTIFER_BODY(identifier))
                    ->GetDimension();

        if (aggregateDimension == nullptr)
          throw std::runtime_error("Schema element " +
              GET_IDENTIFER_BODY(identifier) +
              " is not a dimension.");

        operation->AddParam(PARAM(TAL_AGGREGATE, _DIMENSION, aggr_dim_count++),
                            GET_IDENTIFER_BODY(identifier));
      } else {
        throw std::runtime_error("Schema element " +
            GET_IDENTIFER_BODY(identifier) +
            " is not a valid member.");
      }
    } else {
      throw std::runtime_error(aggregation_error);
    }
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

OperationPtr
DefaultParser::ParseUserDefined(QueryExpressionPtr queryExpressionNode,
                                QueryPlanPtr queryPlan, int &idCounter) {
  OperationPtr operation = std::make_shared<Operation>(TAL_USER_DEFINED);
  TARPtr inputTAR;
  list<ValueExpressionPtr> params;
  IdentifierChainPtr identifier;
  QueryExpressionPtr queryExpression;
  CharacterStringLiteralPtr stringLiteral;
  UnsignedNumericLiteralPtr unsignedNumericLiteral;
  SignedNumericLiteralPtr signedNumericLiteral;
  int32_t op_count = 0;

  params = queryExpressionNode->_value_expression_list->ParamsToList();
  operation->AddParam(OPERATOR_NAME,
                      queryExpressionNode->_identifier->_identifierBody);

  for (const auto &param : params) {
    // Check first parameter (tar)
    if (identifier = PARSE(param, IdentifierChain)) {
      inputTAR = _metadaManager->GetTARByName(_currentTARS,
                                              GET_IDENTIFER_BODY(identifier));

      if (inputTAR != nullptr)
        operation->AddParam(OPERAND(op_count++), inputTAR);
      else
        operation->AddParam(OPERAND(op_count++),
                            GET_IDENTIFER_BODY(identifier));

    } else if (queryExpression = PARSE(param, QueryExpression)) {
      inputTAR = ParseOperation(queryExpression, queryPlan, idCounter);
      if (inputTAR)
        operation->AddParam(OPERAND(op_count++), inputTAR);
    } else if (stringLiteral = PARSE(param, CharacterStringLiteral)) {
      operation->AddParam(OPERAND(op_count++), stringLiteral->_literalString);
    } else if (unsignedNumericLiteral = PARSE(param, UnsignedNumericLiteral)) {
      operation->AddParam(OPERAND(op_count++),
                          unsignedNumericLiteral->_doubleValue);
    } else if (signedNumericLiteral = PARSE(param, SignedNumericLiteral)) {
      operation->AddParam(OPERAND(op_count++),
                          signedNumericLiteral->_doubleValue);
    }
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;
}

//Predict Operator
OperationPtr
DefaultParser::ParsePredict(QueryExpressionPtr queryExpressionNode,
                                QueryPlanPtr queryPlan, int &idCounter) {
#define EXPECTED_PREDICT_PARAMS_NUM 3
  CharacterStringLiteralPtr stringLiteral;
  OperationPtr operation = std::make_shared<Operation>(TAL_PREDICT);
  IdentifierChainPtr identifier; int32_t identifierCount = 0;
  auto params = queryExpressionNode->_value_expression_list->ParamsToList();
  int32_t op_count = 1;

  if (params.size() == EXPECTED_PREDICT_PARAMS_NUM) {
    TARPtr inputTAR =
        ParseTAR(params.front(), predict_error, queryPlan, idCounter);
    operation->AddParam(PARAM(TAL_PREDICT, _INPUT_TAR), inputTAR);
    params.pop_front();

    auto param = params.begin();

    //First Parameter
    if(identifier = PARSE(*param, IdentifierChain)) {
        string modelName = identifier->getIdentifier()->_identifierBody;
        ifstream f("/home/anderson/Programacao/Savime/Savime/etc/modelscfg/" + modelName);
        if(f.good()){
            operation->AddParam("model_name", modelName);
            f.close();
        }
        else {
            throw std::runtime_error("Element " +
                modelName + " is not a registered model name.");
        }
    } else {
        throw std::runtime_error(predict_error);
    }
    param++;
    //Second Parameter
    if (identifier = PARSE(*param, IdentifierChain)) {
        if (!inputTAR->HasDataElement(GET_IDENTIFER_BODY(identifier))) {
            throw std::runtime_error("Schema element " +
                GET_IDENTIFER_BODY(identifier) +
                " is not a valid attribute in TAR " + inputTAR->GetName() + ".");
        }

    } else {
        throw std::runtime_error(predict_error);
    }
    operation->AddParam("attribute", GET_IDENTIFER_BODY(identifier));

  }
  else {
    throw std::runtime_error(predict_error);
  }

  operation->SetResultingTAR(_schemaBuilder->InferSchema(operation));
  return operation;

}

OperationPtr
DefaultParser::ParseRegisterModel(QueryExpressionPtr queryExpressionNode,
                                  QueryPlanPtr queryPlan, int &idCounter) {
  RegisterModelParser *registerModelParser = new RegisterModelParser(this);
  OperationPtr operation = registerModelParser->parse(queryExpressionNode, queryPlan, idCounter);
  delete(registerModelParser);
  return operation;
}

// MAIN FUNCTIONS
OperationPtr
DefaultParser::ParseDMLOperation(QueryExpressionPtr queryExpressionNode,
                                 QueryPlanPtr queryPlan, int &idCounter) {
  std::string functionName = queryExpressionNode->_identifier->_identifierBody;
  std::transform(functionName.begin(), functionName.end(), functionName.begin(),
                 ::tolower);

  OperationPtr operation;

  if (functionName == _CREATE_TARS) {
    operation = ParseCreateTARS(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _CREATE_TAR) {
    operation = ParseCreateTAR(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _CREATE_TYPE) {
    operation = ParseCreateType(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _CREATE_DATASET) {
    operation = ParseCreateDataset(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _LOAD_SUBTAR) {
    operation = ParseLoad(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _DELETE) {
    operation = ParseDelete(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _DROP_TARS) {
    operation = ParseDropTARS(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _DROP_TAR) {
    operation = ParseDropTAR(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _DROP_TYPE) {
    operation = ParseDropType(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _DROP_DATASET) {
    operation = ParseDropDataset(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _SAVE) {
    operation = ParseSave(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _SHOW) {
    operation = ParseShow(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _BATCH) {
    operation = ParseBatch(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == _REGISTER_MODEL) {
    operation = ParseRegisterModel(queryExpressionNode, queryPlan, idCounter);
  } else {
    throw std::runtime_error("Unknown or invalid operator: " + functionName + ".");
  }

  queryPlan->AddOperation(operation, idCounter);
  return operation;
}

TARPtr DefaultParser::ParseOperation(QueryExpressionPtr queryExpressionNode,
                                     QueryPlanPtr queryPlan, int &idCounter) {
  string functionName = queryExpressionNode->_identifier->_identifierBody;
  std::transform(functionName.begin(), functionName.end(), functionName.begin(),
                 ::tolower);

  OperationPtr operation;

  if (functionName == _SELECT) {
    operation = ParseSelect(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_WHERE)) {
    operation = ParseFilter(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_DERIVE)) {
    operation = ParseDerive(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_SCAN)) {
    operation = ParseScan(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_SUBSET)) {
    operation = ParseSubset(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_CROSS_PRODUCT)) {
    operation = ParseCross(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_EQUIJOIN)) {
    operation = ParseEquiJoin(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_DIMJOIN)) {
    operation = ParseDimJoin(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_ATT2DIM)) {
    operation = ParseAtt2Dim(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_AGGREGATE)) {
    operation = ParseAggregate(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_SLICE)) {
    operation = ParseSlice(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_UNION)) {
    operation = ParseUnion(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_TRANSLATE)) {
    operation = ParseTranslate(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_SPLIT)) {
    operation = ParseSplit(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_REORIENT)) {
    operation = ParseReorient(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_PREDICT)) {
      operation = ParsePredict(queryExpressionNode, queryPlan, idCounter);
  } else if (functionName == (_REGISTER_MODEL)) {
    operation = ParseRegisterModel(queryExpressionNode, queryPlan, idCounter);
  } else if (_configurationManager->GetBooleanValue(
      OPERATOR(functionName))) {
    operation = ParseUserDefined(queryExpressionNode, queryPlan, idCounter);
  } else {
    throw std::runtime_error("Unknown or invalid operator: " + functionName +
        ".");
  }

  queryPlan->AddOperation(operation, idCounter);
  return operation->GetResultingTAR();
}

int DefaultParser::CreateQueryPlan(ParseTreeNodePtr root,
                                   QueryDataManagerPtr queryDataManager) {
  try {
    int idCounter = 1;
    QueryPlanPtr queryPlan = std::make_shared<QueryPlan>();
    QueryExpressionPtr queryExpression = PARSE(root, QueryExpression);

    if (queryExpression) {
      std::string functionName = queryExpression->_identifier->_identifierBody;
      std::transform(functionName.begin(), functionName.end(),
                     functionName.begin(), ::tolower);

      if (!functionName.compare(0, 6, "create") ||
          !functionName.compare(0, 4, "drop") ||
          !functionName.compare(0, 4, "load") ||
          !functionName.compare(0, 4, "show") ||
          !functionName.compare(0, 4, "save") ||
          !functionName.compare(0, 6, "delete") ||
          !functionName.compare(0, 7, "restore") ||
          !functionName.compare(0, 5, "batch") ||
          !functionName.compare(0, 8, "register")) {
        ParseDMLOperation(queryExpression, queryPlan, idCounter);
        queryPlan->SetType(DDL);
      } else {
        ParseOperation(queryExpression, queryPlan, idCounter);
        queryPlan->SetType(DML);
      }

      queryDataManager->SetQueryPlan(queryPlan);
    } else {
      throw std::runtime_error("Invalid top level element in AST.");
    }

    return SAVIME_SUCCESS;
  } catch (std::exception &e) {
    queryDataManager->SetErrorResponseText(e.what());
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }
}

TARPtr DefaultParser::InferOutputTARSchema(OperationPtr operation) {
  return _schemaBuilder->InferSchema(operation);
}

SavimeResult DefaultParser::Parse(QueryDataManagerPtr queryDataManager) {
  ParseTreeNodePtr node;
  void *scanner, *ret;
  int error;
  if (_schemaBuilder == nullptr) {
    _schemaBuilder = std::make_shared<SchemaBuilder>(_configurationManager,
      _metadaManager, _storageManager);
  }

  try {
    SavimeTime t1, t2;
    GET_T1_LOCAL();

    string queryText = queryDataManager->GetQueryText();
    queryText.erase(std::remove(queryText.begin(), queryText.end(), '\n'),
                    queryText.end());

    _systemLogger->LogEvent(_moduleName,
                            "Parsing query  " +
                                std::to_string(queryDataManager->GetQueryId()) +
                                ": " + queryText + ".");

    _currentTARS = _metadaManager->GetTARS(
        _configurationManager->GetIntValue(DEFAULT_TARS));

    // Initialize parser
    yylex_init(&scanner);
    yy_scan_string(queryDataManager->GetQueryText().c_str(), scanner);
    error = yyparse(scanner, &ret);

    if (error != 0) {
      queryDataManager->SetErrorResponseText((char *)ret);
      throw std::runtime_error("Error during query parse: " +
          std::string((char *)ret));
    } else {
      // Convert ret to QueryExpression shared_ptr pointer
      auto *queryExpression = (QueryExpressionPtr *)ret;
      node = PARSE(*queryExpression, ParseTreeNode);

      // Removing the node dynamically created during yyparse
      delete queryExpression;
    }

    if (CreateQueryPlan(node, queryDataManager) != SAVIME_SUCCESS) {
      throw std::runtime_error("Error while creating query plan.");
    }

#ifdef DEBUG
    // node->printTreeNode(1);
    if(queryDataManager->GetQueryPlan() != nullptr) {
      auto queryplanStr = queryDataManager->GetQueryPlan()->toString();
      _systemLogger->LogEvent(_moduleName, "Query Plan:\n" + queryplanStr);
    }
#endif

    yylex_destroy(scanner);

    GET_T2_LOCAL();
    _systemLogger->LogEvent(
        _moduleName, "Parsing time: " + std::to_string(GET_DURATION()) + " ms");
  } catch (std::exception &e) {
    yylex_destroy(scanner);
    _systemLogger->LogEvent(this->_moduleName, e.what());
    return SAVIME_FAILURE;
  }

  return SAVIME_SUCCESS;
}