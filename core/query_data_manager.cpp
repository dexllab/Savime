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
#include "include/query_data_manager.h"

string PARAM(OperationCode operation, TALParameter parameterType, int32_t index) {
#define __PARAM_NO_INDEX -1
#define __BASE_PARAMETER "base"

  if(parameterType == _INPUT_TAR && index == __PARAM_NO_INDEX)
    return INPUT_TAR;

  if(parameterType == _INPUT_TAR)
    return INPUT_TAR+std::to_string(index);

  if(parameterType == _OPERAND)
    return OPERAND(index);

  if(parameterType == _OPERATOR)
    return OP;

  if(parameterType == _LOWER_BOUND)
    return LB(index);

  if(parameterType == _UPPER_BOUND)
    return UP(index);

  if(parameterType == _DIMENSION)
    return DIM(index);

  if(parameterType == _IDENTIFIER  && index == __PARAM_NO_INDEX)
    return IDENTIFIER;

  if(parameterType == _IDENTIFIER)
    return IDENTIFIER+std::to_string(index);

  if(parameterType == _LITERAL  && index == __PARAM_NO_INDEX)
    return LITERAL;

  if(parameterType == _LITERAL)
    return LITERAL+std::to_string(index);

  if(parameterType == _COMMAND && index == __PARAM_NO_INDEX)
    return COMMAND;

  if(parameterType == _COMMAND)
    return COMMAND+std::to_string(index);

  if(parameterType ==  _AUX_TAR && index == __PARAM_NO_INDEX)
    return AUX_TAR;

  if(parameterType ==  _AUX_TAR)
    return AUX_TAR+std::to_string(index);

  if(parameterType ==  _NEW_MEMBER)
    return NEW_MEMBER;

  if(parameterType == _EMPTY_SUBSET)
    return EMPTY_SUBSET;

  if(parameterType == _NO_INTERSECTION_JOIN)
    return NO_INTERSECTION_JOIN;

  return __BASE_PARAMETER;
}

bool compare_tar_name(const OperationPtr& first, const OperationPtr& second)
{
  auto tar1 = first->GetResultingTAR();
  auto tar2 = second->GetResultingTAR();

  if(tar1 == nullptr && tar2 == nullptr)
    return true;

  if(tar1 == nullptr)
    return false;

  if(tar2 == nullptr)
    return true;

  auto pos1 = strtoll(split(tar1->GetName(), '_')[1].c_str(), nullptr, 10);
  auto pos2 = strtoll(split(tar2->GetName(), '_')[1].c_str(), nullptr, 10);

  return pos1 < pos2;
}


// Parameter class member functions
Parameter::Parameter(std::string paramName, TARPtr param) {
  name = std::move(paramName);
  tar = std::move(param);
  type = TAR_PARAM;
}

Parameter::Parameter(string paramName, string param, bool isIdentifier) {
  name = std::move(paramName);
  literal_str = std::move(param);

  if(isIdentifier) {
    type = IDENTIFIER_PARAM;
  } else {
    type = LITERAL_STRING_PARAM;
  }
}

Parameter::Parameter(std::string paramName, float param) {
  name = std::move(paramName);
  literal_flt = param;
  type = LITERAL_FLOAT_PARAM;
}

Parameter::Parameter(std::string paramName, double param) {
  name = std::move(paramName);
  literal_dbl = param;
  type = LITERAL_DOUBLE_PARAM;
}

Parameter::Parameter(std::string paramName, int32_t param) {
  name = std::move(paramName);
  literal_int = param;
  type = LITERAL_INT_PARAM;
}

Parameter::Parameter(std::string paramName, int64_t param) {
  name = std::move(paramName);
  literal_lng = param;
  type = LITERAL_LONG_PARAM;
}

Parameter::Parameter(std::string paramName, bool param) {
  name = std::move(paramName);
  literal_bool = param;
  type = LITERAL_BOOLEAN_PARAM;
}

Parameter::Parameter(std::string paramName, std::string param) {
  name = std::move(paramName);
  literal_str = std::move(param);
  type = LITERAL_STRING_PARAM;
}

// Operation class member functions
Operation::Operation(OperationCode type) { _type = type; }

std::string Operation::OpToString(OperationCode op) {
  switch (op) {
  case TAL_CREATE_TARS:
    return std::string("CREATE_TARS");
  case TAL_CREATE_TAR:
    return std::string("CREATE_TAR");
  case TAL_CREATE_TYPE:
    return std::string("CREATE_TYPE");
  case TAL_CREATE_DATASET:
    return std::string("CREATE_DATASET");
  case TAL_LOAD_SUBTAR:
    return std::string("LOAD_SUBTAR");
  case TAL_DROP_TARS:
    return std::string("DROP_TARS");
  case TAL_DROP_TAR:
    return std::string("DROP_TAR");
  case TAL_DROP_TYPE:
    return std::string("DROP_TYPE");
  case TAL_DROP_DATASET:
    return std::string("DROP_DATASET");
  case TAL_SAVE:
    return std::string("SAVE");
  case TAL_SHOW:
    return std::string("SHOW");
  case TAL_SCAN:
    return std::string("SCAN");
  case TAL_SELECT:
    return std::string("SELECT");
  case TAL_SUBSET:
    return std::string("SUBSET");
  case TAL_FILTER:
    return std::string("FILTER");
  case TAL_LOGICAL:
    return std::string("LOGICAL");
  case TAL_COMPARISON:
    return std::string("COMPARISON");
  case TAL_CROSS:
    return std::string("CROSS");
  case TAL_EQUIJOIN:
    return std::string("EQUIJOIN");
  case TAL_DIMJOIN:
    return std::string("DIMJOIN");
  case TAL_ARITHMETIC:
    return std::string("DERIVE");
  case TAL_AGGREGATE:
    return std::string("AGGREGATE");
  case TAL_UNION:
    return std::string("SPLIT");
  case TAL_USER_DEFINED:
    return std::string("USER");   
  default:
    return std::string("HAL");
  }
}

std::string Operation::ParamToString(ParameterPtr param) {
  switch (param->type) {
  case TAR_PARAM:
    return param->tar->GetName();
  case IDENTIFIER_PARAM:
    return param->literal_str;
  case LITERAL_BOOLEAN_PARAM:
    return std::to_string(param->literal_bool);
  case LITERAL_DOUBLE_PARAM:
    return std::to_string(param->literal_dbl);
  case LITERAL_FLOAT_PARAM:
    return std::to_string(param->literal_flt);
  case LITERAL_INT_PARAM:
    return std::to_string(param->literal_int);
  case LITERAL_LONG_PARAM:
    return std::to_string(param->literal_lng);
  case LITERAL_STRING_PARAM:
    return param->literal_str;
  default:
    return "";
  }
}

std::list<ParameterPtr>& Operation::GetParameters() { return _parameters; }

ParameterPtr Operation::GetParametersByName(std::string name) {
  for (auto &param : _parameters) {
    if (param->name == name)
      return param;
  }

  return nullptr;
}

void Operation::SetOperation(OperationCode type) { _type = type; }

OperationCode Operation::GetOperation() { return _type; }

void Operation::SetResultingTAR(TARPtr tar) { _resultingTAR = std::move(tar); }

TARPtr Operation::GetResultingTAR() { return _resultingTAR; }

void Operation::AddIdentifierParam(string name, string identifier) {
  ParameterPtr parameter = std::make_shared<Parameter>(name, identifier, true);
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, TARPtr paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
  if(paramData == nullptr){
    int a = 2;
    double b = a*100;
  }
}

void Operation::AddParam(std::string paramName, float paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, double paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, int32_t paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, int64_t paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, bool paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, std::string paramData) {
  ParameterPtr parameter = std::make_shared<Parameter>(paramName, paramData);
  _parameters.push_back(parameter);
}

void Operation::AddParam(ParameterPtr parameter) {
  switch(parameter->type){
    case TAR_PARAM: AddParam(parameter->name, parameter->tar); break;
    case IDENTIFIER_PARAM: AddIdentifierParam(parameter->name, parameter->literal_str); break;
    case LITERAL_FLOAT_PARAM: AddParam(parameter->name, parameter->literal_flt); break;
    case LITERAL_DOUBLE_PARAM: AddParam(parameter->name, parameter->literal_dbl); break;
    case LITERAL_INT_PARAM: AddParam(parameter->name, parameter->literal_int); break;
    case LITERAL_LONG_PARAM: AddParam(parameter->name, parameter->literal_lng); break;
    case LITERAL_STRING_PARAM: AddParam(parameter->name, parameter->literal_str); break;
    case LITERAL_BOOLEAN_PARAM: AddParam(parameter->name, parameter->literal_bool); break;
  }
}

void Operation::SetName(std::string name) { _name = std::move(name); }

std::string Operation::GetName() { return _name; }

std::string Operation::toString() {
  std::string str;
  if (_resultingTAR)
    str = _resultingTAR->toString() + " = ";
  str = str + OpToString(_type) + " (";

  for (auto &param : _parameters) {
    if (_parameters.front() == param)
      str += ParamToString(param);
    else
      str +=  ", " + ParamToString(param);
  }

  return str + ")";
}

//Operation::~Operation() = default;

// Query plan member functions
void QueryPlan::AddOperation(OperationPtr operation) {
  _operations.push_back(operation);
}

void QueryPlan::AddOperation(OperationPtr operation, int &idCounter) {
  _operations.push_back(operation);

  if (operation->GetResultingTAR() != nullptr) {
    operation->GetResultingTAR()->AlterTAR(
        UNSAVED_ID, "TAR_" + std::to_string(idCounter));
    idCounter++;
  }
}

std::list<OperationPtr> &QueryPlan::GetOperations() { return _operations; }

void QueryPlan::SetType(QueryType type) { _type = type; }

QueryType QueryPlan::GetType() { return _type; }


void QueryPlan::SortOperations() {
  _operations.sort(compare_tar_name);
}

string QueryPlan::toString() {
  string queryplanStr;
  for (const auto &op : _operations) {
    if (!queryplanStr.empty())
      queryplanStr += _NEWLINE;
    queryplanStr += op->toString();
  }
  return queryplanStr;
}

QueryPlan::~QueryPlan() = default;


