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

// Parameter class member functions
Parameter::Parameter(std::string paramName, TARPtr param) {
  name = paramName;
  tar = param;
  type = TAR_PARAM;
}

Parameter::Parameter(std::string paramName, float param) {
  name = paramName;
  literal_flt = param;
  type = LITERAL_FLOAT_PARAM;
}

Parameter::Parameter(std::string paramName, double param) {
  name = paramName;
  literal_dbl = param;
  type = LITERAL_DOUBLE_PARAM;
}

Parameter::Parameter(std::string paramName, int32_t param) {
  name = paramName;
  literal_int = param;
  type = LITERAL_INT_PARAM;
}

Parameter::Parameter(std::string paramName, int64_t param) {
  name = paramName;
  literal_lng = param;
  type = LITERAL_LONG_PARAM;
}

Parameter::Parameter(std::string paramName, bool param) {
  name = paramName;
  literal_bool = param;
  type = LITERAL_BOOLEAN_PARAM;
}

Parameter::Parameter(std::string paramName, std::string param) {
  name = paramName;
  literal_str = param;
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
  case TAL_SPLIT:
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
  case LITERAL_BOOLEAN_PARAM:
    return "";
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
  }
}

std::list<ParameterPtr> Operation::GetParameters() { return _parameters; }

ParameterPtr Operation::GetParametersByName(std::string name) {
  for (auto &param : _parameters) {
    if (!param->name.compare(name))
      return param;
  }

  return NULL;
}

void Operation::SetOperation(OperationCode type) { _type = type; }

OperationCode Operation::GetOperation() { return _type; }

void Operation::SetResultingTAR(TARPtr tar) { _resultingTAR = tar; }

TARPtr Operation::GetResultingTAR() { return _resultingTAR; }

void Operation::AddParam(std::string paramName, TARPtr paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, float paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, double paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, int32_t paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, int64_t paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, bool paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::AddParam(std::string paramName, std::string paramData) {
  ParameterPtr parameter = ParameterPtr(new Parameter(paramName, paramData));
  _parameters.push_back(parameter);
}

void Operation::SetName(std::string name) { _name = name; }

std::string Operation::GetName() { return _name; }

std::string Operation::toString() {
  std::string str;
  if (_resultingTAR)
    str = _resultingTAR->GetName() + " = ";
  str = str + OpToString(_type) + " (";

  for (auto &param : _parameters) {
    if (_parameters.front() == param)
      str = str + ParamToString(param);
    else
      str = str + ", " + ParamToString(param);
  }

  return str + ")";
}

Operation::~Operation() {}

// Query plan member functions
void QueryPlan::AddOperation(OperationPtr operation) {
  _operations.push_back(operation);
}

void QueryPlan::AddOperation(OperationPtr operation, int &idCounter) {
  _operations.push_back(operation);

  if (operation->GetResultingTAR() != NULL) {
    operation->GetResultingTAR()->AlterTAR(
        UNSAVED_ID, "TAR_" + std::to_string(idCounter));
    idCounter++;
  }
}

std::list<OperationPtr> &QueryPlan::GetOperations() { return _operations; }

void QueryPlan::SetType(QueryType type) { _type = type; }

QueryType QueryPlan::GetType() { return _type; }

QueryPlan::~QueryPlan() {}
