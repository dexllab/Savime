#include <memory>

#include <memory>

#include <memory>

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
#include <cassert>
#include <math.h>
#include "schema_builder.h"
#include "default_parser.h"

void SchemaBuilder::SetResultingType(TARPtr inputTAR, TARPtr resultingTAR) {
  bool resultMaintainsType = true;
  TypePtr type = inputTAR->GetType();

  if (type) {
    for (auto roleImplementation : inputTAR->GetRoles()) {
      DataElementPtr de =
          resultingTAR->GetDataElement(roleImplementation.first);

      // if data element doesn't exist
      if (!de) {
        // check if it was implementing a mandatory role
        for (auto entry : type->roles) {
          auto role = entry.second;

          // if the role implementeting for the data element is found
          if (role->name == (roleImplementation.second->name)) {
            // if is mandatory, then resulting TAR can't be of the same type
            if (role->is_mandatory) {
              resultMaintainsType = false;
              break;
            }
          }
        }
      }

      if (!resultMaintainsType)
        break;
    }

    // If no mandatory data element is missing
    if (resultMaintainsType) {
      // Resulting TAR receives inputTAR type
      resultingTAR->AlterType(type);

      // Implementing roles
      for (auto roleImplementation : inputTAR->GetRoles()) {
        DataElementPtr de =
            resultingTAR->GetDataElement(roleImplementation.first);

        // If data element is present, then it still implements the role
        if (de) {
          resultingTAR->GetRoles()[roleImplementation.first] =
              roleImplementation.second;
        }
      }
    }
  }
}

TARPtr SchemaBuilder::InferSchemaForScanOp(OperationPtr operation) {
  ParameterPtr inputTARParam = operation->GetParameters().front();
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
  SetResultingType(inputTARParam->tar, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForSelectOp(OperationPtr operation) {
  TARPtr resultingTAR = std::make_shared<TAR>(0, "", nullptr);
  TARPtr inputTAR = operation->GetParametersByName(PARAM(TAL_SELECT, _INPUT_TAR))->tar;
  std::map<std::string, DataElementPtr> inputDimensions;
  double totalTARSize = 1;

  // Adding input dimensions into a map
  for (auto &dataElement : inputTAR->GetDataElements()) {
    if (dataElement->GetType() == DIMENSION_SCHEMA_ELEMENT) {
      inputDimensions[dataElement->GetName()] = dataElement;
      totalTARSize *= dataElement->GetDimension()->GetCurrentLength();
    }
  }

  // Checking if all dimensions are specified as parameters for select
  for (auto &param : operation->GetParameters()) {
    if (param->type == IDENTIFIER_PARAM) {
      if (inputDimensions.find(param->literal_str) != inputDimensions.end()) {
        inputDimensions.erase(param->literal_str);
      }
    }
  }

  // if no elements have been selected, add all of them.
  if (operation->GetParameters().size() == 1) {
    resultingTAR = inputTAR->Clone(false, false, false);
  }
    // If map is empty, then all dimensions are presented in the parameters
  else if (inputDimensions.empty()) {
    for (auto &param : operation->GetParameters()) {
      if (param->type == IDENTIFIER_PARAM) {
        DataElementPtr dataElement =
            inputTAR->GetDataElement(param->literal_str);

        switch (dataElement->GetType()) {
        case DIMENSION_SCHEMA_ELEMENT:
          resultingTAR->AddDimension(dataElement->GetDimension());
          break;
        case ATTRIBUTE_SCHEMA_ELEMENT:
          resultingTAR->AddAttribute(dataElement->GetAttribute());
          break;
        }
      }
    }
  } else // Otherwise, dimensions have been dropped, they all become variables
    // plus a synthetic dim
  {
    DataType type(INT64, 1);
    resultingTAR->AddDimension(DEFAULT_SYNTHETIC_DIMENSION, type, 1,
                               totalTARSize, totalTARSize - 1);

    for (auto &param : operation->GetParameters()) {
      if (param->type == IDENTIFIER_PARAM) {
        DataElementPtr dataElement =
            inputTAR->GetDataElement(param->literal_str);

        //if the ID dim has been projected, overwrite it
        if(dataElement->GetName() == DEFAULT_SYNTHETIC_DIMENSION)
          continue;

        switch (dataElement->GetType()) {
        case DIMENSION_SCHEMA_ELEMENT:
          resultingTAR->AddAttribute(dataElement->GetDimension()->GetName(),
                                     dataElement->GetDimension()->GetType());
          break;
        case ATTRIBUTE_SCHEMA_ELEMENT:
          resultingTAR->AddAttribute(dataElement->GetAttribute()->GetName(),
                                     dataElement->GetAttribute()->GetType());
          break;
        }
      }
    }
  }

  SetResultingType(inputTAR, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForFilterOp(OperationPtr operation) {
  ParameterPtr inputTARParam = operation->GetParameters().front();
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
  SetResultingType(inputTARParam->tar, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForSubsetOp(OperationPtr operation) {
  ParameterPtr inputTARParam = operation->GetParameters().front();
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
  int64_t numberOfDimensions = (operation->GetParameters().size() - 1) / 3;

  for (int64_t i = 0; i < numberOfDimensions; i++) {
    auto param =
        operation->GetParametersByName(PARAM(TAL_SUBSET, _DIMENSION, i));
    auto dataElement = resultingTAR->GetDataElement(param->literal_str);
    auto dim = dataElement->GetDimension();
    double newLowerBound, newUpperBound;
    double lb =
        operation->GetParametersByName(PARAM(TAL_SUBSET, _LOWER_BOUND, i))
                 ->literal_dbl;
    double ub =
        operation->GetParametersByName(PARAM(TAL_SUBSET, _UPPER_BOUND, i))
                 ->literal_dbl;
    double preamble = lb - dim->GetLowerBound();
    double rem = fmod(preamble, dim->GetSpacing());

    if (preamble > 0.0 && rem != 0)
      newLowerBound =
          ((preamble - rem) + dim->GetSpacing()) + dim->GetLowerBound();
    else
      newLowerBound = lb;

    preamble = ub - dim->GetLowerBound();
    rem = fmod(preamble, dim->GetSpacing());
    if (preamble > 0.0 && rem != 0)
      newUpperBound = (preamble - rem) + dim->GetLowerBound();
    else
      newUpperBound = ub;

    if (newUpperBound >= newLowerBound) {
      DimensionPtr newDim = make_shared<Dimension>(
          UNSAVED_ID, dim->GetName(), dim->GetType(), newLowerBound,
          newUpperBound, dim->GetSpacing());
      newDim->CurrentUpperBound() = dim->CurrentUpperBound();

      if (newDim->CurrentUpperBound() > newDim->GetRealUpperBound()) {
        newDim->CurrentUpperBound() = newDim->GetRealUpperBound();
      }

      resultingTAR->RemoveDataElement(dataElement->GetName());
      resultingTAR->AddDimension(newDim);
    } else {
      operation->AddParam(PARAM(TAL_SUBSET, _EMPTY_SUBSET), true);
    }
  }

  SetResultingType(inputTARParam->tar, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForLogicalOp(OperationPtr operation) {
  ParameterPtr inputTARParam =
      operation->GetParametersByName(PARAM(TAL_LOGICAL, _INPUT_TAR));
  assert(inputTARParam);
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, true);
  assert(inputTARParam);
  resultingTAR->AddAttribute(DEFAULT_MASK_ATTRIBUTE,
                             DataType(SUBTAR_POSITION, 1));
  resultingTAR->AddAttribute(DEFAULT_OFFSET_ATTRIBUTE, DataType(REAL_INDEX, 1));
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForComparisonOp(OperationPtr operation) {
  ParameterPtr inputTARParam =
      operation->GetParametersByName(PARAM(TAL_COMPARISON, _INPUT_TAR));
  assert(inputTARParam);
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, true);
  assert(inputTARParam);
  resultingTAR->AddAttribute(DEFAULT_MASK_ATTRIBUTE,
                             DataType(SUBTAR_POSITION, 1));
  resultingTAR->AddAttribute(DEFAULT_OFFSET_ATTRIBUTE, DataType(REAL_INDEX, 1));
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForArithmeticOp(OperationPtr operation) {
  DataType newMemberType;

  ParameterPtr inputTARParam =
      operation->GetParametersByName(PARAM(TAL_ARITHMETIC, _INPUT_TAR));
  assert(inputTARParam);
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
  assert(inputTARParam);

  const char *op =
      operation->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERATOR))
               ->literal_str.c_str();
  ParameterPtr operand0 =
      operation->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERAND, 0));
  ParameterPtr operand1 =
      operation->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERAND, 1));
  DataType t1, t2;

  if (operand1 == nullptr) {
    operand1 = std::make_shared<Parameter>("dummy", (double)0);
  }

  if (operand0->type == LITERAL_DOUBLE_PARAM) {
    Literal literal;
    literal.Simplify(operand0->literal_dbl);
    t1 = literal.type;
  } else if (operand0->type == IDENTIFIER_PARAM) {
    t1 = inputTARParam->tar->GetDataElement(operand0->literal_str)
                      ->GetDataType();
  }

  if (operand1->type == LITERAL_DOUBLE_PARAM) {
    Literal literal;
    literal.Simplify(operand1->literal_dbl);
    t2 = literal.type;
  } else if (operand1->type == IDENTIFIER_PARAM) {
    t2 = inputTARParam->tar->GetDataElement(operand1->literal_str)
                      ->GetDataType();
  }

  if(string(op) != _EQ) {
    newMemberType = SelectType(t1, t2, op);
  } else {
    newMemberType = t1;
    resultingTAR->RemoveDataElement(operand0->literal_str);
  }

  ParameterPtr newMemberParam =
      operation->GetParametersByName(PARAM(TAL_ARITHMETIC, _NEW_MEMBER));
  if (newMemberParam) {
    resultingTAR->AddAttribute(newMemberParam.get()->literal_str,
                               newMemberType);
  } else {
    resultingTAR->AddAttribute("op_result", DataType(DOUBLE, 1));
  }

  SetResultingType(inputTARParam->tar, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForCrossOp(OperationPtr operation) {

  const char *prefix[] = {LEFT_DATAELEMENT_PREFIX, RIGHT_DATAELEMENT_PREFIX};
  TARPtr leftTARParam = operation->GetParameters().front()->tar;
  TARPtr rightTARParam = operation->GetParameters().back()->tar;
  TARPtr tars[] = {leftTARParam, rightTARParam};
  TARPtr resultingTAR = std::make_shared<TAR>(0, "", nullptr);

  for (int32_t i = NUM_TARS_FOR_JOIN - 1; i >= 0; i--) {
    for (const auto &dim : tars[i]->GetDimensions()) {

      DimensionPtr newDim;
      if (dim->GetDimensionType() == IMPLICIT) {
        newDim = make_shared<Dimension>(
            UNSAVED_ID, prefix[i] + dim->GetName(), dim->GetType(),
            dim->GetLowerBound(), dim->GetUpperBound(), dim->GetSpacing());
      } else {
        newDim = make_shared<Dimension>(UNSAVED_ID, prefix[i] + dim->GetName(),
                                        dim->GetDataset());
      }

      newDim->CurrentUpperBound() = dim->CurrentUpperBound();
      resultingTAR->AddDimension(newDim);
    }

    for (const auto &att : tars[i]->GetAttributes()) {
      AttributePtr _att = make_shared<Attribute>(
          att->GetId(), prefix[i] + att->GetName(), att->GetType());
      resultingTAR->AddAttribute(_att);
    }
  }

  if (leftTARParam->GetType()) {
    resultingTAR->AlterType(leftTARParam->GetType());
    for (auto entry : leftTARParam->GetRoles()) {
      resultingTAR->SetRole(LEFT_DATAELEMENT_PREFIX + entry.first,
                            entry.second);
    }
  }

  if (resultingTAR->CheckMaxSpan()) {
    throw runtime_error("Resulting TAR spans a region larger than allowed.");
  }

  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForDimJoinOp(OperationPtr operation) {

  const char *prefix[] = {LEFT_DATAELEMENT_PREFIX, RIGHT_DATAELEMENT_PREFIX};
  int32_t count = 0;

  TARPtr leftTARParam =
      operation->GetParametersByName(PARAM(TAL_DIMJOIN, _OPERAND, 0))->tar;
  TARPtr rightTARParam =
      operation->GetParametersByName(PARAM(TAL_DIMJOIN, _OPERAND, 1))->tar;
  TARPtr tars[] = {leftTARParam, rightTARParam};
  TARPtr resultingTAR = std::make_shared<TAR>(0, "", nullptr);

  map<string, string> leftDims, rightDims;

  while (true) {
    auto param1 =
        operation->GetParametersByName(PARAM(TAL_DIMJOIN, _DIMENSION, count++));
    auto param2 =
        operation->GetParametersByName(PARAM(TAL_DIMJOIN, _DIMENSION, count++));
    if (param1 == nullptr)
      break;
    leftDims[param1->literal_str] = param2->literal_str;
    rightDims[param2->literal_str] = param1->literal_str;
  }

  for (const auto &dim : leftTARParam->GetDimensions()) {
    if (leftDims.find(dim->GetName()) == leftDims.end()) {
      resultingTAR->AddDimension(dim);
      DimensionPtr _dim =
          resultingTAR->GetDataElement(dim->GetName())->GetDimension();
      _dim->AlterName(prefix[0] + _dim->GetName());

    } else {
      DimensionPtr newDim;
      string leftDimName = dim->GetName();
      string rightDimName = leftDims[dim->GetName()];
      auto leftDim = leftTARParam->GetDataElement(leftDimName)->GetDimension();
      auto rightDim =
          rightTARParam->GetDataElement(rightDimName)->GetDimension();
      _storageManager->IntersectDimensions(leftDim, rightDim, newDim);

      newDim->AlterName(prefix[0] + leftDimName);
      resultingTAR->AddDimension(newDim);

      /*Intersection is empty if returned dataset is null.*/
      if (newDim->GetDataset() == nullptr)
        operation->AddParam(PARAM(TAL_DIMJOIN, _NO_INTERSECTION_JOIN), true);
    }
  }

  for (const auto &dim : rightTARParam->GetDimensions()) {
    if (rightDims.find(dim->GetName()) == rightDims.end()) {
      resultingTAR->AddDimension(dim);
      DimensionPtr _dim =
          resultingTAR->GetDataElement(dim->GetName())->GetDimension();
      _dim->AlterName(prefix[1] + _dim->GetName());
    }
  }

  for (int32_t i = NUM_TARS_FOR_JOIN - 1; i >= 0; i--) {

    for (const auto &att : tars[i]->GetAttributes()) {
      AttributePtr _att = make_shared<Attribute>(
          att->GetId(), prefix[i] + att->GetName(), att->GetType());

      resultingTAR->AddAttribute(_att);
    }
  }

  if (leftTARParam->GetType()) {
    resultingTAR->AlterType(leftTARParam->GetType());
    for (auto entry : leftTARParam->GetRoles()) {
      resultingTAR->SetRole(LEFT_DATAELEMENT_PREFIX + entry.first,
                            entry.second);
    }
  }

  if (resultingTAR->CheckMaxSpan()) {
    throw runtime_error("Resulting TAR spans a region larger than allowed.");
  }

  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForAggregationOp(OperationPtr operation) {

  ParameterPtr inputTARParam =
      operation->GetParametersByName(PARAM(TAL_AGGREGATE, _INPUT_TAR));
  TARPtr inputTAR = inputTARParam->tar;
  TARPtr resultingTAR = std::make_shared<TAR>(0, "", nullptr);
  int32_t countOp = 2, dims = 0;

  while (true) {
    auto param = operation->GetParametersByName(
        PARAM(TAL_AGGREGATE, _DIMENSION, dims++));
    if (param == nullptr)
      break;
    auto dataElement = inputTAR->GetDataElement(param->literal_str);
    resultingTAR->AddDimension(dataElement->GetDimension());
  }

  if (resultingTAR->GetDataElements().empty()) {
    resultingTAR->AddDimension(DEFAULT_SYNTHETIC_DIMENSION,
                               DataType(INT32, 1), 0, 0, 0);
  }

  while (true) {
    auto param =
        operation->GetParametersByName(PARAM(TAL_AGGREGATE, _OPERAND, countOp));
    if (param == nullptr)
      break;
    resultingTAR->AddAttribute(param->literal_str, DataType(DOUBLE, 1));
    countOp += 3;
  }

  SetResultingType(inputTARParam->tar, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForSplitOp(OperationPtr operation) {
  ParameterPtr inputTARParam = operation->GetParameters().front();
  TARPtr resultingTAR = inputTARParam->tar->Clone(false, false, false);
  SetResultingType(inputTARParam->tar, resultingTAR);
  return resultingTAR;
}

TARPtr SchemaBuilder::InferSchemaForUserDefined(OperationPtr operation) {
  // Get operator name
  std::string operatorName = operation->GetName();

  // Get schema infer string
  std::string inferSchemaString = _configurationManager->GetStringValue(
      OPERATOR_SCHEMA_INFER_STRING(operatorName));
  throw std::runtime_error(
      "Infer schema for user defined functions not implemented yet.");

  return nullptr;
}

TARPtr SchemaBuilder::InferSchema(OperationPtr operation) {

  if (operation->GetOperation() == TAL_SCAN) {
    return InferSchemaForScanOp(operation);
  } else if (operation->GetOperation() == TAL_SELECT) {
    return InferSchemaForSelectOp(operation);
  } else if (operation->GetOperation() == TAL_FILTER) {
    return InferSchemaForFilterOp(operation);
  } else if (operation->GetOperation() == TAL_SUBSET) {
    return InferSchemaForSubsetOp(operation);
  } else if (operation->GetOperation() == TAL_LOGICAL) {
    return InferSchemaForLogicalOp(operation);
  } else if (operation->GetOperation() == TAL_COMPARISON) {
    return InferSchemaForComparisonOp(operation);
  } else if (operation->GetOperation() == TAL_ARITHMETIC) {
    return InferSchemaForArithmeticOp(operation);
  } else if (operation->GetOperation() == TAL_CROSS) {
    return InferSchemaForCrossOp(operation);
  } else if (operation->GetOperation() == TAL_DIMJOIN) {
    return InferSchemaForDimJoinOp(operation);
  } else if (operation->GetOperation() == TAL_AGGREGATE) {
    return InferSchemaForAggregationOp(operation);
  } else if (operation->GetOperation() == TAL_UNION) {
    return InferSchemaForSplitOp(operation);
  } else if (operation->GetOperation() == TAL_USER_DEFINED) {
    return InferSchemaForUserDefined(operation);
  }

  return nullptr;
}