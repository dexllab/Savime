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
*    HERMANO L. S. LUSTOSA				JANUARY 2019
*/
#include "default_optimizer.h"

list<DataElementPtr> SchemaDependencyChecker::GetSelectDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElement;
  auto op = graph->GetOriginalOperator(node);
  auto tar = op->GetResultingTAR();
  for(const auto& de : tar->GetDataElements()) {
    if(de->GetType() == ATTRIBUTE_SCHEMA_ELEMENT)
      dataElement.push_back(de);
  }
  return dataElement;
}

list<DataElementPtr> SchemaDependencyChecker::GetFilterDependencies(QueryGraphPtr graph, int64_t node) {

  list<DataElementPtr> dataElements;
  auto op = graph->GetOperator(node);

  switch(op->GetOperation()){
    case TAL_FILTER: {

      auto parents = graph->GetParents(node);
      for (auto parent : parents) {
        auto opParent = graph->GetOperator(parent)->GetOperation();

        if (opParent == TAL_LOGICAL || opParent == TAL_COMPARISON) {
          auto subElements = GetFilterDependencies(graph, parent);
          dataElements.insert(dataElements.end(), subElements.begin(), subElements.end());
        }
      }

    }break;
    case TAL_LOGICAL: {

      auto parents = graph->GetParents(node);
      for(auto parent : parents){
        auto opParent = graph->GetOperator(parent)->GetOperation();

        if(opParent == TAL_LOGICAL || opParent == TAL_COMPARISON){
          auto subElements = GetFilterDependencies(graph, parent);
          dataElements.insert(dataElements.end(), subElements.begin(), subElements.end());
        }
      }

    } break;
    case TAL_COMPARISON: {

      auto inputar = op->GetParametersByName(PARAM(TAL_COMPARISON, _INPUT_TAR))->tar;
      for(const auto& param : op->GetParameters()){
        if(param->type  == IDENTIFIER_PARAM ||  param->type  == LITERAL_STRING_PARAM){
            auto de = inputar->GetDataElement(param->literal_str);
            if(de != nullptr)
              dataElements.push_back(de);
        }
      }
    }
  }

  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetSubsetDependencies(QueryGraphPtr graph, int64_t node) {
  auto op = graph->GetOperator(node);
  auto inputar = op->GetParametersByName(PARAM(TAL_SUBSET, _INPUT_TAR))->tar;
  list<DataElementPtr> dataElements;
  for(const auto& de :inputar->GetDataElements()){
    if(de->GetType() == DIMENSION_SCHEMA_ELEMENT) {
      for(const auto &param : op->GetParameters()){
        if(param->type ==  IDENTIFIER_PARAM || param->type == LITERAL_STRING_PARAM){
          if(param->literal_str == de->GetName()) {
            dataElements.push_back(de);
            break;
          }
        }
      }
    }
  }
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetLogicalDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetComparisonDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetArithmeticDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  auto op = graph->GetOperator(node);
  auto inputar = op->GetParametersByName(PARAM(TAL_ARITHMETIC, _INPUT_TAR))->tar;
  for(const auto& param : op->GetParameters()){
    if(param->type  == IDENTIFIER_PARAM ||  param->type  == LITERAL_STRING_PARAM){
      auto de = inputar->GetDataElement(param->literal_str);
      if(de != nullptr)
        dataElements.push_back(de);
    }
  }
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetCrossDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetEquiJoinDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetDimJoinDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  auto op = graph->GetOriginalOperator(node);
  auto tar = op->GetResultingTAR();
  for(const auto& param : op->GetParameters()){
    if(param->type  == LITERAL_STRING_PARAM ){
      auto de = tar->GetDataElement(param->literal_str);
      if(de != nullptr)
        if(de->GetType() == DIMENSION_SCHEMA_ELEMENT)
          dataElements.push_back(de);
    }
  }
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetSliceDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetAtt2DimDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetAgreggateDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  auto op = graph->GetOperator(node);
  auto inputar = op->GetParametersByName(PARAM(TAL_AGGREGATE, _INPUT_TAR))->tar;
  for(const auto& param : op->GetParameters()){
    if(param->type  == IDENTIFIER_PARAM ||  param->type  == LITERAL_STRING_PARAM){
      auto de = inputar->GetDataElement(param->literal_str);
      if(de != nullptr)
        if(de->GetType() == DIMENSION_SCHEMA_ELEMENT)
          dataElements.push_back(de);
    }
  }
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetUnionDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetTranslateDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  return dataElements;
}

list<DataElementPtr> SchemaDependencyChecker::GetDependencies(QueryGraphPtr graph, int64_t node) {
  list<DataElementPtr> dataElements;
  auto op = graph->GetOriginalOperator(node);

  switch(op->GetOperation()){
    case TAL_SELECT: dataElements = GetSelectDependencies(graph, node); break;
    case TAL_FILTER: dataElements = GetFilterDependencies(graph, node); break;
    case TAL_SUBSET: dataElements = GetSubsetDependencies(graph, node); break;
    case TAL_LOGICAL: dataElements = GetLogicalDependencies(graph, node); break;
    case TAL_COMPARISON: dataElements = GetComparisonDependencies(graph, node); break;
    case TAL_ARITHMETIC: dataElements = GetArithmeticDependencies(graph, node); break;
    case TAL_CROSS: dataElements = GetCrossDependencies(graph, node); break;
    case TAL_EQUIJOIN: dataElements = GetEquiJoinDependencies(graph, node); break;
    case TAL_DIMJOIN: dataElements = GetDimJoinDependencies(graph, node); break;
    case TAL_SLICE: dataElements = GetSliceDependencies(graph, node); break;
    case TAL_ATT2DIM: dataElements = GetAtt2DimDependencies(graph, node); break;
    case TAL_AGGREGATE: dataElements = GetAgreggateDependencies(graph, node); break;
    case TAL_UNION: dataElements = GetUnionDependencies(graph, node); break;
    case TAL_TRANSLATE: dataElements = GetTranslateDependencies(graph, node); break;
  }

  return dataElements;
}



