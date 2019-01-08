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
#include "default_optimizer.h"

void OperationSplitter::RemovePrefix(QueryGraphPtr queryGraph, int64_t node,
                                      string prefix, bool deleteRemaining,
                                     ParameterType typeToRemove ){

  auto op = queryGraph->GetOperator(node)->GetOperation();
  auto& parameters = queryGraph->GetOriginalOperator(node)->GetParameters();
  auto prefixLen = prefix.length();
  list<list<ParameterPtr>::iterator> positionsToRemove;

  for(auto it = parameters.begin(); it != parameters.end(); it++) {
    auto param = *it;
    if(param->type == typeToRemove){
      if(!param->literal_str.compare(0, prefixLen, prefix)){
        param->literal_str = param->literal_str.erase(0, prefixLen);
      } else {
        positionsToRemove.push_back(it);
      }
    }
  }

  if(deleteRemaining) {

    if(op == TAL_SUBSET){

      list<list<ParameterPtr>::iterator> allPositionsToRemove;

      for (auto it : positionsToRemove) {
        allPositionsToRemove.push_back(it);

        if(op == TAL_SUBSET){
          it++;
          allPositionsToRemove.push_back(it);
          it++;
          allPositionsToRemove.push_back(it);
        }
      }

      for (auto it : allPositionsToRemove) {
        parameters.erase(it);
      }

      int32_t countDim = 0;
      for(auto it = parameters.begin(); it != parameters.end(); it++){
        if((*it)->type == LITERAL_STRING_PARAM){
          (*it)->name = PARAM(TAL_SUBSET, _DIMENSION, countDim);
          it++;
          (*it)->name = PARAM(TAL_SUBSET, _LOWER_BOUND, countDim);
          it++;
          (*it)->name = PARAM(TAL_SUBSET, _UPPER_BOUND, countDim);
          countDim++;
        }
      }

    } else {
      for (auto it : positionsToRemove) {
        parameters.erase(it);
      }
    }
  }
}

SplitNodes OperationSplitter::SplitSelectOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{};

  auto newNode = queryGraph->DuplicateNode(child);
  RemovePrefix(queryGraph, child, LEFT_DATAELEMENT_PREFIX, true);
  RemovePrefix(queryGraph, newNode, RIGHT_DATAELEMENT_PREFIX, true);
  splitNodes.leftNode = child;
  splitNodes.rightNode = newNode;

  return splitNodes;
}


SplitNodes OperationSplitter::SplitSelectOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SplitNodes splitNodes{};
  auto newNode = queryGraph->DuplicateNode(child);
  RemovePrefix(queryGraph, child, LEFT_DATAELEMENT_PREFIX, true);
  RemovePrefix(queryGraph, newNode, RIGHT_DATAELEMENT_PREFIX, true);

  auto rightTar = queryGraph->GetOriginalOperator(parent)->GetParametersByName(PARAM(TAL_DIMJOIN, _OPERAND, 1))->tar;
  auto op = queryGraph->GetOriginalOperator(newNode);

  auto counter = op->GetParameters().size()-2;

  for(const auto& de : rightTar->GetDataElements()){
    if(de->GetType() == DIMENSION_SCHEMA_ELEMENT)
      op->AddIdentifierParam(PARAM(TAL_SELECT, _IDENTIFIER, static_cast<int32_t>(counter++)), de->GetName());
  }

  splitNodes.leftNode = child;
  splitNodes.rightNode = newNode;

  return splitNodes;
}


SplitNodes OperationSplitter::SplitFilterOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  if(CHECK_LEFT_RIGHT_DEPENDENCIES(des)) {

    auto prefix = split(des.front()->GetName(), _UNDERSCORE[0])[0]+_UNDERSCORE;
    auto filterSubtree = queryGraph->GetFilterSubtree(child);

    for(const auto &subtreeNode : filterSubtree) {
      RemovePrefix(queryGraph, subtreeNode, prefix, false);
    }

    if(prefix == LEFT_DATAELEMENT_PREFIX){
      splitNodes.leftNode = child;
    } else {
      splitNodes.rightNode = child;
    }
  }

  return splitNodes;
}

SplitNodes OperationSplitter::SplitFilterOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  if(CHECK_LEFT_RIGHT_DEPENDENCIES(des)) {

    auto prefix = split(des.front()->GetName(), _UNDERSCORE[0])[0]+_UNDERSCORE;
    auto filterSubtree = queryGraph->GetFilterSubtree(child);

    for(const auto &subtreeNode : filterSubtree) {
      RemovePrefix(queryGraph, subtreeNode, prefix, false);
    }

    if(prefix == LEFT_DATAELEMENT_PREFIX){
      splitNodes.leftNode = child;
    } else {
      splitNodes.rightNode = child;
    }
  }

  return splitNodes;
}


SplitNodes OperationSplitter::SplitSubsetOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  if(CHECK_LEFT_RIGHT_DEPENDENCIES(des)) {

    auto prefix = split(des.front()->GetName(), _UNDERSCORE[0])[0]+_UNDERSCORE;
    RemovePrefix(queryGraph, child, prefix, false, LITERAL_STRING_PARAM);

    if(prefix == LEFT_DATAELEMENT_PREFIX){
      splitNodes.leftNode = child;
    } else {
      splitNodes.rightNode = child;
    }

  } else {

    auto newNode = queryGraph->DuplicateNode(child);
    RemovePrefix(queryGraph, child, LEFT_DATAELEMENT_PREFIX, true, LITERAL_STRING_PARAM);
    RemovePrefix(queryGraph, newNode, RIGHT_DATAELEMENT_PREFIX, true, LITERAL_STRING_PARAM);
    splitNodes.leftNode = child;
    splitNodes.rightNode = newNode;
  }

  return splitNodes;
}

SplitNodes OperationSplitter::SplitSubsetOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  if(CHECK_LEFT_RIGHT_DEPENDENCIES(des)) {

    auto prefix = split(des.front()->GetName(), _UNDERSCORE[0])[0]+_UNDERSCORE;
    RemovePrefix(queryGraph, child, prefix, false, LITERAL_STRING_PARAM);

    if(prefix == LEFT_DATAELEMENT_PREFIX){
      splitNodes.leftNode = child;
    } else {
      splitNodes.rightNode = child;
    }

  } else {
    auto newNode = queryGraph->DuplicateNode(child);
    RemovePrefix(queryGraph, child, LEFT_DATAELEMENT_PREFIX, true, LITERAL_STRING_PARAM);
    RemovePrefix(queryGraph, newNode, RIGHT_DATAELEMENT_PREFIX, true, LITERAL_STRING_PARAM);
    splitNodes.leftNode = child;
    splitNodes.rightNode = newNode;
  }

  return splitNodes;
}


SplitNodes OperationSplitter::SplitArithmeticOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  if(CHECK_LEFT_RIGHT_DEPENDENCIES(des)) {

    auto op = queryGraph->GetOriginalOperator(child);
    auto newMemberName = op->GetParametersByName(PARAM(TAL_ARITHMETIC, _NEW_MEMBER))->literal_str;

    map<string, string> prefixes;
    for(const auto& de : des){
      auto splitName = split(de->GetName(), _UNDERSCORE[0]);
      prefixes[splitName[0]+_UNDERSCORE] = splitName[0]+_UNDERSCORE;
    }

    string prefix;
    if(prefixes.find(LEFT_DATAELEMENT_PREFIX) != prefixes.end()){
      prefix = LEFT_DATAELEMENT_PREFIX;
    } else {
      prefix = RIGHT_DATAELEMENT_PREFIX;
    }

    if(newMemberName[0] != _UNDERSCORE[0]){
      auto newNode = queryGraph->DuplicateNode(child);
      auto aux = child;
      child = newNode;
      newNode = aux;

      auto newOp = queryGraph->GetOriginalOperator(newNode);
      auto newMemberParam = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _NEW_MEMBER));
      auto operatorParam = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERATOR));
      operatorParam->literal_str = _EQ;
      auto op0 = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERAND, 0));
      op0->literal_str = prefix+newMemberParam->literal_str;
      op0->type = IDENTIFIER_PARAM;
      auto op1 = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERAND, 1));
      op1->literal_str = op0->literal_str;
      op1->type = IDENTIFIER_PARAM;
    }

    RemovePrefix(queryGraph, child, prefix, false, IDENTIFIER_PARAM);

    if(prefix == LEFT_DATAELEMENT_PREFIX){
      splitNodes.leftNode = child;
    } else {
      splitNodes.rightNode = child;
    }

  }

  return splitNodes;
}

SplitNodes OperationSplitter::SplitArithmeticOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  SchemaDependencyChecker checker;
  auto des = checker.GetDependencies(queryGraph, child);
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  if(CHECK_LEFT_RIGHT_DEPENDENCIES(des)) {

    auto op = queryGraph->GetOriginalOperator(child);
    auto newMemberName = op->GetParametersByName(PARAM(TAL_ARITHMETIC, _NEW_MEMBER))->literal_str;

    map<string, string> prefixes;
    for(const auto& de : des){
      auto splitName = split(de->GetName(), _UNDERSCORE[0]);
      prefixes[splitName[0]+_UNDERSCORE] = splitName[0]+_UNDERSCORE;
    }

    string prefix;
    if(prefixes.find(LEFT_DATAELEMENT_PREFIX) != prefixes.end()){
      prefix = LEFT_DATAELEMENT_PREFIX;
    } else {
      prefix = RIGHT_DATAELEMENT_PREFIX;
    }

    if(newMemberName[0] != _UNDERSCORE[0]){
      auto newNode = queryGraph->DuplicateNode(child);
      auto aux = child;
      child = newNode;
      newNode = aux;

      auto newOp = queryGraph->GetOriginalOperator(newNode);
      auto newMemberParam = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _NEW_MEMBER));
      auto operatorParam = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERATOR));
      operatorParam->literal_str = _EQ;
      auto op0 = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERAND, 0));
      op0->literal_str = prefix+newMemberParam->literal_str;
      op0->type = IDENTIFIER_PARAM;
      auto op1 = newOp->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERAND, 1));
      op1->literal_str = op0->literal_str;
      op1->type = IDENTIFIER_PARAM;
    }

    RemovePrefix(queryGraph, child, prefix, false, IDENTIFIER_PARAM);

    if(prefix == LEFT_DATAELEMENT_PREFIX){
      splitNodes.leftNode = child;
    } else {
      splitNodes.rightNode = child;
    }
  }

  return splitNodes;
}

SplitNodes OperationSplitter::Split(QueryGraphPtr queryGraph, int64_t child, int64_t parent){

  auto childOp = queryGraph->GetOriginalOperator(child)->GetOperation();
  auto parentOp = queryGraph->GetOriginalOperator(parent)->GetOperation();
  SplitNodes splitNodes{INVALID_QUERY_GRAPH_NODE, INVALID_QUERY_GRAPH_NODE};

  switch(childOp){
    case TAL_SELECT:{
      if(parentOp == TAL_CROSS)
        return SplitSelectOverCross(queryGraph, child, parent);
      else if (parentOp == TAL_DIMJOIN)
        return SplitSelectOverDimJoin(queryGraph, child, parent);
    }break;
    case TAL_FILTER:
      if(parentOp == TAL_CROSS)
        return SplitFilterOverCross(queryGraph, child, parent);
      else if (parentOp == TAL_DIMJOIN)
        return SplitFilterOverDimJoin(queryGraph, child, parent);
      break;
    case TAL_SUBSET:
      if(parentOp == TAL_CROSS)
        return SplitSubsetOverCross(queryGraph, child, parent);
      else if (parentOp == TAL_DIMJOIN)
        return SplitSubsetOverDimJoin(queryGraph, child, parent);
      break;
    case TAL_ARITHMETIC:
      if(parentOp == TAL_CROSS)
        return SplitArithmeticOverCross(queryGraph, child, parent);
      else if (parentOp == TAL_DIMJOIN)
        return SplitArithmeticOverDimJoin(queryGraph, child, parent);
      break;
  }

  return splitNodes;
}