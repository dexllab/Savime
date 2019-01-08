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

//----------------------------------------------------------------------------------------------------------------------
list<PushDownOperationPtr> DefaultOptimizer::OptimizeSelect(int64_t node, OperationPtr op, QueryGraphPtr queryGraph) {

  list<PushDownOperationPtr> pushDownOperations;
  auto parents = queryGraph->GetParents(node);

  bool hasSingleDimension = op->GetResultingTAR()->GetDimensions().size() == 1;
  bool hasIdimension = op->GetResultingTAR()->GetDataElement(DEFAULT_SYNTHETIC_DIMENSION) != nullptr;

  if (hasIdimension && hasSingleDimension) {
    return pushDownOperations;
  }

  if(!parents.empty()){
    auto parentNode = parents.front();
    auto parentOperator = queryGraph->GetOperator(parents.front());

    switch(parentOperator->GetOperation()){
      case TAL_FILTER: {
       SchemaDependencyChecker checker;
       auto de = checker.GetDependencies(queryGraph, parentNode);
       auto tar = queryGraph->GetOriginalOperator(node)->GetResultingTAR();

       if(CHECK_DEPEDENCIES(tar, de)){
         if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                   PARAM(TAL_SELECT, _INPUT_TAR),
                                   PARAM(TAL_FILTER, _INPUT_TAR),
                                   queryGraph, _metadataManager)){

           auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                   PARAM(TAL_FILTER, _INPUT_TAR),
                                                                   node,
                                                                   PARAM(TAL_SELECT, _INPUT_TAR));
           pushDownOperations.push_back(pushDownOperation);
         }
       }
      } break;
      case TAL_SUBSET: {

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_SUBSET, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_SELECT, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);

      } break;
      case TAL_ARITHMETIC: {

        SchemaDependencyChecker checker;
        auto de = checker.GetDependencies(queryGraph, parentNode);
        auto param = queryGraph->GetOriginalOperator(parentNode)->GetParametersByName(PARAM(TAL_ARITHMETIC, _NEW_MEMBER));
        auto tar = queryGraph->GetOriginalOperator(node)->GetResultingTAR();

        if(CHECK_DEPEDENCIES(tar, de) && tar->HasDataElement(param->literal_str)){
           auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_ARITHMETIC, _INPUT_TAR),
                                                                  node,
                                                                  PARAM(TAL_SELECT, _INPUT_TAR));
           pushDownOperations.push_back(pushDownOperation);
        }
        //TODO[HEMANO] = WHEN A PROJECTION DROPS AN ATTRIBUTE WHICH WAS DERIVED, THE DERIVED OPERATION
        //CAN BE DROPPED FROM THE QUERY AS WELL.

      } break;
      case TAL_CROSS: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_SELECT, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_SELECT, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_EQUIJOIN: break;
      case TAL_DIMJOIN: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_SELECT, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_SELECT, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_SLICE: {

        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_SLICE, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_SELECT, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);

      } break;
      case TAL_TRANSLATE: {
        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_TRANSLATE, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_SELECT, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);
      } break;
    }

  }

  return pushDownOperations;
}

list<PushDownOperationPtr> DefaultOptimizer::OptimizeFilter(int64_t node, OperationPtr op, QueryGraphPtr queryGraph) {

  list<PushDownOperationPtr> pushDownOperations;
  auto parents = queryGraph->GetParents(node, PARAM(TAL_FILTER, _INPUT_TAR));

  if(!parents.empty()){

    auto parentNode = parents.front();
    auto parentOperator = queryGraph->GetOperator(parents.front());

    switch(parentOperator->GetOperation()){
      case TAL_SELECT: {

        bool hasSingleDimension = parentOperator->GetResultingTAR()->GetDimensions().size() == 1;
        bool hasIdimension = parentOperator->GetResultingTAR()->GetDataElement(DEFAULT_SYNTHETIC_DIMENSION) != nullptr;

        if(!hasSingleDimension || !hasIdimension)
          if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                    PARAM(TAL_FILTER, _INPUT_TAR),
                                    PARAM(TAL_SELECT, _INPUT_TAR),
                                    queryGraph, _metadataManager)){

            auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                    PARAM(TAL_SELECT, _INPUT_TAR),
                                                                    node,
                                                                    PARAM(TAL_FILTER, _INPUT_TAR));

          }

      } break;
      case TAL_FILTER: {

        if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                  PARAM(TAL_FILTER, _INPUT_TAR),
                                  PARAM(TAL_FILTER, _INPUT_TAR),
                                  queryGraph, _metadataManager)){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR),
                                                                  node,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_SUBSET: {

        if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                  PARAM(TAL_FILTER, _INPUT_TAR),
                                  PARAM(TAL_SUBSET, _INPUT_TAR),
                                  queryGraph, _metadataManager)){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR),
                                                                  node,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_ARITHMETIC: {

        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_ARITHMETIC, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_FILTER, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);

      } break;
      case TAL_CROSS: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_EQUIJOIN: break;
      case TAL_DIMJOIN: {


        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_SLICE: {

        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_SLICE, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_FILTER, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);

      } break;
      case TAL_TRANSLATE: {
        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_TRANSLATE, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_FILTER, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);
      } break;
    }

  }

  return pushDownOperations;
}

list<PushDownOperationPtr> DefaultOptimizer::OptimizeSubset(int64_t node, OperationPtr op, QueryGraphPtr queryGraph) {
  list<PushDownOperationPtr> pushDownOperations;
  auto parents = queryGraph->GetParents(node, PARAM(TAL_SUBSET, _INPUT_TAR));

  if(!parents.empty()){

    auto parentNode = parents.front();
    auto parentOperator = queryGraph->GetOperator(parents.front());

    switch(parentOperator->GetOperation()){
      case TAL_SELECT: {

        bool hasSingleDimension = parentOperator->GetResultingTAR()->GetDimensions().size() == 1;
        bool hasIdimension = parentOperator->GetResultingTAR()->GetDataElement(DEFAULT_SYNTHETIC_DIMENSION) != nullptr;

        if(!hasSingleDimension || !hasIdimension)
          if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                    PARAM(TAL_SUBSET, _INPUT_TAR),
                                    PARAM(TAL_SELECT, _INPUT_TAR),
                                    queryGraph, _metadataManager)){

            auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                    PARAM(TAL_SELECT, _INPUT_TAR),
                                                                    node,
                                                                    PARAM(TAL_SUBSET, _INPUT_TAR));

          }

      } break;
      case TAL_FILTER: {

        if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                  PARAM(TAL_SUBSET, _INPUT_TAR),
                                  PARAM(TAL_FILTER, _INPUT_TAR),
                                  queryGraph, _metadataManager)){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_FILTER, _INPUT_TAR),
                                                                  node,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_SUBSET: {

        if(CHECK_ALTERNATIVE_PATH(node, parentNode,
                                  PARAM(TAL_SUBSET, _INPUT_TAR),
                                  PARAM(TAL_SUBSET, _INPUT_TAR),
                                  queryGraph, _metadataManager)){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR),
                                                                  node,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_ARITHMETIC: {

        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_ARITHMETIC, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_SUBSET, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);

      } break;
      case TAL_CROSS: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_EQUIJOIN: break;
      case TAL_DIMJOIN: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_SUBSET, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_SLICE: {

        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_SLICE, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_SUBSET, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);

      } break;
      case TAL_TRANSLATE: {
        auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                PARAM(TAL_TRANSLATE, _INPUT_TAR),
                                                                node,
                                                                PARAM(TAL_SUBSET, _INPUT_TAR));
        pushDownOperations.push_back(pushDownOperation);
      } break;
    }

  }

  return pushDownOperations;
}

list<PushDownOperationPtr> DefaultOptimizer::OptimizeArithmetic(int64_t node, OperationPtr op,
                                                                QueryGraphPtr queryGraph) {
  list<PushDownOperationPtr> pushDownOperations;

  auto parents = queryGraph->GetParents(node, PARAM(TAL_ARITHMETIC, _INPUT_TAR));

  if(!parents.empty()){

    auto parentNode = parents.front();
    auto parentOperator = queryGraph->GetOperator(parents.front());
    auto arithmeticOP = op->GetParametersByName(PARAM(TAL_ARITHMETIC, _OPERATOR))->literal_str;
    if(arithmeticOP == _EQ)
      return pushDownOperations;

    switch(parentOperator->GetOperation()){
      case TAL_CROSS: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_ARITHMETIC, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_CROSS, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_ARITHMETIC, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
      case TAL_EQUIJOIN: break;
      case TAL_DIMJOIN: {

        OperationSplitter splitter;
        auto split = splitter.Split(queryGraph, node, parentNode);

        if(split.leftNode != INVALID_QUERY_GRAPH_NODE){

          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 0),
                                                                  split.leftNode,
                                                                  PARAM(TAL_ARITHMETIC, _INPUT_TAR));

          pushDownOperations.push_back(pushDownOperation);
        }

        if(split.rightNode != INVALID_QUERY_GRAPH_NODE){
          auto pushDownOperation = make_shared<PushDownOperation>(parentNode,
                                                                  PARAM(TAL_DIMJOIN, _OPERAND, 1),
                                                                  split.rightNode,
                                                                  PARAM(TAL_ARITHMETIC, _INPUT_TAR));
          pushDownOperations.push_back(pushDownOperation);
        }

      } break;
    }

  }

  return pushDownOperations;
}

list<PushDownOperationPtr> DefaultOptimizer::OptimizeSlice(int64_t node, OperationPtr op, QueryGraphPtr queryGraph) {
  list<PushDownOperationPtr> pushDownOperations;
  return pushDownOperations;
}

void DefaultOptimizer::OptimizeNode(int64_t node, QueryGraphPtr queryGraph, QueryPlanPtr queryPlan) {


  list<PushDownOperationPtr> pushDownOperations;

  /*Check every operation from last to first, until none can be optimized*/
  while(true){

    auto op = queryGraph->GetOriginalOperator(node);

    switch(op->GetOperation()){
      case TAL_SELECT: pushDownOperations = OptimizeSelect(node, op, queryGraph); break;
      case TAL_FILTER: pushDownOperations = OptimizeFilter(node, op, queryGraph); break;
      case TAL_SUBSET: pushDownOperations = OptimizeSubset(node, op, queryGraph); break;
      case TAL_ARITHMETIC: pushDownOperations = OptimizeArithmetic(node, op, queryGraph); break;
      case TAL_SLICE: pushDownOperations = OptimizeSlice(node, op, queryGraph); break;
    }

    if(pushDownOperations.empty())
      break;

    for(const auto& pushDownOperation : pushDownOperations)
      queryGraph->PushDown(pushDownOperation->child,
                           pushDownOperation->parent,
                           pushDownOperation->childInpurTarParamName,
                           pushDownOperation->parentInpurTarParamName);
  }

}


SavimeResult DefaultOptimizer::Optimize(QueryDataManagerPtr queryDataManager) {

  if(queryDataManager->GetQueryPlan()->GetType() == DDL)
    return SAVIME_SUCCESS;

  auto old = queryDataManager->GetQueryPlan();
  QueryGraphPtr queryGraph = make_shared<QueryGraph>();
  queryGraph->Create(queryDataManager->GetQueryPlan());
  //printf("-------INITIAL-------------\n%s\n", queryGraph->toDBGString().c_str());

  queryGraph->SimplifyNots(_parser);
  //printf("-------AFTER simpligy-------------\n%s\n", queryGraph->toDBGString().c_str());

  queryGraph->SplitConjuctions(_parser);
  //printf("-------AFTER SPLIT-------------\n%s\n", queryGraph->toDBGString().c_str());


  auto& unOptimizedNodes = queryGraph->GetUnoptimizedNodes();

  while(!unOptimizedNodes.empty()) {
    int64_t node = unOptimizedNodes.front();
    unOptimizedNodes.pop_front();
    OptimizeNode(node, queryGraph, queryDataManager->GetQueryPlan());
  }


  //printf("-------SFINAL-------------\n%s\n", queryGraph->toDBGString().c_str());
  auto oldQueryplan = queryDataManager->GetQueryPlan()->toString();
  queryDataManager->SetQueryPlan(queryGraph->ToQueryPlan(_parser));
  auto queryplan = queryDataManager->GetQueryPlan()->toString();

  //if(oldQueryplan != queryplan){
  //  printf("-----\n%s\n---------------\n%s\n", oldQueryplan.c_str(), queryplan.c_str());
  // }
  //auto newGraph = make_shared<QueryGraph>();
  //newGraph->Create(queryDataManager->GetQueryPlan());
  //printf("-------FINAL-------------\n%s\n", newGraph->toDBGString().c_str());

  return SAVIME_SUCCESS;
}
