#include <utility>

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

//Quey Graph definitions
//----------------------------------------------------------------------------------------------------------------------
void QueryGraph::Create(QueryPlanPtr queryPlan) {
  vector<OperationPtr> operations;
  unordered_map<string, int64_t> tarsMap;

  for(const auto& op : queryPlan->GetOperations()){
    operations.push_back(op);
  }

  for(int64_t i = 0; i < operations.size(); i++) {
    _nodes[i] = operations[i];
    _nodesList.push_back(i);
    _unOptimizedNodesList.push_back(i);

    auto tar = operations[i]->GetResultingTAR();
    if(tar != nullptr) {
      tarsMap[tar->GetName()] = i;
    }
  }

  for(int64_t i = 0; i < operations.size(); i++) {
    for(const auto & param : operations[i]->GetParameters()){
      if(param->type == TAR_PARAM){
        int64_t dest = i;
        if(tarsMap.find(param->tar->GetName()) != tarsMap.end()){
          int64_t origin = tarsMap[param->tar->GetName()];
          QueryGraphEdge edge{origin, dest, param->tar, param->name};
          _edges.push_back(edge);
        } else {
          QueryGraphEdge edge{INVALID_QUERY_GRAPH_NODE, dest, param->tar, param->name};
          _edges.push_back(edge);
        }//end else
      }//end if
    }//end for
  }//end for


  auto lastOp = operations.back();
  if(lastOp != nullptr){
    if(lastOp->GetResultingTAR() != nullptr){
      int64_t node = operations.size() - 1;
      QueryGraphEdge edge{node, END_POINT, lastOp->GetResultingTAR(), ""};
      _edges.push_back(edge);
    }
  }
}

int64_t QueryGraph::AddNode(OperationPtr op) {
  int64_t newNode = _nodesList.size();
  _nodesList.push_back(newNode);

  return newNode;
}


list<int64_t> QueryGraph::GetChildren(int64_t node) {
  list<int64_t> children;
  for(const auto& edge : _edges){
    if(edge.origin == node){
      children.push_back(edge.destiny);
    }
  }
  return children;
}

list<int64_t> QueryGraph::GetParents(int64_t node) {
  list<int64_t> parent;
  for(const auto& edge : _edges){
    if(edge.destiny == node && edge.origin != INVALID_QUERY_GRAPH_NODE){
      parent.push_back(edge.origin);
    }
  }
  return parent;
}

list<int64_t> QueryGraph::GetParents(int64_t node, string label) {
  list<int64_t> parent;
  for(const auto& edge : _edges){
    if(edge.destiny == node
        && edge.origin != INVALID_QUERY_GRAPH_NODE
        && edge.label == label){
      parent.push_back(edge.origin);
    }
  }
  return parent;
}

void QueryGraph::PushDown(int64_t child, int64_t parent, string childLabel, string parentLabel) {

  TARPtr parentInpuTar = nullptr;
  auto parentOpCode = _nodes[parent]->GetOperation();
  auto childOpCode = _nodes[child]->GetOperation();
  int64_t parentOrigin = INVALID_QUERY_GRAPH_NODE;

  for(auto& edge : _edges){
    if(edge.destiny == parent && edge.label == parentLabel) {
      edge.destiny = child;
      edge.label = childLabel;
      parentOrigin = edge.origin;
      parentInpuTar = edge.tar;
      break;
    }
  }

  for(auto& edge : _edges) {
    if (edge.origin == child) {
      edge.origin = parent;
    } else if (childOpCode == TAL_FILTER) {

      if (edge.origin == parent && edge.destiny != child && edge.destiny != parentOrigin) {
        edge.origin = parentOrigin;
        edge.tar = parentInpuTar;
      } else if (parentOpCode == TAL_FILTER) {

        if (edge.origin == parentOrigin && edge.destiny != child
            && parentInpuTar->GetName() == edge.tar->GetName()
            && edge.origin != INVALID_QUERY_GRAPH_NODE) {
          edge.origin = child;
          //break;
        }
      }
    }
  }


  //pushing down Logical and comparison operations
  if(childOpCode != TAL_FILTER && parentOpCode == TAL_FILTER){
    for(auto& edge : _edges){
      if(edge.origin == parentOrigin && edge.destiny != child
          && parentInpuTar->GetName() == edge.tar->GetName() ) {
        edge.origin = child;
        //break;
      }
    }
  }


  for(auto& edge : _edges){
    if(edge.origin == parent && edge.destiny == child ) {
      edge.origin = child;
      edge.destiny = parent;
      edge.label = parentLabel;
      break;
    }
  }

  //printf("-------\n%s\n", toDBGString().c_str());
}

list<TARPtr> QueryGraph::GetInputTARS(int64_t node) {
  list<TARPtr> tars;
  for(auto& edge : _edges){
    if(edge.destiny == node) {
      tars.push_back(edge.tar);
    }
  }
  return tars;
}

TARPtr QueryGraph::GetOutputTAR(int64_t node) {
  for(auto& edge : _edges){
    if(edge.origin == node) {
      return edge.tar;
    }
  }

  auto op = GetOriginalOperator(node);
  if(op != nullptr)
    return op->GetResultingTAR();

  return nullptr;
}

list<int64_t>& QueryGraph::GetUnoptimizedNodes() {
  return _unOptimizedNodesList;
}

vector<int64_t>& QueryGraph::GetNodes() {
  return _nodesList;
}

list<QueryGraphEdge>& QueryGraph::GetEdges() {
  return _edges;
}

list<int64_t> QueryGraph::GetFilterSubtree(int64_t node) {

  list<int64_t> openNodes;
  list<int64_t> subtree;
  openNodes.push_back(node);

  while(!openNodes.empty()) {

    auto current = openNodes.front();
    openNodes.pop_front();

    for (const auto &edge : _edges) {
      if(edge.destiny == current){
        if(_nodes.find(edge.origin) != _nodes.end()){
          if(_nodes[edge.origin]->GetOperation() == TAL_LOGICAL
              ||_nodes[edge.origin]->GetOperation() == TAL_COMPARISON ){
            subtree.push_back(edge.origin);
            openNodes.push_back(edge.origin);
          }
        }
      }
    }
  }

  return subtree;
}

list<int64_t> QueryGraph::GetSortedNodes() {

  struct NodeData{
      int64_t node;
      int64_t level;
  };
  vector<NodeData> nodes2Sort(_nodesList.size());

  list<int64_t> sorted;
  unordered_map<int64_t, int64_t> visited;
  list<int64_t> rootNodes;
  list<QueryGraphEdge> edges;
  edges.insert(edges.begin(), _edges.begin(), _edges.end());

  unordered_map<int64_t, int64_t> nonRoot;
  for(const auto& edge : _edges ){
    if(edge.origin != INVALID_QUERY_GRAPH_NODE) {
      nonRoot[edge.destiny] = edge.destiny;
    }
  }

  for(const auto& node : _nodesList){
    if(nonRoot.find(node) == nonRoot.end()){
      rootNodes.push_back(node);
    }
  }

  for(int64_t i = 0; i < _nodesList.size(); i++){
    nodes2Sort[i].node = i;
    nodes2Sort[i].level = 0;
  }

  while(!rootNodes.empty()){


    auto n = rootNodes.front();
    rootNodes.pop_front();
    nodes2Sort[n].level = 0;

    list<int64_t> openNodes;
    openNodes.push_back(n);

    while(!openNodes.empty()){

      auto current = openNodes.front();
      openNodes.pop_front();

      for(const auto &edge : _edges){
        if(edge.origin == current && edge.destiny >= 0){

          if(nodes2Sort[edge.destiny].level < nodes2Sort[current].level + 1){
            nodes2Sort[edge.destiny].level = nodes2Sort[current].level + 1;
          }
          openNodes.push_back(edge.destiny);
        }
      }
    }
  }

  sort(nodes2Sort.begin(), nodes2Sort.end(),
       [](const NodeData & a, const NodeData & b) -> bool
       {
           return a.level < b.level;
       });


  for(int64_t i = 0; i < nodes2Sort.size(); i++){
    sorted.push_back(nodes2Sort[i].node);
  }

  return sorted;
}

void QueryGraph::FlipDisjuctions(ParserPtr parser){

  map<int64_t, OperationPtr> nodesToAdd;
  auto counter = 0ul;

  for(const auto& entry : _nodes){

    auto node = entry.first;
    auto op = entry.second;

    if(op->GetOperation() == TAL_LOGICAL){
      if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _OR){

        int64_t parentNode = INVALID_QUERY_GRAPH_NODE;
        TARPtr parentInputTar = nullptr;

        for(const auto& edge: _edges){
          if(edge.destiny == node && edge.label == PARAM(TAL_LOGICAL, _INPUT_TAR)){
            parentNode = edge.origin;
            parentInputTar = edge.tar;
          }
        }

        auto intputTAR = op->GetParametersByName(PARAM(TAL_LOGICAL, _INPUT_TAR))->tar;
        op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str = _AND;
        list<QueryGraphEdge> edgesToAdd;

        auto outNotOperation = make_shared<Operation>(TAL_LOGICAL);
        outNotOperation->AddParam(PARAM(TAL_LOGICAL, _INPUT_TAR), intputTAR);
        outNotOperation->AddParam(PARAM(TAL_LOGICAL, _OPERATOR), string(_NOT));
        outNotOperation->AddParam(PARAM(TAL_LOGICAL, _OPERAND, 0), op->GetResultingTAR());

        auto newOutNode = _nodesList.size(); //AddNode(outNotOperation);
        _nodesList.push_back(newOutNode);
        nodesToAdd[newOutNode] = outNotOperation;

        outNotOperation->SetResultingTAR(parser->InferOutputTARSchema(outNotOperation));
        outNotOperation->GetResultingTAR()->AlterTAR(UNSAVED_ID, "ATAR_"+to_string(counter++));

        for(auto& edge : _edges){
          if(edge.origin == node){
            edge.origin = newOutNode;
            edge.tar = outNotOperation->GetResultingTAR();
          }
        }

        QueryGraphEdge newInEdge(node, newOutNode, op->GetResultingTAR(), PARAM(TAL_LOGICAL, _OPERAND, 0));
        edgesToAdd.push_back(newInEdge);
        QueryGraphEdge newInputTAREdge(parentNode, newOutNode,parentInputTar, PARAM(TAL_LOGICAL, _INPUT_TAR));
        edgesToAdd.push_back(newInputTAREdge);

        for(auto& edge : _edges){
          if(edge.destiny == node && edge.label != PARAM(TAL_LOGICAL, _INPUT_TAR)){
            auto notOperation = make_shared<Operation>(TAL_LOGICAL);
            notOperation->AddParam(PARAM(TAL_LOGICAL, _INPUT_TAR), intputTAR);
            notOperation->AddParam(PARAM(TAL_LOGICAL, _OPERATOR), string(_NOT));
            notOperation->AddParam(PARAM(TAL_LOGICAL, _OPERAND, 0), edge.tar);
            notOperation->SetResultingTAR(parser->InferOutputTARSchema(notOperation));
            notOperation->GetResultingTAR()->AlterTAR(UNSAVED_ID, "ATAR_"+to_string(counter++));

            auto newNode = _nodesList.size(); //AddNode(outNotOperation);
            _nodesList.push_back(newNode);
            nodesToAdd[newNode] = notOperation;
            edge.destiny = newNode;

            QueryGraphEdge newEdge(newNode, node, notOperation->GetResultingTAR(), edge.label);
            edge.label = PARAM(TAL_LOGICAL, _OPERAND, 0);
            edgesToAdd.push_back(newEdge);

            QueryGraphEdge newInputEdge(parentNode, newNode, parentInputTar, PARAM(TAL_LOGICAL, _INPUT_TAR));
            edgesToAdd.push_back(newInputEdge);

          }
        }

        for(const auto& edge : edgesToAdd){
          _edges.push_back(edge);
        }
      }
    }
  }

  for(const auto& entry : nodesToAdd){
    _unOptimizedNodesList.push_back(entry.first);
    _nodes[entry.first] = entry.second;
  }
}

void QueryGraph::SplitConjuctions(ParserPtr parser) {

  auto counter = 0ul;

  while(true) {

    bool notFound = true;

    for (const auto &entry : _nodes) {

      auto node = entry.first;
      auto op = entry.second;

      if (op->GetOperation() == TAL_LOGICAL) {
        if (op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _AND) {

          bool isAndBeforeFilter = false;
          int64_t filterNode = INVALID_QUERY_GRAPH_NODE;
          for(const auto& edge : _edges){
            if(edge.origin == node){
              if(_nodes.find(edge.destiny) != _nodes.end()){
                if(_nodes[edge.destiny]->GetOperation() == TAL_FILTER){
                  filterNode = edge.destiny;
                  isAndBeforeFilter = true;
                  notFound = false;
                  break;
                }
              }
            }
          }

          if(!isAndBeforeFilter) continue;

          int64_t op1Node = INVALID_QUERY_GRAPH_NODE;
          int64_t op2Node = INVALID_QUERY_GRAPH_NODE;
          TARPtr op2TAR = nullptr;
          for(auto& edge : _edges){
            if(edge.destiny == node && edge.label == PARAM(TAL_LOGICAL, _OPERAND, 0)){
              op1Node = edge.origin;
              edge.label = PARAM(TAL_FILTER, _AUX_TAR);
            }

            if(edge.destiny == node && edge.label == PARAM(TAL_LOGICAL, _OPERAND, 1)){
              op2Node = edge.origin;
              op2TAR = edge.tar;
            }
          }

          //turning AND into FILTER
          op->SetOperation(TAL_FILTER);
          auto op1Param = op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERAND, 0));
          op1Param->name = PARAM(TAL_FILTER, _AUX_TAR);
          auto op2Param = op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERAND, 1));
          auto& params = op->GetParameters();

          list<std::list<ParameterPtr>::iterator> it2Remove;
          for(auto it = params.begin(); it != params.end(); it++){
            if((*it)->name == PARAM(TAL_LOGICAL, _OPERAND, 1)
                || (*it)->name == PARAM(TAL_LOGICAL, _OPERATOR)){
              it2Remove.push_back(it);
            }
          }

          for(auto it : it2Remove)
            params.erase(it);

          for(const auto& edge : _edges){
            if(edge.origin == node){
              if(_nodes[edge.destiny] != nullptr){
                if(_nodes[edge.destiny]->GetOperation() == TAL_FILTER){
                  filterNode = edge.destiny;
                  notFound = false;
                  break;
                }
              }
            }
          }

          op->SetResultingTAR(parser->InferOutputTARSchema(op));
          op->GetResultingTAR()->AlterTAR(UNSAVED_ID, "ATAR_"+to_string(counter++));

          //adjusting second FILTER
          for(auto& edge : _edges){

            if(edge.origin == node && edge.destiny == filterNode){
              edge.tar = op->GetResultingTAR();
              edge.label = PARAM(TAL_FILTER, _INPUT_TAR);
            }

            if(edge.origin != node && edge.destiny == filterNode){
              edge.tar = op2TAR;
              edge.origin = op2Node;
              edge.label = PARAM(TAL_FILTER, _AUX_TAR);
            }
          }

          for(auto it = _edges.begin(); it != _edges.end(); it++){
            if((*it).origin == op2Node && (*it).destiny == node){
              _edges.erase(it);
              break;
            }
          }

          list<int64_t> nodesToChangeInput;
          list<int64_t> openNodes;
          openNodes.push_back(op2Node);
          while(!openNodes.empty()){
            auto nextNode = openNodes.front();
            openNodes.pop_front();
            nodesToChangeInput.push_back(nextNode);

            for(auto& edge: _edges){
              if(edge.destiny == nextNode
                  && (edge.label == PARAM(TAL_COMPARISON, _INPUT_TAR)
                  || edge.label == PARAM(TAL_LOGICAL, _INPUT_TAR)) ){

                edge.origin = node;
                edge.tar = op->GetResultingTAR();
              }

              if(edge.destiny == nextNode){
                if(_nodes.find(edge.origin) != _nodes.end()){
                  auto opCode =_nodes[edge.origin]->GetOperation();
                  if(opCode == TAL_LOGICAL || opCode == TAL_COMPARISON){
                    openNodes.push_back(edge.origin);
                  }
                }
              }
            }
          }
        }
      }
    }

    if(notFound)
      break;
  }
}

void QueryGraph::SimplifyNots(ParserPtr parse) {

  map<string, string> invertedOp = {{_EQ, _NEQ}, {_NEQ, _EQ}, {_LE, _GEQ}, {_GEQ, _LE},
                                    {_GE, _LEQ}, {_LEQ, _GE}};
  while(true) {

    list<int64_t> listNodesToRemove;

    for (const auto &entry : _nodes) {
      auto node = entry.first;
      if(entry.second->GetOperation() == TAL_LOGICAL){
        auto op = entry.second->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str;
        if(op == _NOT){
          for(const auto& edge : _edges){
            if(edge.destiny == node && _nodes.find(edge.origin) != _nodes.end()){

              if(_nodes[edge.origin]->GetOperation() == TAL_COMPARISON){
                auto parentOp = _nodes[edge.origin]->GetParametersByName(PARAM(TAL_COMPARISON, _OPERATOR));
                parentOp->literal_str = invertedOp[parentOp->literal_str];
                listNodesToRemove.push_back(node);
              }

              if(_nodes[edge.origin]->GetOperation() == TAL_LOGICAL){
                auto parentOp = entry.second->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str;
                if(parentOp == _NOT) {
                  listNodesToRemove.push_back(node);
                  listNodesToRemove.push_back(edge.origin);
                }
              }
            }
          }
        }
      }
    }

    if(listNodesToRemove.empty())
      break;

    for(auto node : listNodesToRemove){
      int64_t origin = INVALID_QUERY_GRAPH_NODE;

      for(const auto& edge : _edges){
        if(edge.destiny == node && edge.label == PARAM(TAL_LOGICAL, _OPERAND, 0 )) {
          origin = edge.origin;
          break;
        }
      }

      for(auto& edge : _edges){
        if(edge.origin == node) {
          edge.origin = origin;
        }
      }

      _nodes.erase(node);
      for(auto it = _nodesList.begin(); it != _nodesList.end(); it++){
        if((*it) == node){
          _nodesList.erase(it);
          break;
        }
      }

      for(auto it = _unOptimizedNodesList.begin(); it != _unOptimizedNodesList.end(); it++){
        if((*it) == node){
          _unOptimizedNodesList.erase(it);
          break;
        }
      }

      list<list<QueryGraphEdge>::iterator> edgesToRemove;

      for(auto it = _edges.begin(); it != _edges.end(); it++){
        if((*it).origin == node || (*it).destiny == node){
          edgesToRemove.push_back(it);
        }
      }

      for(auto it : edgesToRemove){
        _edges.erase(it);
      }

      int64_t maxNode = 0;
      int64_t maxPosition = 0;
      for(int64_t i = 0; i < _nodesList.size(); i++){
        if(_nodesList[i] > maxNode) {
          maxNode = _nodesList[i];
          maxPosition = i;
        }
      }

      _unOptimizedNodesList.pop_back();
      _nodesList[maxPosition] = node;
      _nodes[node] = _nodes[maxNode];
      _nodes.erase(maxNode);

      for(auto& edge : _edges){
        if(edge.origin == maxNode)
          edge.origin = node;

        if(edge.destiny == maxNode)
          edge.destiny = node;
      }

    }
  }
}


OperationPtr QueryGraph::GetOriginalOperator(int64_t node){
  return _nodes[node];
}

OperationPtr QueryGraph::GetOperator(int64_t node) {

  OperationPtr oldOp = _nodes[node];

  if(oldOp == nullptr) return nullptr;

  OperationPtr newOp = make_shared<Operation>(oldOp->GetOperation());
  unordered_map<string, TARPtr> tars;

  for(const auto& edge : _edges){
    if(edge.destiny == node){
      tars[edge.label] = edge.tar;
    }

    if(edge.origin == node){
      newOp->SetResultingTAR(edge.tar);
    }
  }

  for(auto const& param : oldOp->GetParameters()){
    switch(param->type){
      case TAR_PARAM:
        if(tars.find(param->name) == tars.end()) {
          int a = 2;
          int b = a*10;
        }
        newOp->AddParam(param->name, tars[param->name]); break;
      case IDENTIFIER_PARAM: newOp->AddIdentifierParam(param->name, param->literal_str); break;
      case LITERAL_FLOAT_PARAM: newOp->AddParam(param->name, param->literal_flt); break;
      case LITERAL_DOUBLE_PARAM: newOp->AddParam(param->name, param->literal_dbl); break;
      case LITERAL_INT_PARAM: newOp->AddParam(param->name, param->literal_int); break;
      case LITERAL_LONG_PARAM: newOp->AddParam(param->name, param->literal_lng); break;
      case LITERAL_STRING_PARAM: newOp->AddParam(param->name, param->literal_str); break;
      case LITERAL_BOOLEAN_PARAM: newOp->AddParam(param->name, param->literal_bool); break;
    }
  }

  return newOp;
}

int64_t QueryGraph::DuplicateNode(int64_t node, bool cloneOutput) {

  int64_t newNode = _nodesList.size();
  _nodesList.push_back(newNode);
  _unOptimizedNodesList.push_back(newNode);

  auto op = _nodes[node];
  OperationPtr newOp = make_shared<Operation>(op->GetOperation());

  for(const auto& param : op->GetParameters()){
    newOp->AddParam(param);
  }

  newOp->SetResultingTAR(op->GetResultingTAR());

  _nodes[newNode] = newOp;

  list<QueryGraphEdge> nodeEdges;

  for(const auto &edge : _edges){
    if(edge.destiny == node){
      QueryGraphEdge newEdge(edge.origin, newNode, edge.tar, edge.label);
      nodeEdges.push_back(newEdge);
    }

    if(cloneOutput && edge.origin == node){
      QueryGraphEdge newEdge(newNode, edge.destiny, edge.tar, edge.label);
      nodeEdges.push_back(newEdge);
    }
  }

  for(const auto &newEdge : nodeEdges){
    _edges.push_back(newEdge);
  }

  return newNode;
}

QueryPlanPtr QueryGraph::ToQueryPlan(ParserPtr parser) {
  QueryPlanPtr queryPlan = make_shared<QueryPlan>();
  queryPlan->SetType(DML);
  auto sortedNodes = GetSortedNodes();
  int32_t idCounter = 1;


  for(const auto& node : sortedNodes){
    auto op = GetOperator(node);
    if(op == nullptr) continue;

    auto tar = parser->InferOutputTARSchema(op);

    for(auto& edge : _edges){
      if(edge.origin == node){
        edge.tar = tar;
      }
    }

    op->SetResultingTAR(tar);
    queryPlan->AddOperation(op, idCounter);
  }

  return queryPlan;
}

QueryGraphPtr QueryGraph::Clone() {

  QueryGraphPtr clonedGraph = make_shared<QueryGraph>();
  clonedGraph->_nodesList.insert(clonedGraph->_nodesList.end(),
                                 _nodesList.begin(), _nodesList.end());

  clonedGraph->_unOptimizedNodesList.insert(clonedGraph->_unOptimizedNodesList.end(),
                                            _unOptimizedNodesList.begin(), _unOptimizedNodesList.end());


  for(const auto& entry : _nodes){
    clonedGraph->_nodes[entry.first] = entry.second;
  }

  clonedGraph->_edges.insert(clonedGraph->_edges.end(),
                             _edges.begin(), _edges.end());

  return clonedGraph;
}

string QueryGraph::toDBGString() {

  string dotGraph = "digraph SavimeQueryGraph {\n";

  for(const auto& node : _nodesList ){
    dotGraph += std::to_string(node) +" [label = \""+to_string(node)+" - "+split(_nodes[node]->toString(), '=')[1]+"\"];\n";
  }

  for(const auto& _edge : _edges ){
    dotGraph += std::to_string(_edge.origin) + " -> "+std::to_string(_edge.destiny) +" [label = \""+ _edge.tar->GetName()+"\"];\n";
  }

  dotGraph +="}\n";

  return dotGraph;
}