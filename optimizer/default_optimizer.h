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
#ifndef DEFAULT_OPTIMIZER_H
#define DEFAULT_OPTIMIZER_H

#include <unordered_map>
#include "../core/include/optimizer.h"

//Coeficients Table
//----------------------------------------------------------------------------------------------------------------------
extern unordered_map<OperationCode, vector<double>> COEFICIENTS;

//Optimizer declarations
//----------------------------------------------------------------------------------------------------------------------
#define INVALID_QUERY_GRAPH_NODE -1
#define END_POINT -2

class QueryGraph;
typedef std::shared_ptr<QueryGraph> QueryGraphPtr;

struct PushDownOperation {
  int64_t parent;
  string parentInpurTarParamName;
  int64_t child;
  string childInpurTarParamName;

  PushDownOperation(int64_t parent, string parentParamName, int64_t child, string childParamName){
    this->parent = parent;
    this->parentInpurTarParamName = std::move(parentParamName);
    this->child = child;
    this->childInpurTarParamName = std::move(childParamName);
  }
};
typedef std::shared_ptr<PushDownOperation> PushDownOperationPtr;

struct QueryGraphEdge {

  int64_t origin = 0;
  int64_t destiny = 0;
  TARPtr tar;
  string label;

  QueryGraphEdge() = default;
  QueryGraphEdge(int64_t origin, int64_t destiny, TARPtr tar, string label) {
    this->origin = origin;
    this->destiny = destiny;
    this->tar = std::move(tar);
    this->label = std::move(label);
  }
};

class QueryGraph {

  vector<int64_t> _nodesList;
  list<int64_t> _unOptimizedNodesList;
  unordered_map<int64_t, OperationPtr> _nodes;
  list<QueryGraphEdge> _edges;

public:

  void Create(QueryPlanPtr queryPlan);
  int64_t AddNode(OperationPtr op);
  list<int64_t> GetChildren(int64_t node);
  list<int64_t> GetParents(int64_t node);
  list<int64_t> GetParents(int64_t node, string label);
  void PushDown(int64_t child, int64_t parent, string childLabel, string parentLabel);
  list<TARPtr> GetInputTARS(int64_t node);
  TARPtr GetOutputTAR(int64_t node);
  list<int64_t>& GetUnoptimizedNodes();
  vector<int64_t>& GetNodes();
  list<QueryGraphEdge>& GetEdges();
  list<int64_t> GetFilterSubtree(int64_t node);
  list<int64_t> GetSortedNodes(); //Sorted in topological order
  void FlipDisjuctions(ParserPtr parser); //Turns ors into ands
  void SplitConjuctions(ParserPtr parse);
  void SimplifyNots(ParserPtr parse);
  OperationPtr GetOperator(int64_t node);
  OperationPtr GetOriginalOperator(int64_t node);
  int64_t DuplicateNode(int64_t node, bool cloneOutput = false);
  QueryPlanPtr ToQueryPlan(ParserPtr parser);
  QueryGraphPtr Clone();
  string toDBGString();
};
typedef std::shared_ptr<QueryGraph> QueryGraphPtr;

class DefaultOptimizer : public Optimizer {

    MetadataManagerPtr _metadataManager;
    ParserPtr _parser;

    double GetQueryCost(QueryGraphPtr queryGraph);
    list<PushDownOperationPtr> OptimizeSelect(int64_t node, OperationPtr op, QueryGraphPtr queryGraph);
    list<PushDownOperationPtr> OptimizeFilter(int64_t node, OperationPtr op, QueryGraphPtr queryGraph);
    list<PushDownOperationPtr> OptimizeSubset(int64_t node, OperationPtr op, QueryGraphPtr queryGraph);
    list<PushDownOperationPtr> OptimizeArithmetic(int64_t node, OperationPtr op, QueryGraphPtr queryGraph);
    list<PushDownOperationPtr> OptimizeSlice(int64_t node, OperationPtr op, QueryGraphPtr queryGraph);
    void OptimizeNode(int64_t node, QueryGraphPtr queryGraph, QueryPlanPtr queryPlan);

public:
    DefaultOptimizer(ConfigurationManagerPtr configurationManager,
                     SystemLoggerPtr systemLogger, MetadataManagerPtr metadataManager)
      : Optimizer(configurationManager, systemLogger, metadataManager) {
      _metadataManager = metadataManager;
    }

    void SetParser(ParserPtr parser){
      _parser = parser;
    }

    SavimeResult Optimize(QueryDataManagerPtr queryDataManager);
};

//Schema Dependency Declarations
//----------------------------------------------------------------------------------------------------------------------

class SchemaDependencyChecker{

  list<DataElementPtr> GetSelectDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetFilterDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetSubsetDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetLogicalDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetComparisonDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetArithmeticDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetCrossDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetEquiJoinDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetDimJoinDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetSliceDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetAtt2DimDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetAgreggateDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetUnionDependencies(QueryGraphPtr graph, int64_t node);
  list<DataElementPtr> GetTranslateDependencies(QueryGraphPtr graph, int64_t node);

public:
  list<DataElementPtr> GetDependencies(QueryGraphPtr graph, int64_t node);
};

struct TARFeatures {
  int64_t dimensions;
  int64_t attributes;
  int64_t subtars;
  int64_t avgSubtarsLen;
};

//Operation Splitter
//----------------------------------------------------------------------------------------------------------------------
struct SplitNodes {
    int64_t leftNode;
    int64_t rightNode;
};

class OperationSplitter {

  void RemovePrefix(QueryGraphPtr queryGraph, int64_t node, string prefix,
                    bool deleteRemaining, ParameterType typeToRemove = IDENTIFIER_PARAM);
  SplitNodes SplitSelectOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitSelectOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitFilterOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitFilterOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitSubsetOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitSubsetOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitArithmeticOverCross(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
  SplitNodes SplitArithmeticOverDimJoin(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
public:
  SplitNodes Split(QueryGraphPtr queryGraph, int64_t child, int64_t parent);
};

//QueryCostCalculator Declarations
//----------------------------------------------------------------------------------------------------------------------
class SelectivityEstimator {
public:
    double Estimate(QueryGraphPtr graph, int64_t node);
};

class QueryCostCalculator{

  QueryGraphPtr _graph;
  unordered_map<string, TARFeatures> _features;
  unordered_map<string, OperationPtr> _operations;

  void DefaultEstimation(int64_t node, string inputTarName, double factor);
  TARFeatures CalculateBaseCost(TARPtr tar, MetadataManagerPtr metadataManager);
  void CalculateScanCost(int64_t node);
  void CalculateSelecetCost(int64_t node);
  void CalculateFilterCost(int64_t node);
  void CalculateSubsetCost(int64_t node);
  void CalculateComparisonCost(int64_t node);
  void CalculateLogicalCost(int64_t node);
  void CalculateArithmeticCost(int64_t node);
  void CalculateCrossCost(int64_t node);
  void CalculateEquiJoinCost(int64_t node);
  void CalculatedimJoinCost(int64_t node);
  void CalculateSliceCost(int64_t node);
  void CalculateAggregateCost(int64_t node);
  void CalculateUnionCost(int64_t node);
  void CalculateTranslateCost(int64_t node);

public:
  double CalculateCost(QueryGraphPtr graph, MetadataManagerPtr metadataManager);
};


inline bool CHECK_DEPEDENCIES(const TARPtr &resultingTAR, list<DataElementPtr> dataElements){

  bool containsAll = true;

  for(const auto& de : dataElements){
    containsAll &= resultingTAR->HasDataElement(de->GetName());
  }

  return containsAll;
}

inline bool CHECK_ALTERNATIVE_PATH(int64_t node, int64_t parentNode,
                                   string childLabel, string parentLabel,
                                   QueryGraphPtr queryGraph,
                                   MetadataManagerPtr metadataManager){
  QueryCostCalculator costCalculator;
  auto optionalPathGraph = queryGraph->Clone();
  optionalPathGraph->PushDown(node, parentNode, std::move(parentLabel), std::move(childLabel));
  auto optionalPathCost = costCalculator.CalculateCost(optionalPathGraph, metadataManager);
  auto currentCost = costCalculator.CalculateCost(queryGraph, metadataManager);
  return optionalPathCost < currentCost;
}


inline bool CHECK_LEFT_RIGHT_DEPENDENCIES(list<DataElementPtr> dataElements){

  map<string, string> prefixes;

  for(const auto& de : dataElements){
    auto splitName = split(de->GetName(), _UNDERSCORE[0]);
    prefixes[splitName[0]+_UNDERSCORE] = splitName[0]+_UNDERSCORE;
  }

  bool hasboth = prefixes.find(LEFT_DATAELEMENT_PREFIX) != prefixes.end()
                && prefixes.find(RIGHT_DATAELEMENT_PREFIX) != prefixes.end();

  return !hasboth;
}

#endif /* DEFAULT_OPTIMIZER_H */

