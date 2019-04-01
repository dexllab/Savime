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

double SelectivityEstimator::Estimate(QueryGraphPtr graph, int64_t node) {
  auto op = graph->GetOperator(node);

  switch(op->GetOperation()){
    case TAL_FILTER: {

      auto parents = graph->GetParents(node);
      for (auto parent : parents) {
        auto opParent = graph->GetOperator(parent)->GetOperation();

        if (opParent == TAL_LOGICAL || opParent == TAL_COMPARISON) {
          return Estimate(graph, parent);
        }
      }
    }break;
    case TAL_LOGICAL: {

      auto parents = graph->GetParents(node);
      vector<double> estimation;
      for(auto parent : parents){
        auto opParent = graph->GetOperator(parent)->GetOperation();

        if(opParent == TAL_LOGICAL || opParent == TAL_COMPARISON){
          estimation.push_back(Estimate(graph, parent));
        }
      }

      if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _NOT) {
        return  1 - estimation[0];
      } else if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _AND) {
        return estimation[0]*estimation[1];
      } else if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _OR) {
        return std::min(estimation[0]+estimation[1], 1.0);
      }

    } break;
    case TAL_COMPARISON: {

      if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _EQ) {
        return  0.05;
      } else if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _LE
                || op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _LEQ
                || op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _GE
                || op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _GEQ) {
        return  0.5;
      } else if(op->GetParametersByName(PARAM(TAL_LOGICAL, _OPERATOR))->literal_str == _NEQ) {
        return  0.95;
      } else {
        return 1.0;
      }
    }
  }
  return 1.0;
}

//----------------------------------------------------------------------------------------------------------------------
/*unordered_map<OperationCode, vector<double>> COEFICIENTS = { {TAL_SCAN, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_SELECT, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_FILTER, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_SUBSET, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_LOGICAL, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_COMPARISON, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_ARITHMETIC, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_CROSS, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_EQUIJOIN, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_DIMJOIN, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_SLICE, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_ATT2DIM, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_AGGREGATE, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_UNION, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_TRANSLATE, {1.0, 1.0, 1.0, 1.0}},
                                                              {TAL_USER_DEFINED, {1.0, 1.0, 1.0, 1.0}}};
*/
void QueryCostCalculator::DefaultEstimation(int64_t node, string inputTarName, double factor) {
  auto op = _graph->GetOperator(node);

  TARPtr inputTar = nullptr;
  for(const auto& tar : _graph->GetInputTARS(node)) {
    if(tar->GetName() == inputTarName)
      inputTar = tar;
  }

  auto outputTar = _graph->GetOutputTAR(node);
  if(outputTar == nullptr)
    return;

  TARFeatures features{};
  auto inputFeatures = _features[inputTar->GetName()];
  features.avgSubtarsLen = static_cast<int64_t>(inputFeatures.avgSubtarsLen * factor);
  features.subtars =  static_cast<int64_t>(inputFeatures.subtars*factor);
  features.attributes = outputTar->GetAttributes().size();
  features.dimensions = outputTar->GetDimensions().size();
  _operations[outputTar->GetName()] = op;
  _features[outputTar->GetName()] = features;
}

TARFeatures QueryCostCalculator::CalculateBaseCost(TARPtr tar, MetadataManagerPtr metadataManager){

  TARFeatures features{};

  auto subtars = metadataManager->GetSubtars(tar);
  int64_t sum = 0;
  int64_t numSubtars = subtars.size();

  for(const auto &subtar : subtars){
    sum += subtar->GetFilledLength();
  }

  features.attributes = tar->GetAttributes().size();
  features.dimensions = tar->GetDimensions().size();
  features.subtars = numSubtars;
  features.avgSubtarsLen = static_cast<int64_t>((double)sum / (double)numSubtars);

  return features;
}

void QueryCostCalculator::CalculateScanCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_SELECT, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateSelecetCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_SELECT, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateFilterCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_FILTER, _INPUT_TAR))->tar->GetName();
  SelectivityEstimator estimator;
  double factor = estimator.Estimate(_graph, node);
  DefaultEstimation(node, name, factor);
}

void QueryCostCalculator::CalculateSubsetCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto inputTar = _graph->GetInputTARS(node).front();
  auto outputTar = _graph->GetOutputTAR(node);
  auto factor = (double)outputTar->GetSpannedTARLen()/(double)inputTar->GetSpannedTARLen();
  auto name = op->GetParametersByName(PARAM(TAL_SUBSET, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, factor);
}

void QueryCostCalculator::CalculateLogicalCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_LOGICAL, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateComparisonCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_COMPARISON, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateArithmeticCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_ARITHMETIC, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateCrossCost(int64_t node) {
  auto op = _graph->GetOperator(node);

  auto nameLeft = op->GetParametersByName(PARAM(TAL_CROSS, _OPERAND, 0))->tar->GetName();
  auto nameRight = op->GetParametersByName(PARAM(TAL_CROSS, _OPERAND, 1))->tar->GetName();

  TARPtr leftTar = nullptr;
  TARPtr rightTar = nullptr;

  for(const auto& tar : _graph->GetInputTARS(node)) {
    if(tar->GetName() == nameLeft)
      leftTar = tar;
    if(tar->GetName() == nameRight)
      rightTar = tar;
  }

  auto outputTar = _graph->GetOutputTAR(node);
  TARFeatures features{};
  auto leftFeatures = _features[leftTar->GetName()];
  auto rightFeatures = _features[rightTar->GetName()];

  features.avgSubtarsLen = leftFeatures.avgSubtarsLen*rightFeatures.avgSubtarsLen;
  features.subtars =  leftFeatures.subtars*rightFeatures.subtars;
  features.attributes = outputTar->GetAttributes().size();
  features.dimensions = outputTar->GetDimensions().size();
  _operations[outputTar->GetName()] = op;
  _features[outputTar->GetName()] = features;
}

void QueryCostCalculator::CalculateEquiJoinCost(int64_t node) {

}

void QueryCostCalculator::CalculatedimJoinCost(int64_t node) {
  auto op = _graph->GetOperator(node);

  auto nameLeft = op->GetParametersByName(PARAM(TAL_CROSS, _OPERAND, 0))->tar->GetName();
  auto nameRight = op->GetParametersByName(PARAM(TAL_CROSS, _OPERAND, 1))->tar->GetName();

  TARPtr leftTar = nullptr;
  TARPtr rightTar = nullptr;

  for(const auto& tar : _graph->GetInputTARS(node)) {
    if(tar->GetName() == nameLeft)
      leftTar = tar;
    if(tar->GetName() == nameRight)
      rightTar = tar;
  }

  auto outputTar = _graph->GetOutputTAR(node);
  TARFeatures features{};
  auto leftFeatures = _features[leftTar->GetName()];
  auto rightFeatures = _features[rightTar->GetName()];
  auto factor = (double)outputTar->GetSpannedTARLen()/(double)leftTar->GetSpannedTARLen();

  features.avgSubtarsLen = static_cast<int64_t>(((double)leftFeatures.avgSubtarsLen) * factor);
  features.subtars = static_cast<int64_t>(((double)leftFeatures.subtars) * factor);
  features.attributes = outputTar->GetAttributes().size();
  features.dimensions = outputTar->GetDimensions().size();
  _operations[outputTar->GetName()] = op;
  _features[outputTar->GetName()] = features;
}

void QueryCostCalculator::CalculateSliceCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_SLICE, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateAggregateCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_AGGREGATE, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateUnionCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_UNION, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

void QueryCostCalculator::CalculateTranslateCost(int64_t node) {
  auto op = _graph->GetOperator(node);
  auto name = op->GetParametersByName(PARAM(TAL_TRANSLATE, _INPUT_TAR))->tar->GetName();
  DefaultEstimation(node, name, 1.0);
}

double QueryCostCalculator::CalculateCost(QueryGraphPtr graph, MetadataManagerPtr metadataManager) {

  _graph = graph;
  auto edges = graph->GetEdges();

  for(auto edge : edges){
    if(edge.origin == INVALID_QUERY_GRAPH_NODE){
      auto features = CalculateBaseCost(edge.tar, metadataManager);
      _features[edge.tar->GetName()] = features;
    }
  }

  auto nodes = graph->GetSortedNodes();
  for(auto node : nodes){

    auto op = _graph->GetOperator(node);

    switch(op->GetOperation()){
      case TAL_SCAN: CalculateScanCost(node); break;
      case TAL_SELECT: CalculateSelecetCost(node); break;
      case TAL_FILTER: CalculateFilterCost(node); break;
      case TAL_SUBSET: CalculateSubsetCost(node); break;
      case TAL_LOGICAL: CalculateLogicalCost(node); break;
      case TAL_COMPARISON: CalculateComparisonCost(node); break;
      case TAL_ARITHMETIC: CalculateArithmeticCost(node); break;
      case TAL_CROSS: CalculateCrossCost(node); break;
      case TAL_EQUIJOIN: CalculateEquiJoinCost(node); break;
      case TAL_DIMJOIN: CalculatedimJoinCost(node); break;
      case TAL_SLICE: CalculateSliceCost(node); break;
      case TAL_AGGREGATE: CalculateAggregateCost(node); break;
      case TAL_UNION: CalculateUnionCost(node); break;
      case TAL_TRANSLATE: CalculateTranslateCost(node); break;
    }
  }

  double cost = 0.0;
  for(auto entry : _operations){

    auto features = _features[entry.first];
    auto op = entry.second->GetOperation();

    /*
     * cost += COEFICIENTS[op][0]*features.avgSubtarsLen
            + COEFICIENTS[op][1]*features.subtars
            + COEFICIENTS[op][2]*features.dimensions
            + COEFICIENTS[op][3]*features.attributes;
    */
    cost += features.avgSubtarsLen + features.subtars
            + features.dimensions + features.attributes;

  }

  return cost;
}

