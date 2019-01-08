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

#include "../catch2/catch.hpp"
#include "../mock_builder.h"
#include "test/test_queries_creator.h"

#include "../../optimizer/default_optimizer.h"

TEST_CASE("Optimizer", "[Optimizer]") {

  //auto builder = new MockModulesBuilder();
  //auto optimizer = builder->BuildOptimizer();
  //auto parser = builder->BuildParser();
  //auto queryDataManager = builder->BuildQueryDataManager();
  //builder->RunBootQueries(SETUP_QUERIES);


  //queryDataManager->AddQueryTextPart(TEST_QUERIES[0]);
  //parser->Parse(queryDataManager);

  //printf("%s\n", queryDataManager->GetQueryPlan()->toString().c_str());

  //QueryGraphPtr queryGraph = make_shared<QueryGraph>(queryDataManager->GetQueryPlan());
  //queryGraph->PushDown(queryDataManager->GetQueryPlan()->GetOperations().front(), INPUT_TAR,
  //                     queryDataManager->GetQueryPlan()->GetOperations().back(), INPUT_TAR);


  //printf("%s\n", queryDataManager->GetQueryPlan()->toString().c_str());
}
