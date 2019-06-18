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

/*
TEST_CASE("CreateTestQueries", "[Parser]") {
  get_parse_results();
}
*/

TEST_CASE("Parser", "[Parser]") {

  auto builder = new MockModulesBuilder();
  auto parser = builder->BuildParser();
  auto queryDataManager = builder->BuildQueryDataManager();
  builder->RunBootQueries(SETUP_QUERIES);

  for(int64_t i = 0; i <  TEST_QUERIES.size(); i++) {
    queryDataManager->AddQueryTextPart(TEST_QUERIES[i]);



    parser->Parse(queryDataManager);
    string queryPlanStr;
    if(queryDataManager->GetQueryPlan() != nullptr){
      queryPlanStr = queryDataManager->GetQueryPlan()->toString();
    }

    queryPlanStr.erase(std::remove(queryPlanStr.begin(),
                       queryPlanStr.end(), '\n'), queryPlanStr.end());


    REQUIRE(queryPlanStr == TEST_QUERY_PLANS[i]);
    REQUIRE(queryDataManager->GetErrorResponse() == TEST_ERROR_RESPONSES[i]);
    queryDataManager->Release();
  }
  
  delete builder;
}

