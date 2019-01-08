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
#include "mock_query_data.h"
#include "../catch2/catch.hpp"
#include "../mock_builder.h"
#include "../test_utils.h"

TEST_CASE("Test MetadataManager", "[QueryDataManager]") {
  auto builder = new MockModulesBuilder();
  auto queryData = builder->BuildQueryDataManager();
  auto storageManager = builder->BuildStorageManager();
  DatasetComparator datasetComparator(storageManager);
  MetadataComparator metadataComparator(storageManager);
  QueryStrucuturesComparator queryStructureComparator(storageManager,
                                                      datasetComparator,
                                                      metadataComparator);

  SECTION("Testing error message storage.") {
    string expectedMsg = "error";
    queryData->SetErrorResponseText(expectedMsg);
    auto result = queryData->GetErrorResponse();
    REQUIRE(result == expectedMsg);
  }

  SECTION("Testing id storage.") {
    int32_t expectedId = 0x42;
    queryData->SetQueryId(expectedId);
    auto result = queryData->GetQueryId();
    REQUIRE(result == expectedId);
  }

  SECTION("Testing response text storage.") {
    string expectedMsg = "response text";
    queryData->SetQueryResponseText(expectedMsg);
    auto result = queryData->GetQueryResponseText();
    REQUIRE(result == expectedMsg);
  }

  SECTION("Testing query text storage.") {
    string query1 = "SELECT(WHERE(JOIN(A,B), A==2), A.a, A.b);\n";
    string query2 = "SELECT(DERIVE(JOIN(A,B), A==2), A.a, A.b);\n";
    string query3 = "SELECT(CROSS(JOIN(A,B), A==2), A.a, A.b);\n";
    string query4 = "SELECT(IFF(JOIN(A,B), A==2), A.a, A.b);\n";

    queryData->AddQueryTextPart(query1);
    queryData->AddQueryTextPart(query2);
    queryData->AddQueryTextPart(query3);
    queryData->AddQueryTextPart(query4);

    auto result = queryData->GetQueryText();
    REQUIRE(result == query1+query2+query3+query4);
  }

  SECTION("Testing query text storage.") {
    string paramName = "param1";
    int32_t fd1 = queryData->GetParamFile(paramName);
    int32_t fd2 = queryData->GetParamFile(paramName);
    REQUIRE(fd1 == fd2);

    auto file1 = queryData->GetParamFilePath(paramName);
    auto file2 = queryData->GetParamFilePath(paramName);
    REQUIRE(file1 == file2);

    queryData->RemoveParamFile(paramName);
    file1 = queryData->GetParamFilePath(paramName);
    REQUIRE(file1.empty());
  }

  SECTION("Testing queryplan storage.") {

    QueryPlanPtr expected = make_shared<QueryPlan>();
    expected->SetType(QueryType::DDL);

    auto op1 = make_shared<Operation>(OperationCode::TAL_COMPARISON);
    op1->AddParam("a", 10.0f);
    op1->AddParam("b", 115.0f);

    auto op2 = make_shared<Operation>(OperationCode::TAL_DIMJOIN);
    op1->AddParam("c", true);
    op1->AddParam("d", "foo");

    expected->AddOperation(op1);
    expected->AddOperation(op2);

    queryData->SetQueryPlan(expected);
    auto result = queryData->GetQueryPlan();
    REQUIRE(queryStructureComparator.Compare(expected, result, 0));
  }

  delete builder;
}