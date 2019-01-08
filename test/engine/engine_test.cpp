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

#include <iomanip>
#include "../catch2/catch.hpp"
#include "../query/mock_query_data.h"
#include "mock_engine.h"
#include "test/test_queries_creator.h"


/*TEST_CASE("EngineResultGeneration", "[Engine]") {
  auto builder = new MockModulesBuilder();
  auto parser = builder->BuildParser();
  auto storageManager = builder->BuildStorageManager();
  auto queryDataManager = builder->BuildQueryDataManager();
  auto engineListerner = new TestCaseMatcher(storageManager);
  auto optimizer = builder->BuildOptimizer();

  builder->RunBootQueries(SETUP_QUERIES);

  string literalResultsStr = _LEFT_CURLY_BRACKETS;
  for (int64_t i = 0; i < TEST_QUERIES.size(); i++) {

    std::cout << TEST_QUERIES[i] << _NEWLINE;
    queryDataManager->AddQueryTextPart(TEST_QUERIES[i]);
    engineListerner->StartNewQuery();
    parser->Parse(queryDataManager);


    auto engine = EnginePtr(
        new MockDefaultEngine(builder->BuildConfigurationManager(),
                              builder->BuildSystemLogger(),
                              builder->BuildMetadaManager(),
                              builder->BuildStorageManager()));

    ((DefaultEngine *) engine.get())->SetThisPtr(engine);

    if (queryDataManager->GetQueryPlan() != nullptr) {
 //     optimizer->Optimize(queryDataManager);
      engine->Run(queryDataManager, engineListerner);
      auto binaryString = engineListerner->GetBinaryString();
      //std::cout << binaryString << _NEWLINE;
      literalResultsStr += binaryString + _COMMA + _NEWLINE;

    } else {
      literalResultsStr +=
          _LEFT_CURLY_BRACKETS _RIGHT_CURLY_BRACKETS _COMMA _NEWLINE;
    }
    queryDataManager->Release();

  }
  literalResultsStr += _RIGHT_CURLY_BRACKETS;

  for(int64_t linePos = 80; linePos < literalResultsStr.size(); linePos+=80){
    literalResultsStr.insert(linePos,"\\\n");
  }

  std::ofstream out("/tmp/literalstrdata.data");
  out << literalResultsStr << _NEWLINE;
  out.close();
}
*/


void printCase(std::map<std::string, std::vector<std::vector<uint8_t>>> binData1,
               std::map<std::string, std::vector<std::vector<uint8_t>>> binData2,
               DataType type, string dataElement, int64_t i_error, int64_t j_error) {


  if(binData1.size() != binData2.size()) {
    std::cout << "Result has a different number of data elements, " << binData1.size() <<
                 " while " << binData2.size() << " were expected. "<< _NEWLINE;
    return;
  }

  bool hasAllExpected = true;
  for(const auto& entry : binData2) {
    hasAllExpected &= binData1.find(entry.first) != binData1.end();
  }

  if(!hasAllExpected){
    std::cout << "Result has a different set of data elements:" << _NEWLINE;
    std::cout << "Returned:" << _NEWLINE;
    for(const auto& entry : binData1){
      std::cout << entry.first << "  ";
    }
    std::cout << "Expected:" << _NEWLINE;
    for(const auto& entry : binData2){
      std::cout << entry.first << "  ";
    }
    return;
  }

  auto sd1 = binData1[dataElement];
  auto sd2 = binData2[dataElement];

  if(sd1.size() != sd2.size()) {
    std::cout << "Result has a different number of  subtars, " << sd1.size() <<
              " while " << sd2.size() << " were expected. "<< _NEWLINE;
    return;
  }

  auto d1 = binData1[dataElement][i_error];
  auto d2 = binData2[dataElement][i_error];
  auto tsize = TYPE_SIZE(type);
  auto len = d1.size() / tsize;

  std::cout << " Values are different or are in different order: " << _NEWLINE;
  std::cout << " Obtained         |      Expected " << _NEWLINE;
  std::cout << " -------------------------------- " << _NEWLINE;

  for (int64_t i = 0; i < len; i++) {
    switch (type.type()) {
      case INT32:
        std::cout << ((int32_t *) d1.data())[i] << " - " << ((int32_t *) d2.data())[i] << _NEWLINE;
        break;
      case REAL_INDEX :
      case SUBTAR_POSITION :
      case INT64:
        std::cout << ((int64_t *) d1.data())[i] << " - " << ((int64_t *) d2.data())[i] << _NEWLINE;
        break;
      case FLOAT:
        std::cout << std::fixed << std::setprecision(4) << ((float *) d1.data())[i] << " - " << std::fixed
                  << std::setprecision(4) << ((float *) d2.data())[i] << _NEWLINE;
        break;
      case DOUBLE:
        std::cout << std::fixed << std::setprecision(4) << ((double *) d1.data())[i] << " - " << std::fixed
                  << std::setprecision(4) << ((double *) d2.data())[i] << _NEWLINE;
        break;
      case TAR_POSITION:
        std::cout << ((uint64_t *) d1.data())[i] << " - " << ((uint64_t *) d2.data())[i] << _NEWLINE;
        break;
      default:
        std::cout << "Unsupported type for print dataset.";
    }
  }
}


TEST_CASE("Engine", "[Engine]") {

  bool stressMode = false;
  int32_t num_threads = 2;
  int32_t num_subtars = 2;
  int64_t runsCount = 0;

  bool optimized = true;
  auto builder = new MockModulesBuilder();
  auto config = builder->BuildConfigurationManager();
  auto parser = builder->BuildParser();
  auto storageManager = builder->BuildStorageManager();
  auto queryDataManager = builder->BuildQueryDataManager();
  auto optimizer = builder->BuildOptimizer();
  auto engineListener = new TestCaseMatcher(storageManager);
  string dataElement; int64_t error_i, error_j;

  config->SetIntValue(MAX_PARA_SUBTARS, num_subtars);
  config->SetIntValue(MAX_THREADS, num_threads);
  builder->RunBootQueries(SETUP_QUERIES);

  auto engine = EnginePtr(
    new MockDefaultEngine(builder->BuildConfigurationManager(),
                          builder->BuildSystemLogger(),
                          builder->BuildMetadaManager(),
                          builder->BuildStorageManager()));

  ((DefaultEngine *) engine.get())->SetThisPtr(engine);

  while(true){
    for (int64_t i = 0; i < TEST_QUERIES.size(); i++) {

      queryDataManager->AddQueryTextPart(TEST_QUERIES[i]);
      engineListener->StartNewQuery();
      parser->Parse(queryDataManager);


      if (queryDataManager->GetQueryPlan() != nullptr) {

        if (optimized)
          optimizer->Optimize(queryDataManager);

        engine->Run(queryDataManager, engineListener);

        auto resultBinaryData = engineListener->GetBinaryData();

        auto asExpected = false;
        if (optimized) {
          auto expectedBinaryData = QUERY_RESULTS_WITH_OPTIMIZER[i];
          asExpected = Compare(resultBinaryData, expectedBinaryData,
                               dataElement, error_i, error_j);
        } else {
          auto expectedBinaryData = QUERY_RESULTS[i];
          asExpected = Compare(resultBinaryData, expectedBinaryData,
                               dataElement, error_i, error_j);
        }

        if (!asExpected) {

          if (!dataElement.empty()) {
            auto lastOp = queryDataManager->GetQueryPlan()->GetOperations().back();
            auto tar = lastOp->GetResultingTAR();
            auto type = tar->GetDataElement(dataElement)->GetDataType();
            printCase(resultBinaryData, QUERY_RESULTS_WITH_OPTIMIZER[i], type, dataElement, error_i, error_j);
          }
          WARN("Wrong query result for: " << TEST_QUERIES[i]);
        }

        REQUIRE(asExpected);
      }

      queryDataManager->Release();
    }

    if(!stressMode) break;
    WARN("Run " << ++runsCount << " finished.");
  }
}
