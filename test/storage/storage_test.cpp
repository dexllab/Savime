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
#include "mock_default_storage_manager.h"
#include "../test_utils.h"

TEST_CASE("Logical2Real(DimensionPtr, Literal)", "[StorageManager]") {
  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  vector<string> vec = {"1.2", "4.8", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);
  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 10);
  auto implicitDim2 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 0.1);
  auto implicitDim3 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);

  SECTION("Testing implicit dimensions.") {
    Literal l;
    SET_LITERAL(l, DataType(DOUBLE), 20);
    auto result = storage->Logical2Real(implicitDim1, l);
    REQUIRE(result == 2);

    SET_LITERAL(l, DataType(DOUBLE), 20.000000000000000001);
    result = storage->Logical2Real(implicitDim1, l);
    REQUIRE(result == 2);

    SET_LITERAL(l, DataType(DOUBLE), 15);
    result = storage->Logical2Real(implicitDim1, l);
    REQUIRE(result == -1);

    SET_LITERAL(l, DataType(DOUBLE), 0.2);
    result = storage->Logical2Real(implicitDim2, l);
    REQUIRE(result == 2);

    SET_LITERAL(l, DataType(DOUBLE), 0.19999999999999);
    result = storage->Logical2Real(implicitDim2, l);
    REQUIRE(result == 2);

    SET_LITERAL(l, DataType(DOUBLE), 0.15);
    result = storage->Logical2Real(implicitDim1, l);
    REQUIRE(result == -1);

    SET_LITERAL(l, DataType(DOUBLE), 4.8);
    result = storage->Logical2Real(implicitDim3, l);
    REQUIRE(result == 3);

    SET_LITERAL(l, DataType(DOUBLE), 4.79999999999999);
    result = storage->Logical2Real(implicitDim3, l);
    REQUIRE(result == 3);

    SET_LITERAL(l, DataType(DOUBLE), 0);
    result = storage->Logical2Real(implicitDim3, l);
    REQUIRE(result == -1);

    SET_LITERAL(l, DataType(DOUBLE), 100.5);
    result = storage->Logical2Real(implicitDim3, l);
    REQUIRE(result == -1);
  }

  SECTION("Testing explicit dimensions.") {
    Literal l;
    SET_LITERAL(l, DataType(DOUBLE), 1.2);
    auto result = storage->Logical2Real(explicitDim, l);
    REQUIRE(result == 0);

    SET_LITERAL(l, DataType(DOUBLE), 5.7);
    result = storage->Logical2Real(explicitDim, l);
    REQUIRE(result == 2);

    SET_LITERAL(l, DataType(DOUBLE), 1.19999999999999);
    result = storage->Logical2Real(explicitDim, l);
    REQUIRE(result == 0);

    SET_LITERAL(l, DataType(DOUBLE), 10.09);
    result = storage->Logical2Real(explicitDim, l);
    REQUIRE(result == 4);

    SET_LITERAL(l, DataType(DOUBLE), 13.7);
    result = storage->Logical2Real(explicitDim, l);
    REQUIRE(result == -1);

    SET_LITERAL(l, DataType(DOUBLE), 10.011);
    result = storage->Logical2Real(explicitDim, l);
    REQUIRE(result == -1);
  }

  delete builder;
}

TEST_CASE("Logical2ApproxReal(DimensionPtr, Literal)", "[StorageManager]") {
  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  vector<string> vec = {"1.2", "4.8", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);
  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 10);
  auto implicitDim2 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 0.1);
  auto implicitDim3 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);

  SECTION("Testing implicit dimensions.") {
    Literal l;
    SET_LITERAL(l, DataType(DOUBLE), 20);
    auto result = storage->Logical2ApproxReal(implicitDim1, l);
    REQUIRE(result.inf == 2);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 15);
    result = storage->Logical2ApproxReal(implicitDim1, l);
    REQUIRE(result.inf == 1);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 19.99999999999999);
    result = storage->Logical2ApproxReal(implicitDim1, l);
    REQUIRE(result.inf == 2);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 0.2);
    result = storage->Logical2ApproxReal(implicitDim2, l);
    REQUIRE(result.inf == 2);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 0.19999999999999);
    result = storage->Logical2ApproxReal(implicitDim2, l);
    REQUIRE(result.inf == 2);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 0.15);
    result = storage->Logical2ApproxReal(implicitDim2, l);
    REQUIRE(result.inf == 1);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 4.8);
    result = storage->Logical2ApproxReal(implicitDim3, l);
    REQUIRE(result.inf == 3);
    REQUIRE(result.sup == 3);

    SET_LITERAL(l, DataType(DOUBLE), 4.79999999999999);
    result = storage->Logical2ApproxReal(implicitDim3, l);
    REQUIRE(result.inf == 3);
    REQUIRE(result.sup == 3);

    SET_LITERAL(l, DataType(DOUBLE), 0);
    result = storage->Logical2ApproxReal(implicitDim3, l);
    REQUIRE(result.inf == 0);
    REQUIRE(result.sup == 0);

    SET_LITERAL(l, DataType(DOUBLE), 100.5);
    result = storage->Logical2ApproxReal(implicitDim3, l);
    REQUIRE(result.inf == 89);
    REQUIRE(result.sup == 89);
  }

  SECTION("Testing explicit dimensions.") {
    Literal l;
    SET_LITERAL(l, DataType(DOUBLE), 1.2);
    auto result = storage->Logical2ApproxReal(explicitDim, l);
    REQUIRE(result.inf == 0);
    REQUIRE(result.sup == 0);

    SET_LITERAL(l, DataType(DOUBLE), 5.7);
    result = storage->Logical2ApproxReal(explicitDim, l);
    REQUIRE(result.inf == 2);
    REQUIRE(result.sup == 2);

    SET_LITERAL(l, DataType(DOUBLE), 1.1999999);
    result = storage->Logical2ApproxReal(explicitDim, l);
    REQUIRE(result.inf == 0);
    REQUIRE(result.sup == 0);

    SET_LITERAL(l, DataType(DOUBLE), 10.09);
    result = storage->Logical2ApproxReal(explicitDim, l);
    REQUIRE(result.inf == 4);
    REQUIRE(result.sup == 4);

    SET_LITERAL(l, DataType(DOUBLE), 13.7);
    result = storage->Logical2ApproxReal(explicitDim, l);
    REQUIRE(result.inf == 4);
    REQUIRE(result.sup == 4);

    SET_LITERAL(l, DataType(DOUBLE), 10.011);
    result = storage->Logical2ApproxReal(explicitDim, l);
    REQUIRE(result.inf == 3);
    REQUIRE(result.sup == 4);
  }

  delete builder;
}

TEST_CASE("Logical2Real(DimensionPtr, DimSpecPtr, DatasetPtr, DatasetPtr)",
          "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator dsComp(storage);

  vector<string> vec = {"1.2", "4.8", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);

  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto explicitDimSpecs = make_shared<DimensionSpecification>(
      UNSAVED_ID, explicitDim, 0, explicitDim->GetLength() - 1, 1, 1);

  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 10);
  auto implicitDim1Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim1, 0, implicitDim1->GetLength() - 1, 1, 1);

  auto implicitDim2 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 0.1);
  auto implicitDim2Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim2, 0, implicitDim2->GetLength() - 1, 1, 1);

  auto implicitDim3 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);
  auto implicitDim3Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim3, 0, implicitDim3->GetLength() - 1, 1, 1);

  SECTION("Testing implicit dimensions.") {

    DatasetPtr destiny = nullptr;

    vector<string> vecTest1 = {"0", "10", "20", "30", "40"};
    auto ds1 = storage->Create(DOUBLE, vecTest1);
    auto result = storage->Logical2Real(implicitDim1, implicitDim1Specs, ds1,
                                        destiny);
    vector<string> vecTest2 = {"0", "1", "2", "3", "4"};
    auto expected = storage->Create(DOUBLE, vecTest2);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest3 = {"0", "40", "80", "100"};
    ds1 = storage->Create(DOUBLE, vecTest3);
    result = storage->Logical2Real(implicitDim1, implicitDim1Specs, ds1,
                                   destiny);
    vector<string> vecTest4 = {"0", "4", "8", "10"};
    expected = storage->Create(DOUBLE, vecTest4);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest6 = {"0", "50", "80", "101"};
    ds1 = storage->Create(DOUBLE, vecTest6);
    result = storage->Logical2Real(implicitDim1, implicitDim1Specs, ds1,
                                   destiny);
    REQUIRE(result == SAVIME_FAILURE);

    vector<string> vecTest7 = {"-1", "50", "80", "100"};
    ds1 = storage->Create(DOUBLE, vecTest7);
    result = storage->Logical2Real(implicitDim1, implicitDim1Specs, ds1,
                                   destiny);
    REQUIRE(result == SAVIME_FAILURE);

    vector<string> vecTest8 = {"0", "0.2", "0.3999999999999999999", "0.7"};
    ds1 = storage->Create(DOUBLE, vecTest8);
    result = storage->Logical2Real(implicitDim2, implicitDim2Specs, ds1,
                                   destiny);
    vector<string> vecTest9 = {"0", "2", "4", "7"};
    expected = storage->Create(DOUBLE, vecTest9);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest10 = {"0", "0.25", "0.4", "0.7"};
    ds1 = storage->Create(DOUBLE, vecTest10);
    result = storage->Logical2Real(implicitDim2, implicitDim2Specs, ds1,
                                   destiny);
    REQUIRE(result == SAVIME_FAILURE);

    vector<string> vecTest11 = {"0", "0.3", "0.2", "0.8"};
    ds1 = storage->Create(DOUBLE, vecTest11);
    result = storage->Logical2Real(implicitDim2, implicitDim2Specs, ds1,
                                   destiny);
    vector<string> vecTest12 = {"0", "3", "2", "8"};
    expected = storage->Create(DOUBLE, vecTest12);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));
  }

  SECTION("Testing explicit dimensions.") {

    DatasetPtr destiny = nullptr;
    vector<string> vecTest1 = {"1.2", "4.8", "5.7", "10.09"};
    auto ds1 = storage->Create(DOUBLE, vecTest1);
    auto result = storage->Logical2Real(explicitDim, explicitDimSpecs, ds1,
                                        destiny);
    vector<string> vecTest2 = {"0", "1", "2", "4"};
    auto expected = storage->Create(DOUBLE, vecTest2);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest3 = {"4.8", "1.2", "10.09"};
    ds1 = storage->Create(DOUBLE, vecTest3);
    result = storage->Logical2Real(explicitDim, explicitDimSpecs, ds1,
                                   destiny);
    vector<string> vecTest4 = {"1", "0", "4"};
    expected = storage->Create(DOUBLE, vecTest4);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest6 = {"1.2", "5.0", "10.09"};
    ds1 = storage->Create(DOUBLE, vecTest6);
    result = storage->Logical2Real(explicitDim, explicitDimSpecs, ds1,
                                   destiny);
    REQUIRE(result == SAVIME_FAILURE);

    vector<string> vecTest7 = {"-0.3", "4.8", "6.2", "10.09"};
    ds1 = storage->Create(DOUBLE, vecTest7);
    result = storage->Logical2Real(explicitDim, explicitDimSpecs, ds1,
                                   destiny);
    REQUIRE(result == SAVIME_FAILURE);

    vector<string> vecTest8 = {"4.79999", "1.2", "10.09"};
    ds1 = storage->Create(DOUBLE, vecTest8);
    result = storage->Logical2Real(explicitDim, explicitDimSpecs, ds1,
                                   destiny);
    REQUIRE(result == SAVIME_FAILURE);
  }

  delete builder;
}

TEST_CASE(
    "UnsafeLogical2Real(DimensionPtr, DimSpecPtr, DatasetPtr, DatasetPtr)",
    "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator dsComp(storage);

  vector<string> vec = {"1.2", "4.8", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);

  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto explicitDimSpecs = make_shared<DimensionSpecification>(
      UNSAVED_ID, explicitDim, 0, explicitDim->GetLength() - 1, 1, 1);

  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 10);
  auto implicitDim1Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim1, 0, implicitDim1->GetLength() - 1, 1, 1);

  auto implicitDim2 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 0.1);
  auto implicitDim2Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim2, 0, implicitDim2->GetLength() - 1, 1, 1);

  auto implicitDim3 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);
  auto implicitDim3Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim3, 0, implicitDim3->GetLength() - 1, 1, 1);

  SECTION("Testing implicit dimensions.") {

    DatasetPtr destiny = nullptr;

    vector<string> vecTest1 = {"0", "10", "20", "30", "40"};
    auto ds1 = storage->Create(DOUBLE, vecTest1);
    auto result = storage->UnsafeLogical2Real(implicitDim1, implicitDim1Specs,
                                              ds1, destiny);
    vector<string> vecTest2 = {"0", "1", "2", "3", "4"};
    auto expected = storage->Create(DOUBLE, vecTest2);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest3 = {"0", "40", "80", "100"};
    ds1 = storage->Create(DOUBLE, vecTest3);
    result = storage->UnsafeLogical2Real(implicitDim1, implicitDim1Specs, ds1,
                                         destiny);
    vector<string> vecTest4 = {"0", "4", "8", "10"};
    expected = storage->Create(DOUBLE, vecTest4);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest6 = {"0", "50", "80", "101"};
    ds1 = storage->Create(DOUBLE, vecTest6);
    result = storage->UnsafeLogical2Real(implicitDim1, implicitDim1Specs, ds1,
                                         destiny);
    vector<string> vecTest7 = {"0", "5", "8", "-1"};
    expected = storage->Create(DOUBLE, vecTest7);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest8 = {"0", "0.2", "0.3999999999999999999", "0.7"};
    ds1 = storage->Create(DOUBLE, vecTest8);
    result = storage->UnsafeLogical2Real(implicitDim2, implicitDim2Specs, ds1,
                                         destiny);
    vector<string> vecTest9 = {"0", "2", "4", "7"};
    expected = storage->Create(DOUBLE, vecTest9);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest10 = {"0", "0.25", "0.4", "0.7"};
    ds1 = storage->Create(DOUBLE, vecTest10);
    result = storage->UnsafeLogical2Real(implicitDim2, implicitDim2Specs, ds1,
                                         destiny);
    vector<string> vecTest11 = {"0", "-1", "4", "7"};
    expected = storage->Create(DOUBLE, vecTest11);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest12 = {"0", "0.3", "0.2", "0.8"};
    ds1 = storage->Create(DOUBLE, vecTest12);
    result = storage->UnsafeLogical2Real(implicitDim2, implicitDim2Specs, ds1,
                                         destiny);
    vector<string> vecTest13 = {"0", "3", "2", "8"};
    expected = storage->Create(DOUBLE, vecTest13);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));
  }

  SECTION("Testing explicit dimensions.") {

    DatasetPtr destiny = nullptr;
    vector<string> vecTest1 = {"1.2", "4.8", "5.7", "10.09"};
    auto ds1 = storage->Create(DOUBLE, vecTest1);
    auto result = storage->UnsafeLogical2Real(explicitDim, explicitDimSpecs,
                                              ds1, destiny);
    vector<string> vecTest2 = {"0", "1", "2", "4"};
    auto expected = storage->Create(DOUBLE, vecTest2);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest3 = {"4.8", "1.2", "10.09"};
    ds1 = storage->Create(DOUBLE, vecTest3);
    result = storage->UnsafeLogical2Real(explicitDim, explicitDimSpecs, ds1,
                                         destiny);
    vector<string> vecTest4 = {"1", "0", "4"};
    expected = storage->Create(DOUBLE, vecTest4);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest5 = {"1.2", "5.0", "10.09"};
    ds1 = storage->Create(DOUBLE, vecTest5);
    result = storage->UnsafeLogical2Real(explicitDim, explicitDimSpecs, ds1,
                                         destiny);
    vector<string> vecTest6 = {"0", "-1", "4"};
    expected = storage->Create(DOUBLE, vecTest6);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("Real2Logical(DimensionPtr, RealIndex)", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  vector<string> vec = {"1.2", "4.8", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);
  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 10);
  auto implicitDim2 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 0.1);
  auto implicitDim3 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);

  SECTION("Testing implicit dimensions.") {
    auto real = storage->Real2Logical(implicitDim1, 2);
    REQUIRE(abs(real.dbl - 20) < MIN_SPACING);

    real = storage->Real2Logical(implicitDim2, 2);
    REQUIRE(abs(real.dbl - 0.2) < MIN_SPACING);

    real = storage->Real2Logical(implicitDim3, 3);
    REQUIRE(abs(real.dbl - 4.80) < MIN_SPACING);
  }

  SECTION("Testing explicit dimensions.") {
    auto real = storage->Real2Logical(explicitDim, 0);
    REQUIRE(abs(real.dbl - 1.2) < MIN_SPACING);

    real = storage->Real2Logical(explicitDim, 2);
    REQUIRE(abs(real.dbl - 5.7) < MIN_SPACING);

    real = storage->Real2Logical(explicitDim, 4);
    REQUIRE(abs(real.dbl - 10.09) < MIN_SPACING);
  }

  delete builder;
}

TEST_CASE("Real2Logical(DimensionPtr, DimSpecPtr, DatasetPtr, DatasetPtr)",
          "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator dsComp(storage);

  vector<string> vec = {"1.2", "4.8", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);

  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto explicitDimSpecs = make_shared<DimensionSpecification>(
      UNSAVED_ID, explicitDim, 0, explicitDim->GetLength() - 1, 1, 1);

  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 10);
  auto implicitDim1Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim1, 0, implicitDim1->GetLength() - 1, 1, 1);

  auto implicitDim2 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 0.1);
  auto implicitDim2Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim2, 0, implicitDim2->GetLength() - 1, 1, 1);

  auto implicitDim3 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);
  auto implicitDim3Specs = make_shared<DimensionSpecification>(
      UNSAVED_ID, implicitDim3, 0, implicitDim3->GetLength() - 1, 1, 1);

  SECTION("Testing implicit dimensions.") {

    DatasetPtr destiny = nullptr;
    vector<string> vecTest1 = {"0", "1", "2", "3", "4"};
    auto ds1 = storage->Create(INT64, vecTest1);
    auto result = storage->Real2Logical(implicitDim1, implicitDim1Specs, ds1,
                                        destiny);
    vector<string> vecTest2 = {"0", "10", "20", "30", "40"};
    auto expected = storage->Create(DOUBLE, vecTest2);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest3 = {"0", "4", "8", "10"};
    ds1 = storage->Create(INT64, vecTest3);
    result = storage->Real2Logical(implicitDim1, implicitDim1Specs, ds1,
                                   destiny);
    vector<string> vecTest4 = {"0", "40", "80", "100"};
    expected = storage->Create(DOUBLE, vecTest4);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest11 = {"0", "3", "2", "8"};
    ds1 = storage->Create(INT64, vecTest11);
    result = storage->Real2Logical(implicitDim2, implicitDim2Specs, ds1,
                                   destiny);
    vector<string> vecTest12 = {"0", "0.3", "0.2", "0.8"};
    expected = storage->Create(DOUBLE, vecTest12);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));
  }

  SECTION("Testing explicit dimensions.") {

    DatasetPtr destiny = nullptr;
    vector<string> vecTest1 = {"0", "1", "2", "4"};
    auto ds1 = storage->Create(INT64, vecTest1);
    auto result = storage->Real2Logical(explicitDim, explicitDimSpecs, ds1,
                                        destiny);
    vector<string> vecTest2 = {"1.2", "4.8", "5.7", "10.09"};
    auto expected = storage->Create(DOUBLE, vecTest2);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));

    vector<string> vecTest3 = {"1", "0", "4"};
    ds1 = storage->Create(INT64, vecTest3);
    result = storage->Real2Logical(explicitDim, explicitDimSpecs, ds1,
                                   destiny);
    vector<string> vecTest4 = {"4.8", "1.2", "10.09"};
    expected = storage->Create(DOUBLE, vecTest4);
    REQUIRE(result == SAVIME_SUCCESS);
    REQUIRE(dsComp.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("IntersectDimensions", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  MetadataComparator metadataComparator(storage);

  vector<string> vec = {"1.2", "4.0", "5.7", "6.2", "10.09"};
  auto explicitDs = storage->Create(DOUBLE, vec);

  vector<string> vec2 = {"1.4", "4.8", "5.7", "6.2", "15.09"};
  auto explicitDs2 = storage->Create(DOUBLE, vec2);

  auto explicitDim1 = make_shared<Dimension>(0, "noname", explicitDs);
  explicitDim1->CurrentUpperBound() = explicitDim1->GetRealUpperBound();
  auto explicitDim2 = make_shared<Dimension>(0, "noname", explicitDs2);
  explicitDim2->CurrentUpperBound() = explicitDim2->GetRealUpperBound();

  auto implicitDim1 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 10, 1);
  implicitDim1->CurrentUpperBound() = implicitDim1->GetRealUpperBound();

  auto implicitDim2 =
      make_shared<Dimension>(0, "noname", DOUBLE, 1.5, 100, 1.1);
  implicitDim2->CurrentUpperBound() = implicitDim2->GetRealUpperBound();

  auto implicitDim3 = make_shared<Dimension>(0, "noname", DOUBLE, 0, 10, 2);
  implicitDim3->CurrentUpperBound() = implicitDim3->GetRealUpperBound();

  SECTION("Testing implicit x implicit.") {
    DimensionPtr destiny;
    vector<string> vec2 = {"0", "2", "4", "6", "8", "10"};
    auto intersectedDimDs = storage->Create(DOUBLE, vec2);
    DimensionPtr expected =
        make_shared<Dimension>(UNSAVED_ID, "", intersectedDimDs);
    storage->IntersectDimensions(implicitDim1, implicitDim3, destiny);
    REQUIRE(metadataComparator.Compare(destiny, expected, B(5)));
  }

  SECTION("Testing implicit x explicit.") {
    DimensionPtr destiny;
    vector<string> vec2 = {"4"};
    auto intersectedDimDs = storage->Create(DOUBLE, vec2);
    DimensionPtr expected =
        make_shared<Dimension>(UNSAVED_ID, "", intersectedDimDs);
    storage->IntersectDimensions(implicitDim1, explicitDim1, destiny);
    REQUIRE(metadataComparator.Compare(destiny, expected, B(5)));
  }

  SECTION("Testing excitit x explicit.") {
    DimensionPtr destiny;
    vector<string> vec2 = {"5.7", "6.2"};
    auto intersectedDimDs = storage->Create(DOUBLE, vec2);
    DimensionPtr expected =
        make_shared<Dimension>(UNSAVED_ID, "", intersectedDimDs);
    storage->IntersectDimensions(explicitDim1, explicitDim2, destiny);
    REQUIRE(metadataComparator.Compare(destiny, expected, B(5)));
  }

  SECTION("Testing empty intersection") {
    DimensionPtr destiny;
    DimensionPtr expected = make_shared<Dimension>(
        UNSAVED_ID, "", implicitDim1->GetType(), 0, 0, 1);
    storage->IntersectDimensions(implicitDim1, explicitDim2, destiny);
    REQUIRE(metadataComparator.Compare(destiny, expected, B(5)));
  }

  delete builder;
}


TEST_CASE("CheckSorted", "[StorageManager]") {
  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();

  vector<string> vec = {"1.2", "4.0", "5.7", "6.2", "10.09"};
  auto sorted = storage->Create(DOUBLE, vec);
  auto result = storage->CheckSorted(sorted);
  REQUIRE(result == true);

  vector<string> vec2 = {"1", "2", "3", "4", "5"};
  auto sorted2 = storage->Create(DOUBLE, vec2);
  result = storage->CheckSorted(sorted2);
  REQUIRE(result == true);

  vector<string> vec3 = {"1", "20", "35", "89", "100"};
  auto sorted3 = storage->Create(DOUBLE, vec3);
  result = storage->CheckSorted(sorted3);
  REQUIRE(result == true);

  vector<string> vec4 = {"1", "20", "20", "20", "30"};
  auto sorted4 = storage->Create(DOUBLE, vec4);
  result = storage->CheckSorted(sorted4);
  REQUIRE(result == true);

  vector<string> vec5 = {"20", "1", "20", "20", "30"};
  auto nonsorted = storage->Create(DOUBLE, vec5);
  result = storage->CheckSorted(nonsorted);
  REQUIRE(result == false);

  vector<string> vec6 = {"1", "2", "3", "4", "3"};;
  auto nonsorted2 = storage->Create(DOUBLE, vec6);
  result = storage->CheckSorted(nonsorted2);
  REQUIRE(result == false);

  vector<string> vec7 = {"1", "2", "3", "2.5", "3", "8"};;
  auto nonsorted3 = storage->Create(DOUBLE, vec7);
  result = storage->CheckSorted(nonsorted3);
  REQUIRE(result == false);

  delete builder;
}


TEST_CASE("Copy(DatasetPtr, SubTARPosition, SubTARPosition, SubTARPosition,"
          "savime_size_t, DatasetPtr", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);

  vector<string> vec = {"1", "4", "5", "6", "10",
                        "12", "15", "19", "22", "27",
                        "33", "35", "37", "39", "40",
                        "42", "43", "46", "51", "55"};
  auto ds1 = storage->Create(DOUBLE, vec);

  vector<string> vec2 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds2 = storage->Create(DOUBLE, vec2);


  auto result = storage->Copy(ds1, 0, 5, 0, 1, ds2);
  vector<string> vec3 = {"1", "4", "5", "6", "10",
                         "12", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds3 = storage->Create(DOUBLE, vec3);
  REQUIRE(comparator.Compare(ds2, ds3));

  result = storage->Copy(ds1, 6, 8, 0, 3, ds2);
  vector<string> vec4 = {"15", "4", "5", "19", "10",
                         "12", "22", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds4 = storage->Create(DOUBLE, vec4);
  REQUIRE(comparator.Compare(ds2, ds4));


  result = storage->Copy(ds1, 10, 19, 10, 1, ds2);
  vector<string> vec5 = {"15", "4", "5", "19", "10",
                         "12", "22", "19.74", "22.32", "27.32",
                         "33", "35", "37", "39", "40",
                         "42", "43", "46", "51", "55"};
  auto ds5 = storage->Create(DOUBLE, vec5);
  REQUIRE(comparator.Compare(ds2, ds5));

  delete builder;
}

TEST_CASE("Copy(DatasetPtr, Mapping, DatasetPtr, int64_t", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);
  int64_t copied = 0;

  vector<string> vec = {"1", "4", "5", "6", "10",
                        "12", "15", "19", "22", "27",
                        "33", "35", "37", "39", "40",
                        "42", "43", "46", "51", "55"};
  auto ds1 = storage->Create(DOUBLE, vec);


  vector<string> vec2 = {"0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0"};
  auto ds2 = storage->Create(DOUBLE, vec2);

  std::vector<SubTARPosition> mapVec = {0, 1, 2, 3, 4,
                                        -1, -1, -1, -1, -1,
                                        -1, -1, -1, -1, -1,
                                        -1, -1, -1, -1, -1};
  Mapping mapping = std::make_shared<std::vector<SubTARPosition>>(mapVec);


  auto result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec3 = {"1", "4", "5", "6", "10",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0"};
  auto ds3 = storage->Create(DOUBLE, vec3);
  REQUIRE(comparator.Compare(ds2, ds3));
  REQUIRE(copied == 5);


  std::vector<SubTARPosition> mapVec1 = {15, 16, 17, 18, 19,
                                         -1, -1, -1, -1, -1,
                                         -1, -1, -1, -1, -1,
                                         -1, -1, -1, -1, -1};
  mapping = std::make_shared<std::vector<SubTARPosition>>(mapVec1);


  result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec4 = {"1", "4", "5", "6", "10",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "1", "4", "5", "6", "10"};
  auto ds4 = storage->Create(DOUBLE, vec4);
  REQUIRE(comparator.Compare(ds2, ds4));
  REQUIRE(copied == 5);


  std::vector<SubTARPosition> mapVec2 = {19, 18, 17, 16, 15,
                                         14, 13, 12, 11, 10,
                                         9, 8, 7, 6, 5,
                                         4, 3, 2, 1, 0};
  mapping = std::make_shared<std::vector<SubTARPosition>>(mapVec2);


  result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec5 = {"55", "51", "46", "43", "42",
                         "40", "39", "37", "35", "33",
                         "27", "22", "19", "15", "12",
                         "10", "6", "5", "4", "1"};
  auto ds5 = storage->Create(DOUBLE, vec5);
  REQUIRE(comparator.Compare(ds2, ds5));
  REQUIRE(copied == 20);


  std::vector<SubTARPosition> mapVec3 = {-1, -1, -1, -1, -1,
                                         10, -1, -1, -1, -1,
                                         5, -1, -1, -1, -1,
                                         1, -1, -1, -1, -1};
  mapping = std::make_shared<std::vector<SubTARPosition>>(mapVec3);

  result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec6 = {"55", "42", "46", "43", "42",
                         "33", "39", "37", "35", "33",
                         "12", "22", "19", "15", "12",
                         "10", "6", "5", "4", "1"};
  auto ds6 = storage->Create(DOUBLE, vec6);
  REQUIRE(comparator.Compare(ds2, ds6));
  REQUIRE(copied == 3);

  delete builder;
}


TEST_CASE("Copy(DatasetPtr, DatasetPtr, DatasetPtr, int64_t",
          "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);
  int64_t copied = 0;

  vector<string> vec = {"1", "4", "5", "6", "10",
                        "12", "15", "19", "22", "27",
                        "33", "35", "37", "39", "40",
                        "42", "43", "46", "51", "55"};
  auto ds1 = storage->Create(DOUBLE, vec);


  vector<string> vec2 = {"0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0"};
  auto ds2 = storage->Create(DOUBLE, vec2);

  std::vector<string> mapVec = {"0", "1", "2", "3", "4",
                                "-1", "-1", "-1", "-1", "-1",
                                "-1", "-1", "-1", "-1", "-1",
                                "-1", "-1", "-1", "-1", "-1"};
  auto mapping = storage->Create(INT64, mapVec);



  auto result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec3 = {"1", "4", "5", "6", "10",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0"};
  auto ds3 = storage->Create(DOUBLE, vec3);
  REQUIRE(comparator.Compare(ds2, ds3));
  REQUIRE(copied == 5);


  std::vector<string> mapVec1 = {"15", "16", "17", "18", "19",
                                 "-1", "-1", "-1", "-1", "-1",
                                 "-1", "-1", "-1", "-1", "-1",
                                 "-1", "-1", "-1", "-1", "-1"};
  mapping = storage->Create(INT64, mapVec1);


  result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec4 = {"1", "4", "5", "6", "10",
                         "0", "0", "0", "0", "0",
                         "0", "0", "0", "0", "0",
                         "1", "4", "5", "6", "10"};
  auto ds4 = storage->Create(DOUBLE, vec4);
  REQUIRE(comparator.Compare(ds2, ds4));
  REQUIRE(copied == 5);


  std::vector<string> mapVec2 = {"19", "18", "17", "16", "15",
                                 "14", "13", "12", "11", "10",
                                 "9", "8", "7", "6", "5",
                                 "4", "3", "2", "1", "0"};
  mapping = storage->Create(INT64, mapVec2);


  result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec5 = {"55", "51", "46", "43", "42",
                         "40", "39", "37", "35", "33",
                         "27", "22", "19", "15", "12",
                         "10", "6", "5", "4", "1"};
  auto ds5 = storage->Create(DOUBLE, vec5);
  REQUIRE(comparator.Compare(ds2, ds5));
  REQUIRE(copied == 20);


  std::vector<string> mapVec3 = {"-1", "-1", "-1", "-1", "-1",
                                 "10", "-1", "-1", "-1", "-1",
                                 "5", "-1", "-1", "-1", "-1",
                                 "1", "-1", "-1", "-1", "-1"};
  mapping = storage->Create(INT64, mapVec3);

  result = storage->Copy(ds1, mapping, ds2, copied);
  vector<string> vec6 = {"55", "42", "46", "43", "42",
                         "33", "39", "37", "35", "33",
                         "12", "22", "19", "15", "12",
                         "10", "6", "5", "4", "1"};
  auto ds6 = storage->Create(DOUBLE, vec6);
  REQUIRE(comparator.Compare(ds2, ds6));
  REQUIRE(copied == 3);

  delete builder;
}

TEST_CASE("Filter(DatasetPtr, DatasetPtr, DatasetPtr", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);

  vector<string> vec = {"1", "4", "5", "6", "10",
                        "12", "15", "19", "22", "27",
                        "33", "35", "37", "39", "40",
                        "42", "43", "46", "51", "55"};
  auto ds1 = storage->Create(DOUBLE, vec);


  vector<bool> bits = {false, false, false, false, false,
                       false, false, false, false, false,
                       false, false, true, false, false,
                       false, false, false, false, false,};

  auto filter = make_shared<Dataset>(ds1->GetEntryCount());
  InitBitmask(filter, bits);


  auto result = storage->Filter(ds1, filter, destiny);
  vector<string> vec2 = {"37"};
  auto ds2 = storage->Create(DOUBLE, vec2);
  REQUIRE(comparator.Compare(destiny, ds2));


  vector<bool> bits2 = {true, false, false, false, false,
                        true, false, false, false, false,
                        true, false, false, false, false,
                        true, false, false, false, false,};

  filter = make_shared<Dataset>(ds1->GetEntryCount());
  InitBitmask(filter, bits2);


  result = storage->Filter(ds1, filter, destiny);
  vector<string> vec3 = {"1", "12", "33", "42"};
  auto ds3 = storage->Create(DOUBLE, vec3);
  REQUIRE(comparator.Compare(destiny, ds3));


  vector<bool> bits3 = {true, false, false, false, false,
                        false, true, false, false, false,
                        false, false, true, false, false,
                        false, false, false, true, true,};

  filter = make_shared<Dataset>(ds1->GetEntryCount());
  InitBitmask(filter, bits3);


  result = storage->Filter(ds1, filter, destiny);
  vector<string> vec4 = {"1", "15", "37", "51", "55"};
  auto ds4 = storage->Create(DOUBLE, vec4);
  REQUIRE(comparator.Compare(destiny, ds4));


  vector<bool> bits4 = {true, true, true, true, true,
                        true, true, true, true, true,
                        true, true, true, true, true,
                        true, true, true, true, true,};

  filter = make_shared<Dataset>(ds1->GetEntryCount());
  InitBitmask(filter, bits4);


  result = storage->Filter(ds1, filter, destiny);
  vector<string> vec5 = {"1", "4", "5", "6", "10",
                         "12", "15", "19", "22", "27",
                         "33", "35", "37", "39", "40",
                         "42", "43", "46", "51", "55"};
  auto ds5 = storage->Create(DOUBLE, vec5);
  REQUIRE(comparator.Compare(destiny, ds5));

  delete builder;
}


TEST_CASE("And(DatasetPtr, DatasetPtr, DatasetPtr", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);

  vector<bool> vec1 = {false, false, false, false, false,
                       false, false, false, false, true,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  auto ds1 = make_shared<Dataset>(vec1.size());
  InitBitmask(ds1, vec1);

  vector<bool> vec2 = {true, false, false, false, false,
                       false, true, false, false, false,
                       false, false, true, false, true,
                       false, false, false, false, false,};
  auto ds2 = make_shared<Dataset>(vec2.size());
  InitBitmask(ds2, vec2);
  storage->And(ds1, ds2, destiny);

  vector<bool> vec3 = {false, false, false, false, false,
                       false, false, false, false, false,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  auto expected = make_shared<Dataset>(vec3.size());
  InitBitmask(expected, vec3);
  storage->And(ds1, ds2, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));


  vector<bool> vec4 = {false, true, false, false, false,
                       false, false, false, false, true,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  ds1 = make_shared<Dataset>(vec4.size());
  InitBitmask(ds1, vec4);

  vector<bool> vec5 = {true, false, false, true, false,
                       false, true, false, false, false,
                       false, false, false, false, true,
                       false, false, false, false, false,};
  ds2 = make_shared<Dataset>(vec5.size());
  InitBitmask(ds2, vec5);
  storage->And(ds1, ds2, destiny);

  vector<bool> vec6 = {false, false, false, false, false,
                       false, false, false, false, false,
                       false, false, false, false, false,
                       false, false, false, false, false,};
  expected = make_shared<Dataset>(vec6.size());
  InitBitmask(expected, vec6);
  storage->And(ds1, ds2, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));

  vector<bool> vec7 = {true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true};
  ds1 = make_shared<Dataset>(vec7.size());
  InitBitmask(ds1, vec7);

  vector<bool> vec8 = {true, false, false, true, false,
                       false, true, false, true, false,
                       false, false, true, false, true,
                       true, true, false, true, false};
  ds2 = make_shared<Dataset>(vec8.size());
  InitBitmask(ds2, vec8);
  storage->And(ds1, ds2, destiny);

  vector<bool> vec9 = {true, false, false, true, false,
                       false, true, false, true, false,
                       false, false, true, false, true,
                       true, true, false, true, false};
  expected = make_shared<Dataset>(vec9.size());
  InitBitmask(expected, vec9);
  storage->And(ds1, ds2, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));



  vector<bool> vec10 = {true, false, true, false, true,
                        false, true, false, true, false,
                        true, false, true, false, true,
                        false, true, false, true, false};
  ds1 = make_shared<Dataset>(vec10.size());
  InitBitmask(ds1, vec10);

  vector<bool> vec11 = {false, true, false, true, false,
                        true, false, true, false, true,
                        false, true, false, true, false,
                        true, false, true, false, true};
  ds2 = make_shared<Dataset>(vec11.size());
  InitBitmask(ds2, vec11);
  storage->And(ds1, ds2, destiny);

  vector<bool> vec12 = {false, false, false, false, false,
                        false, false, false, false, false,
                        false, false, false, false, false,
                        false, false, false, false, false};
  expected = make_shared<Dataset>(vec12.size());
  InitBitmask(expected, vec12);
  storage->And(ds1, ds2, destiny);
  REQUIRE(comparator.CompareBitmask(destiny, expected));

  delete builder;
}

TEST_CASE("OR(DatasetPtr, DatasetPtr, DatasetPtr", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);

  vector<bool> vec1 = {false, false, false, false, false,
                       false, false, false, false, true,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  auto ds1 = make_shared<Dataset>(vec1.size());
  InitBitmask(ds1, vec1);

  vector<bool> vec2 = {true, false, false, false, false,
                       false, true, false, false, false,
                       false, false, true, false, true,
                       false, false, false, false, false,};
  auto ds2 = make_shared<Dataset>(vec2.size());
  InitBitmask(ds2, vec2);

  vector<bool> vec3 = {true, false, false, false, false,
                       false, true, false, false, true,
                       false, false, true, false, true,
                       false, false, false, false, false,};
  auto expected = make_shared<Dataset>(vec3.size());
  InitBitmask(expected, vec3);
  storage->Or(ds1, ds2, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));


  vector<bool> vec4 = {false, true, false, false, false,
                       false, false, false, false, true,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  ds1 = make_shared<Dataset>(vec4.size());
  InitBitmask(ds1, vec4);

  vector<bool> vec5 = {true, false, false, true, false,
                       false, true, false, false, false,
                       false, false, false, false, true,
                       false, false, false, false, false,};
  ds2 = make_shared<Dataset>(vec5.size());
  InitBitmask(ds2, vec5);

  vector<bool> vec6 = {true, true, false, true, false,
                       false, true, false, false, true,
                       false, false, true, false, true,
                       false, false, false, false, false,};
  expected = make_shared<Dataset>(vec6.size());
  InitBitmask(expected, vec6);
  storage->Or(ds1, ds2, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));

  vector<bool> vec7 = {true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true};
  ds1 = make_shared<Dataset>(vec7.size());
  InitBitmask(ds1, vec7);

  vector<bool> vec8 = {true, false, false, true, false,
                       false, true, false, true, false,
                       false, false, true, false, true,
                       true, true, false, true, false};
  ds2 = make_shared<Dataset>(vec8.size());
  InitBitmask(ds2, vec8);
  storage->Or(ds1, ds2, destiny);

  vector<bool> vec9 = {true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true};
  expected = make_shared<Dataset>(vec9.size());
  InitBitmask(expected, vec9);
  storage->Or(ds1, ds2, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));



  vector<bool> vec10 = {true, false, true, false, true,
                        false, true, false, true, false,
                        true, false, true, false, true,
                        false, true, false, true, false};
  ds1 = make_shared<Dataset>(vec10.size());
  InitBitmask(ds1, vec10);

  vector<bool> vec11 = {false, true, false, true, false,
                        true, false, true, false, true,
                        false, true, false, true, false,
                        true, false, true, false, true};
  ds2 = make_shared<Dataset>(vec11.size());
  InitBitmask(ds2, vec11);


  vector<bool> vec12 = {true, true, true, true, true,
                        true, true, true, true, true,
                        true, true, true, true, true,
                        true, true, true, true, true};
  expected = make_shared<Dataset>(vec12.size());
  InitBitmask(expected, vec12);
  storage->Or(ds1, ds2, destiny);
  REQUIRE(comparator.CompareBitmask(destiny, expected));

  delete builder;
}

TEST_CASE("NOT(DatasetPtr, DatasetPtr, DatasetPtr", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);

  vector<bool> vec1 = {false, false, false, false, false,
                       false, false, false, false, true,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  auto ds1 = make_shared<Dataset>(vec1.size());
  InitBitmask(ds1, vec1);

  vector<bool> vec3 = {true, true, true, true, true,
                       true, true, true, true, false,
                       true, true, false, true, true,
                       true, true, true, true, true,};
  auto expected = make_shared<Dataset>(vec3.size());
  InitBitmask(expected, vec3);
  storage->Not(ds1, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));


  vector<bool> vec4 = {false, true, false, false, false,
                       false, false, false, false, true,
                       false, false, true, false, false,
                       false, false, false, false, false,};
  ds1 = make_shared<Dataset>(vec4.size());
  InitBitmask(ds1, vec4);


  vector<bool> vec6 = {true, false, true, true, true,
                       true, true, true, true, false,
                       true, true, false, true, true,
                       true, true, true, true, true,};
  expected = make_shared<Dataset>(vec6.size());
  InitBitmask(expected, vec6);
  storage->Not(ds1, destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));

  vector<bool> vec7 = {true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true,
                       true, true, true, true, true};
  ds1 = make_shared<Dataset>(vec7.size());
  InitBitmask(ds1, vec7);


  vector<bool> vec9 = {false, false, false, false, false,
                       false, false, false, false, false,
                       false, false, false, false, false,
                       false, false, false, false, false};
  expected = make_shared<Dataset>(vec9.size());
  InitBitmask(expected, vec9);
  storage->Not(ds1,destiny);

  REQUIRE(comparator.CompareBitmask(destiny, expected));



  vector<bool> vec10 = {true, false, true, false, true,
                        false, true, false, true, false,
                        true, false, true, false, true,
                        false, true, false, true, false};
  ds1 = make_shared<Dataset>(vec10.size());
  InitBitmask(ds1, vec10);

  vector<bool> vec12 = {false, true, false, true, false,
                        true, false, true, false, true,
                        false, true, false, true, false,
                        true, false, true, false, true};

  expected = make_shared<Dataset>(vec12.size());
  InitBitmask(expected, vec12);
  storage->Not(ds1, destiny);
  REQUIRE(comparator.CompareBitmask(destiny, expected));

  delete builder;
}

TEST_CASE("Comparison(DatasetPtr, DatasetPtr, DatasetPtr", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  DatasetComparator comparator(storage);


  vector<string> vec1 = {"1", "4", "5", "6", "10",
                         "12", "15", "19", "22", "27",
                         "33", "35", "37", "39", "40",
                         "42", "43", "46", "51", "55"};
  auto ds1 = storage->Create(DOUBLE, vec1);


  vector<string> vec2 = {"1", "4", "5", "6", "10",
                         "13", "16", "20", "24", "30",
                         "30", "32", "33", "38", "39",
                         "41", "44", "44", "52", "55"};
  auto ds2 = storage->Create(DOUBLE, vec2);


  vector<string> vec3 = {"1", "4", "5", "6", "10",
                         "12", "15", "19", "22", "27",
                         "33", "35", "37", "39", "40",
                         "42", "43", "46", "51", "55"};
  auto ds3 = storage->Create(DataType(DOUBLE, 4, 1), vec3);


  vector<string> vec4 = {"1", "4", "5", "6", "10",
                         "13", "16", "20", "24", "30",
                         "30", "32", "33", "38", "39",
                         "41", "44", "44", "52", "55"};
  auto ds4 = storage->Create(DataType(DOUBLE, 4, 1), vec4);

  SECTION("Testing comparison in scalar typed datasets.") {
    vector<bool> vecbool1 = {true, true, true, true, true,
                             false, false, false, false, false,
                             false, false, false, false, false,
                             false, false, false, false, true};
    auto expected = make_shared<Dataset>(vecbool1.size());
    InitBitmask(expected, vecbool1);
    storage->Comparison(_EQ, ds1, ds2, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool2 = {false, false, false, false, false,
                             true, true, true, true, true,
                             true, true, true, true, true,
                             true, true, true, true, false};
    expected = make_shared<Dataset>(vecbool2.size());
    InitBitmask(expected, vecbool2);
    storage->Comparison(_NEQ, ds1, ds2, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool3 = {false, false, false, false, false,
                             true, true, true, true, true,
                             false, false, false, false, false,
                             false, true, false, true, false};
    expected = make_shared<Dataset>(vecbool3.size());
    InitBitmask(expected, vecbool3);
    storage->Comparison(_LE, ds1, ds2, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool4 = {false, false, false, false, false,
                             false, false, false, false, false,
                             true, true, true, true, true,
                             true, false, true, false, false};
    expected = make_shared<Dataset>(vecbool4.size());
    InitBitmask(expected, vecbool4);
    storage->Comparison(_GE, ds1, ds2, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool5 = {true, true, true, true, true,
                             true, true, true, true, true,
                             false, false, false, false, false,
                             false, true, false, true, true};
    expected = make_shared<Dataset>(vecbool5.size());
    InitBitmask(expected, vecbool5);
    storage->Comparison(_LEQ, ds1, ds2, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool6 = {true, true, true, true, true,
                             false, false, false, false, false,
                             true, true, true, true, true,
                             true, false, true, false, true};
    expected = make_shared<Dataset>(vecbool6.size());
    InitBitmask(expected, vecbool6);
    storage->Comparison(_GEQ, ds1, ds2, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));
  }

  SECTION("Testing comparison in vector typed datasets.") {

    vector<bool> vecbool7 = {true, false, false, false, false};
    auto expected = make_shared<Dataset>(vecbool7.size());
    InitBitmask(expected, vecbool7);
    storage->Comparison(_EQ, ds3, ds4, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool8 = {false, true, true, true, true};
    expected = make_shared<Dataset>(vecbool8.size());
    InitBitmask(expected, vecbool8);
    storage->Comparison(_NEQ, ds3, ds4, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool9 = {false, true, true, false, false};
    expected = make_shared<Dataset>(vecbool9.size());
    InitBitmask(expected, vecbool9);
    storage->Comparison(_LE, ds3, ds4, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool10 = {false, false, false, true, true};
    expected = make_shared<Dataset>(vecbool10.size());
    InitBitmask(expected, vecbool10);
    storage->Comparison(_GE, ds3, ds4, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool11 = {true, true, true, false, false};
    expected = make_shared<Dataset>(vecbool11.size());
    InitBitmask(expected, vecbool11);
    storage->Comparison(_LEQ, ds3, ds4, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool12 = {true, false, false, true, true};
    expected = make_shared<Dataset>(vecbool12.size());
    InitBitmask(expected, vecbool12);
    storage->Comparison(_GEQ, ds3, ds4, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));
  }

  delete builder;
}

TEST_CASE("Comparison(DatasetPtr, DatasetPtr, Literal", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetPtr destiny;
  Literal literal;
  SET_LITERAL(literal, DataType(DOUBLE), 37.0);
  SET_LITERAL(literal, DataType(DOUBLE), 37.0);

  DatasetComparator comparator(storage);


  vector<string> vec1 = {"1", "4", "5", "6", "10",
                         "12", "15", "19", "22", "27",
                         "33", "35", "37", "39", "40",
                         "42", "43", "46", "51", "55"};
  auto ds1 = storage->Create(DOUBLE, vec1);


  vector<string> vec3 = {"1", "4", "5", "6", "10",
                         "12", "15", "19", "22", "27",
                         "33", "35", "37", "39", "40",
                         "42", "43", "46", "51", "55"};
  auto ds3 = storage->Create(DataType(DOUBLE, 4, 1), vec3);


  vector<string> vec4 = {"amanda", "charles", "daniel", "felix",
                         "joane",  "phyllis", "sherman", "silvia"};
  auto ds4 = storage->Create(DataType(CHAR, 10), vec4);


  SECTION("Testing comparison in scalar typed datasets.") {
    vector<bool> vecbool1 = {false, false, false, false, false,
                             false, false, false, false, false,
                             false, false, true, false, false,
                             false, false, false, false, false};
    auto expected = make_shared<Dataset>(vecbool1.size());
    InitBitmask(expected, vecbool1);
    storage->Comparison(_EQ, ds1, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool2 = {true, true, true, true, true,
                             true, true, true, true, true,
                             true, true, false, true, true,
                             true, true, true, true, true};
    expected = make_shared<Dataset>(vecbool1.size());
    InitBitmask(expected, vecbool2);
    storage->Comparison(_NEQ, ds1, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool3 = {true, true, true, true, true,
                             true, true, true, true, true,
                             true, true, false, false, false,
                             false, false, false, false, false};
    expected = make_shared<Dataset>(vecbool3.size());
    InitBitmask(expected, vecbool3);
    storage->Comparison(_LE, ds1, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool4 = {false, false, false, false, false,
                             false, false, false, false, false,
                             false, false, false, true, true,
                             true, true, true, true, true};
    expected = make_shared<Dataset>(vecbool4.size());
    InitBitmask(expected, vecbool4);
    storage->Comparison(_GE, ds1, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool5 = {true, true, true, true, true,
                             true, true, true, true, true,
                             true, true, true, false, false,
                             false, false, false, false, false};
    expected = make_shared<Dataset>(vecbool5.size());
    InitBitmask(expected, vecbool5);
    storage->Comparison(_LEQ, ds1, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool6 = {false, false, false, false, false,
                             false, false, false, false, false,
                             false, false, true, true, true,
                             true, true, true, true, true};
    expected = make_shared<Dataset>(vecbool4.size());
    InitBitmask(expected, vecbool6);
    storage->Comparison(_GEQ, ds1, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));
  }

  SECTION("Testing comparison in scalar typed datasets.") {
    vector<bool> vecbool7 = {false, false, false, false, false};
    auto expected = make_shared<Dataset>(vecbool7.size());
    InitBitmask(expected, vecbool7);
    storage->Comparison(_EQ, ds3, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool8 = {true, true, true, true, true};
    expected = make_shared<Dataset>(vecbool8.size());
    InitBitmask(expected, vecbool8);
    storage->Comparison(_NEQ, ds3, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool9 = {true, true, true, false, false};
    expected = make_shared<Dataset>(vecbool9.size());
    InitBitmask(expected, vecbool9);
    storage->Comparison(_LE, ds3, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool10 = {false, false, false, true, true};
    expected = make_shared<Dataset>(vecbool10.size());
    InitBitmask(expected, vecbool10);
    storage->Comparison(_GE, ds3, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool11 = {true, true, true, false, false};
    expected = make_shared<Dataset>(vecbool11.size());
    InitBitmask(expected, vecbool11);
    storage->Comparison(_LEQ, ds3, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool12 = {false, false, false, true, true};
    expected = make_shared<Dataset>(vecbool12.size());
    InitBitmask(expected, vecbool12);
    storage->Comparison(_GEQ, ds3, literal, destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));
  }

  SECTION("Testing comparison in string typed datasets.") {
    vector<bool> vecbool13 = {true, false, false, false,
                              false, false, false, false};
    auto expected = make_shared<Dataset>(vecbool13.size());
    InitBitmask(expected, vecbool13);
    storage->Comparison(_EQ, ds4, Literal("amanda"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool14 = {false, true, true, true,
                              true, true, true, true};
    expected = make_shared<Dataset>(vecbool14.size());
    InitBitmask(expected, vecbool14);
    storage->Comparison(_NEQ, ds4, Literal("amanda"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool15 = {true, true, false, false,
                              false, false, false, false};
    expected = make_shared<Dataset>(vecbool15.size());
    InitBitmask(expected, vecbool15);
    storage->Comparison(_LE, ds4, Literal("d"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool16 = {false, false, true, true,
                              true, true, true, true};
    expected = make_shared<Dataset>(vecbool16.size());
    InitBitmask(expected, vecbool16);
    storage->Comparison(_GE, ds4, Literal("d"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool17 = {true, true, true, false,
                              false, false, false, false};
    expected = make_shared<Dataset>(vecbool17.size());
    InitBitmask(expected, vecbool17);
    storage->Comparison(_LEQ, ds4, Literal("daniel"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool18 = {false, false, true, true,
                              true, true, true, true};
    expected = make_shared<Dataset>(vecbool18.size());
    InitBitmask(expected, vecbool18);
    storage->Comparison(_GEQ, ds4, Literal("daniel"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool19 = {false, true, false, false,
                              false, true, false, false};
    expected = make_shared<Dataset>(vecbool19.size());
    InitBitmask(expected, vecbool19);
    storage->Comparison(_LIKE, ds4, Literal("%s"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));

    vector<bool> vecbool20 = {false, false, false, false,
                              false, false, true, true};
    expected = make_shared<Dataset>(vecbool20.size());
    InitBitmask(expected, vecbool20);
    storage->Comparison(_LIKE, ds4, Literal("s%"), destiny);
    REQUIRE(comparator.CompareBitmask(destiny, expected));
  }

  delete builder;
}


TEST_CASE("SubsetDims", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny = nullptr;

  auto dim1 = make_shared<Dimension>(UNSAVED_ID, "x", DOUBLE, 0, 100, 1);
  auto dim2 = make_shared<Dimension>(UNSAVED_ID, "y", DOUBLE, 0, 80, 0.5);
  auto dim3 = make_shared<Dimension>(UNSAVED_ID, "z", DOUBLE, 0, 50, 2);

  auto dimSpec31 =
      make_shared<DimensionSpecification>(UNSAVED_ID, dim1, 0, 2, 105, 35);
  auto dimSpec32 =
      make_shared<DimensionSpecification>(UNSAVED_ID, dim2, 0, 6, 35, 5);
  auto dimSpec33 =
      make_shared<DimensionSpecification>(UNSAVED_ID, dim3, 0, 4, 5, 1);
  vector<DimSpecPtr> _3DdimSpecs = {dimSpec31, dimSpec32, dimSpec33};

  auto dimSpec21 =
      make_shared<DimensionSpecification>(UNSAVED_ID, dim1, 0, 6, 35, 5);
  auto dimSpec22 =
      make_shared<DimensionSpecification>(UNSAVED_ID, dim2, 0, 4, 5, 1);
  vector<DimSpecPtr> _2DdimSpecs = {dimSpec21, dimSpec22};

  SECTION("Testing 2D subsetting.") {
    storage->SubsetDims(_2DdimSpecs, {1, 1}, {4, 4}, destiny);
    vector<string> vec1 = {"6",  "7",  "8",  "9",  "11", "12", "13", "14",
                           "16", "17", "18", "19", "21", "22", "23", "24"};
    auto expected = storage->Create(INT64, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->SubsetDims(_2DdimSpecs, {2, 3}, {5, 4}, destiny);
    vector<string> vec2 = {"13", "14", "18", "19", "23", "24", "28", "29"};
    expected = storage->Create(INT64, vec2);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->SubsetDims(_2DdimSpecs, {0, 0}, {5, 2}, destiny);
    vector<string> vec3 = {"0",  "1",  "2",  "5",  "6",  "7",
                           "10", "11", "12", "15", "16", "17",
                           "20", "21", "22", "25", "26", "27"};
    expected = storage->Create(INT64, vec3);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  SECTION("Testing 3D subsetting.") {
    storage->SubsetDims(_3DdimSpecs, {1, 0, 0}, {1, 2, 3}, destiny);
    vector<string> vec2 = {"35", "36", "37", "38", "40", "41",
                           "42", "43", "45", "46", "47", "48"};
    auto expected = storage->Create(INT64, vec2);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->SubsetDims(_3DdimSpecs, {1, 0, 0}, {2, 2, 3}, destiny);
    vector<string> vec3 = {"35", "36", "37", "38", "40", "41", "42", "43",
                           "45", "46", "47", "48", "70", "71", "72", "73",
                           "75", "76", "77", "78", "80", "81", "82", "83"};
    expected = storage->Create(INT64, vec3);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->SubsetDims(_3DdimSpecs, {0, 3, 3}, {2, 3, 3}, destiny);
    vector<string> vec4 = {"18", "53", "88"};
    expected = storage->Create(INT64, vec4);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->SubsetDims(_3DdimSpecs, {1, 2, 1}, {2, 4, 2}, destiny);
    vector<string> vec5 = {"46", "47", "51", "52", "56", "57",
                           "81", "82", "86", "87", "91", "92"};
    expected = storage->Create(INT64, vec5);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->SubsetDims(_3DdimSpecs, {0, 0, 1}, {2, 0, 2}, destiny);
    vector<string> vec6 = {"1", "2", "36", "37", "71", "72"};
    expected = storage->Create(INT64, vec6);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("Apply(string, DatasetPtr, DatasetPtr, DatasetPtr)",
          "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny = nullptr;

  vector<string> vec1 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds1 = storage->Create(DOUBLE, vec1);
  auto ds1v = storage->Create(DataType(DOUBLE, 5, 2), vec1);

  vector<string> vec2 = {"1", "4", "5", "6", "10",
                         "12", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds2 = storage->Create(DOUBLE, vec2);
  auto ds2v = storage->Create(DataType(DOUBLE, 5, 2), vec2);

  vector<string> vec3 = {"1", "4", "5.3", "4.2", "10.07",
                         "12.1", "3.2", "2.5", "6.7", "3.3",
                         "7.1", "2.3", "5.44", "9.2", "4.5",
                         "6.32", "5.6", "2.45", "8.7", "9.3"};
  auto ds3 = storage->Create(DOUBLE, vec3);
  auto ds3v = storage->Create(DataType(DOUBLE, 5, 2), vec3);


  SECTION("Testing apply in scalar typed datasets.") {
    storage->Apply(_ADDITION, ds1, ds2, destiny);
    vector<string> vec6 = {"2.2", "8", "10.7", "12.2", "20.09",
                           "24.01", "30.46", "39.48", "44.64", "54.64",
                           "66.36", "71.98", "74.92", "78.22", "80.24",
                           "84.24", "87.58", "93.66", "102.5", "110.78"};
    auto expected = storage->Create(DOUBLE, vec6);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_SUBTRACTION, ds1, ds2, destiny);
    vector<string> vec7 = {"0.2", "0.0", "0.7", "0.2", "0.09",
                           "0.01", "0.0", "0.0", "0.0", "0.0",
                           "0.0", "0.0", "0.0", "0.0", "0.0",
                           "0.0", "0.0", "0.0", "0.0", "0.0"};
    expected = storage->Create(DOUBLE, vec7);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_MULTIPLICATION, ds1, ds2, destiny);
    vector<string> vec8 = {"1.2", "16", "28.5", "37.2", "100.9",
                           "144.12", "231.9529", "389.6676", "498.1824", "746.3824",
                           "1100.9124", "1295.2801", "1403.2516", "1529.5921", "1609.6144",
                           "1774.0944", "1917.5641", "2193.0489", "2626.5625", "3068.0521"};
    expected = storage->Create(DOUBLE, vec8);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_DIVISION, ds1, ds3, destiny);
    vector<string> vec9 = {"1.200000000000", "1.000000000000", "1.075471698110",
                           "1.476190476190", "1.001986097320", "0.992561983471",
                           "4.759375000000", "7.896000000000", "3.331343283580",
                           "8.278787878790", "4.673239436620", "15.647826087000",
                           "6.886029411760", "4.251086956520", "8.915555555560",
                           "6.664556962030", "7.819642857140", "19.114285714300",
                           "5.890804597700", "5.955913978490"};
    expected = storage->Create(DOUBLE, vec9);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_POWER, ds3, ds3, destiny);
    vector<string> vec10 = {"1.000000000000",
                            "256.00000000000",
                            "6897.029902190000",
                            "414.616918601000",
                            "12603952809.885567",
                            "12638716749269.41",
                            "41.350420527900",
                            "9.882117688030",
                            "342532.02188176272",
                            "51.415729444100",
                            "1106456.3253842776",
                            "6.791630075250",
                            "10038.250159173849",
                            "735949494.91995394",
                            "869.873923381000",
                            "114956.07703515093",
                            "15482.922903100000",
                            "8.983734461390",
                            "149216722.90201455",
                            "1015994651.1580834"};
    expected = storage->Create(DOUBLE, vec10);
    REQUIRE(comparator.Compare(destiny, expected));
  }


  SECTION("Testing apply in vector typed datasets.") {
    storage->Apply(_ADDITION, ds1v, ds2v, destiny);
    vector<string> vec6 = {"10.7", "39.48", "74.92", "93.66"};
    auto expected = storage->Create(DOUBLE, vec6);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_SUBTRACTION, ds1v, ds2v, destiny);
    vector<string> vec7 = {"0.7", "0.0", "0.0", "0.0"};
    expected = storage->Create(DOUBLE, vec7);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_MULTIPLICATION, ds1v, ds2v, destiny);
    vector<string> vec8 = {"28.5", "389.6676", "1403.2516", "2193.0489"};
    expected = storage->Create(DOUBLE, vec8);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_DIVISION, ds1v, ds3v, destiny);
    vector<string> vec9 = {"1.075471698110", "7.896",
                           "6.88602941176", "19.1142857143"};
    expected = storage->Create(DOUBLE, vec9);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_POWER, ds3v, ds3v, destiny);
    vector<string> vec10 = {"6897.02990219", "9.88211768803",
                            "10038.250159173849", "8.98373446139"};
    expected = storage->Create(DOUBLE, vec10);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("Apply(string, DatasetPtr, Literal, DatasetPtr)",
          "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny = nullptr;

  vector<string> vec1 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds1 = storage->Create(DOUBLE, vec1);
  auto ds1v = storage->Create(DataType(DOUBLE, 5, 2), vec1);

  vector<string> vec2 = {"1", "4", "5", "6", "10",
                         "12", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds2 = storage->Create(DOUBLE, vec2);
  auto ds2v = storage->Create(DataType(DOUBLE, 5, 2), vec2);

  vector<string> vec3 = {"1", "4", "5.3", "4.2", "10.07",
                         "12.1", "3.2", "2.5", "6.7", "3.3",
                         "7.1", "2.3", "5.44", "9.2", "4.5",
                         "6.32", "5.6", "2.45", "8.7", "9.3"};
  auto ds3 = storage->Create(DOUBLE, vec3);
  auto ds3v = storage->Create(DataType(DOUBLE, 5, 2), vec3);


  SECTION("Testing apply in scalar typed datasets.") {
    storage->Apply(_ADDITION, ds1, Literal(3.7), destiny);
    vector<string> vec6 = {"4.900000000000",
                           "7.700000000000",
                           "9.400000000000",
                           "9.900000000000",
                           "13.790000000000",
                           "15.710000000000",
                           "18.930000000000",
                           "23.440000000000",
                           "26.020000000000",
                           "31.020000000000",
                           "36.880000000000",
                           "39.690000000000",
                           "41.160000000000",
                           "42.810000000000",
                           "43.820000000000",
                           "45.820000000000",
                           "47.490000000000",
                           "50.530000000000",
                           "54.950000000000",
                           "59.090000000000"};
    auto expected = storage->Create(DOUBLE, vec6);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_SUBTRACTION, ds1, Literal(3.7), destiny);
    vector<string> vec7 = {"-2.500000000000",
                           ".300000000000",
                           "2.000000000000",
                           "2.500000000000",
                           "6.390000000000",
                           "8.310000000000",
                           "11.530000000000",
                           "16.040000000000",
                           "18.620000000000",
                           "23.620000000000",
                           "29.480000000000",
                           "32.290000000000",
                           "33.760000000000",
                           "35.410000000000",
                           "36.420000000000",
                           "38.420000000000",
                           "40.090000000000",
                           "43.130000000000",
                           "47.550000000000",
                           "51.690000000000"};
    expected = storage->Create(DOUBLE, vec7);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_MULTIPLICATION, ds1, Literal(3.7), destiny);
    vector<string> vec8 = {"4.440000000000",
                           "14.800000000000",
                           "21.090000000000",
                           "22.940000000000",
                           "37.333000000000",
                           "44.437000000000",
                           "56.351000000000",
                           "73.038000000000",
                           "82.584000000000",
                           "101.084000000000",
                           "122.766000000000",
                           "133.163000000000",
                           "138.602000000000",
                           "144.707000000000",
                           "148.444000000000",
                           "155.844000000000",
                           "162.023000000000",
                           "173.271000000000",
                           "189.625000000000",
                           "204.943000000000"};
    expected = storage->Create(DOUBLE, vec8);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_DIVISION, ds1, Literal(3.7), destiny);
    vector<string> vec9 = {".324324324324",
                           "1.081081081080",
                           "1.540540540540",
                           "1.675675675680",
                           "2.727027027030",
                           "3.245945945950",
                           "4.116216216220",
                           "5.335135135140",
                           "6.032432432430",
                           "7.383783783780",
                           "8.967567567570",
                           "9.727027027030",
                           "10.124324324300",
                           "10.570270270300",
                           "10.843243243200",
                           "11.383783783800",
                           "11.835135135100",
                           "12.656756756800",
                           "13.851351351400",
                           "14.970270270300"};
    expected = storage->Create(DOUBLE, vec9);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_POWER, ds3, Literal(3.7), destiny);
    vector<string> vec10 = {"1.000000000000",
                            "168.897012579000",
                            "478.432548299000",
                            "202.312340554000",
                            "5142.911382390000",
                            "10146.257270344591",
                            "73.969880958800",
                            "29.674132536400",
                            "1138.876862780000",
                            "82.889892719200",
                            "1411.418775140000",
                            "21.796812747400",
                            "526.885114229000",
                            "3681.416556780000",
                            "261.147757564000",
                            "917.599545212000",
                            "586.537386783000",
                            "27.536866059100",
                            "2993.789319530000",
                            "3831.659509350000"};
    expected = storage->Create(DOUBLE, vec10);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply("sin", ds3, Literal(0.0), destiny);
    vector<string> vec11 = {".841470984808",
                            "-.756802495308",
                            "-.832267442224",
                            "-.871575772414",
                            "-.601375855190",
                            "-.449647464535",
                            "-.058374143428",
                            ".598472144104",
                            ".404849920617",
                            "-.157745694143",
                            ".728969040126",
                            ".745705212177",
                            "-.746765412868",
                            ".222889914100",
                            "-.977530117665",
                            ".036806377426",
                            "-.631266637872",
                            ".637764702135",
                            ".662969230082",
                            ".124454423507"};
    expected = storage->Create(DOUBLE, vec11);
    REQUIRE(comparator.Compare(destiny, expected));
  }


  SECTION("Testing apply in vector typed datasets.") {
    storage->Apply(_ADDITION, ds1v,  Literal(3.7), destiny);
    vector<string> vec6 = {"9.4", "23.44", "41.16", "50.53"};
    auto expected = storage->Create(DOUBLE, vec6);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_SUBTRACTION, ds1v,  Literal(3.7), destiny);
    vector<string> vec7 = {"2", "16.04", "33.76", "43.13"};
    expected = storage->Create(DOUBLE, vec7);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_MULTIPLICATION, ds1v,  Literal(3.7), destiny);
    vector<string> vec8 = {"21.09", "73.038", "138.602", "173.271"};
    expected = storage->Create(DOUBLE, vec8);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_DIVISION, ds1v,  Literal(3.7), destiny);
    vector<string> vec9 = {"1.54054054054", "5.33513513514",
                           "10.1243243243", "12.6567567568"};
    expected = storage->Create(DOUBLE, vec9);
    REQUIRE(comparator.Compare(destiny, expected));

    storage->Apply(_POWER, ds3v,  Literal(3.7), destiny);
    vector<string> vec10 = {"478.432548299", "29.6741325364",
                            "526.885114229", "27.5368660591/"};
    expected = storage->Create(DOUBLE, vec10);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("MaterializeDim", "[StorageManager]") {
  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny;
  vector<string> vec = {"1.2", "4.0", "5.7", "6.2", "10.09",
                        "12.01", "15.23", "19.74", "22.32", "27.32",
                        "33.18", "35.99", "37.46", "39.11", "40.12",
                        "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto explicitDs = storage->Create(DOUBLE, vec);
  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto implicitDim = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 1.1);

  SECTION("Testing materializing ordered dimension specifications.") {
    auto dimspecOrdered = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                              implicitDim, 10, 20, 100, 2);

    storage->MaterializeDim(dimspecOrdered, 22, destiny);
    vector<string> vec1 = {"11.0000",
                           "11.0000",
                           "12.1000",
                           "12.1000",
                           "13.2000",
                           "13.2000",
                           "14.3000",
                           "14.3000",
                           "15.4000",
                           "15.4000",
                           "16.5000",
                           "16.5000",
                           "17.6000",
                           "17.6000",
                           "18.7000",
                           "18.7000",
                           "19.8000",
                           "19.8000",
                           "20.9000",
                           "20.9000",
                           "22.0000",
                           "22.0000"};
    auto expected = storage->Create(DOUBLE, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    dimspecOrdered = make_shared<DimensionSpecification>(UNSAVED_ID,
                                                         explicitDim, 9, 15, 100, 3);

    storage->MaterializeDim(dimspecOrdered, 21, destiny);
    vector<string> vec2 = {"27.3200",
                           "27.3200",
                           "27.3200",
                           "33.1800",
                           "33.1800",
                           "33.1800",
                           "35.9900",
                           "35.9900",
                           "35.9900",
                           "37.4600",
                           "37.4600",
                           "37.4600",
                           "39.1100",
                           "39.1100",
                           "39.1100",
                           "40.1200",
                           "40.1200",
                           "40.1200",
                           "42.1200",
                           "42.1200",
                           "42.1200"};
    expected = storage->Create(DOUBLE, vec2);
    REQUIRE(comparator.Compare(destiny, expected));
  }


  SECTION("Testing materializing partial dimension specifications.") {


    vector<string> vecpart1 = {"11.0000",
                               "13.2000",
                               "18.7000",
                               "22.0000"};
    auto partialDs = storage->Create(DOUBLE, vecpart1);
    auto dimspecPartial = make_shared<DimensionSpecification>(UNSAVED_ID, implicitDim,
                                                              partialDs, 10, 20, 100, 2);
    storage->MaterializeDim(dimspecPartial, 8, destiny);
    vector<string> vec1 = {"11.0000",
                           "11.0000",
                           "13.2000",
                           "13.2000",
                           "18.7000",
                           "18.7000",
                           "22.0000",
                           "22.0000"};
    auto expected = storage->Create(DOUBLE, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    vector<string> vecpart2 = {"10",
                               "12",
                               "14",
                               "15"};
    partialDs = storage->Create(INT64, vecpart2);
    dimspecPartial = make_shared<DimensionSpecification>(UNSAVED_ID, explicitDim,
                                                         partialDs, 9, 15, 100, 3);

    storage->MaterializeDim(dimspecPartial, 12, destiny);
    vector<string> vec2 = {"33.18",
                           "33.18",
                           "33.18",
                           "37.46",
                           "37.46",
                           "37.46",
                           "40.12",
                           "40.12",
                           "40.12",
                           "42.12",
                           "42.12",
                           "42.12",};
    expected = storage->Create(DOUBLE, vec2);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  SECTION("Testing materializing total dimension specifications.") {

    vector<string> vecpart1 = {"11.0000",
                               "13.2000",
                               "18.7000",
                               "22.0000"};
    auto totalDs = storage->Create(DOUBLE, vecpart1);
    auto dimsSpecsTotal = make_shared<DimensionSpecification>(UNSAVED_ID, implicitDim,
                                                              totalDs, 0, 10);

    storage->MaterializeDim(dimsSpecsTotal, 4, destiny);
    vector<string> vec1 = {"11.0000",
                           "13.2000",
                           "18.7000",
                           "22.0000"};
    auto expected = storage->Create(DOUBLE, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    vector<string> vecpart2 = {"10",
                               "12",
                               "14",
                               "15"};
    totalDs = storage->Create(INT64, vecpart2);
    dimsSpecsTotal = make_shared<DimensionSpecification>(UNSAVED_ID, explicitDim,
                                                         totalDs, 9, 15);

    storage->MaterializeDim(dimsSpecsTotal, 4, destiny);
    vector<string> vec2 = {"33.18",
                           "37.46",
                           "40.12",
                           "42.12"};
    expected = storage->Create(DOUBLE, vec2);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("PartiatMaterializeDim", "[StorageManager]") {
  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny;
  DatasetPtr destinyReal;
  vector<string> vec = {"1.2",   "4.0",   "5.7",   "6.2",   "10.09",
                        "12.01", "15.23", "19.74", "22.32", "27.32",
                        "33.18", "35.99", "37.46", "39.11", "40.12",
                        "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto explicitDs = storage->Create(DOUBLE, vec);
  auto explicitDim = make_shared<Dimension>(0, "noname", explicitDs);
  auto implicitDim = make_shared<Dimension>(0, "noname", DOUBLE, 0, 100, 1.1);

  vector<string> vecFilter = {"0", "3", "5", "6", "7", "12"};
  auto filter = storage->Create(INT64, vecFilter);
  filter->HasIndexes() = true;

  vector<string> vecFilter2 = {"0", "3"};
  auto filter2 = storage->Create(INT64, vecFilter2);
  filter2->HasIndexes() = true;

  SECTION("Testing materializing ordered dimension specifications.") {
    auto dimspecOrdered = make_shared<DimensionSpecification>(
        UNSAVED_ID, implicitDim, 10, 20, 100, 2);

    storage->PartiatMaterializeDim(filter, dimspecOrdered, 22, destiny,
                                   destinyReal);
    vector<string> vec1 = {"11.0000", "12.1000", "13.2000",
                           "14.3000", "14.3000", "17.6000"};
    auto expected = storage->Create(DOUBLE, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    dimspecOrdered = make_shared<DimensionSpecification>(
        UNSAVED_ID, explicitDim, 9, 15, 100, 3);

    storage->PartiatMaterializeDim(filter, dimspecOrdered, 21, destiny,
                                   destinyReal);
    vector<string> vec2 = {"27.32", "33.18", "33.18",
                           "35.99", "35.99", "39.11"};
    expected = storage->Create(DOUBLE, vec2);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  SECTION("Testing materializing partial dimension specifications.") {

    vector<string> vecpart1 = {"11.0000", "13.2000", "18.7000", "22.0000"};
    auto partialDs = storage->Create(DOUBLE, vecpart1);
    auto dimspecPartial = make_shared<DimensionSpecification>(
        UNSAVED_ID, implicitDim, partialDs, 10, 20, 100, 2);
    storage->PartiatMaterializeDim(filter, dimspecPartial, 8, destiny,
                                   destinyReal);
    vector<string> vec1 = {"11.0000", "13.2000", "18.7000",
                           "22.0000", "22.0000", "18.7000"};
    auto expected = storage->Create(DOUBLE, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    vector<string> vecpart2 = {"10", "12", "14", "15"};
    partialDs = storage->Create(INT64, vecpart2);
    dimspecPartial = make_shared<DimensionSpecification>(
        UNSAVED_ID, explicitDim, partialDs, 9, 15, 100, 3);

    storage->PartiatMaterializeDim(filter, dimspecPartial, 12, destiny,
                                   destinyReal);
    vector<string> vec2 = {"33.1800", "37.4600", "37.4600",
                           "40.1200", "40.1200", "33.1800"};
    expected = storage->Create(DOUBLE, vec2);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  SECTION("Testing materializing total dimension specifications.") {

    vector<string> vecpart1 = {"11.0000", "13.2000", "18.7000", "22.0000"};
    auto totalDs = storage->Create(DOUBLE, vecpart1);
    auto dimsSpecsTotal = make_shared<DimensionSpecification>(
        UNSAVED_ID, implicitDim, totalDs, 0, 20);

    storage->PartiatMaterializeDim(filter2, dimsSpecsTotal, 4, destiny,
                                   destinyReal);
    vector<string> vec1 = {"11", "22"};
    auto expected = storage->Create(DOUBLE, vec1);
    REQUIRE(comparator.Compare(destiny, expected));

    vector<string> vecpart2 = {"10", "12", "14", "15"};
    totalDs = storage->Create(INT64, vecpart2);
    dimsSpecsTotal = make_shared<DimensionSpecification>(
        UNSAVED_ID, explicitDim, totalDs, 9, 15);

    storage->PartiatMaterializeDim(filter2, dimsSpecsTotal, 4, destiny,
                                   destinyReal);
    vector<string> vec2 = {"33.18", "42.12"};
    expected = storage->Create(DOUBLE, vec2);
    REQUIRE(comparator.Compare(destiny, expected));
  }

  delete builder;
}

TEST_CASE("Stretch", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny;
  vector<string> vec1 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds1 = storage->Create(DOUBLE, vec1);

  storage->Stretch(ds1, ds1->GetEntryCount(),  1, 2, destiny);
  vector<string> vec2 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39",
                         "1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto expected = storage->Create(DOUBLE, vec2);
  REQUIRE(comparator.Compare(destiny, expected));

  storage->Stretch(ds1, 5, 3, 1, destiny);
  vector<string> vec3 = {"1.2", "1.2", "1.2", "4.0", "4.0", "4.0",
                         "5.7", "5.7", "5.7", "6.2", "6.2", "6.2",
                         "10.09", "10.09", "10.09"};
  expected = storage->Create(DOUBLE, vec3);
  REQUIRE(comparator.Compare(destiny, expected));

  storage->Stretch(ds1, 5, 3, 3, destiny);
  vector<string> vec4 = {"1.2", "1.2", "1.2", "4.0", "4.0", "4.0",
                         "5.7", "5.7", "5.7", "6.2", "6.2", "6.2",
                         "10.09", "10.09", "10.09",
                         "1.2", "1.2", "1.2", "4.0", "4.0", "4.0",
                         "5.7", "5.7", "5.7", "6.2", "6.2", "6.2",
                         "10.09", "10.09", "10.09",
                         "1.2", "1.2", "1.2", "4.0", "4.0", "4.0",
                         "5.7", "5.7", "5.7", "6.2", "6.2", "6.2",
                         "10.09", "10.09", "10.09"};
  expected = storage->Create(DOUBLE, vec4);
  REQUIRE(comparator.Compare(destiny, expected));

  delete builder;
}

TEST_CASE("Match", "[StorageManager]") {
  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  DatasetComparator comparator(storage);
  DatasetPtr destiny1, destiny2;

  vector<string> vec1 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds1 = storage->Create(DOUBLE, vec1);

  vector<string> vec2 = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds2 = storage->Create(DOUBLE, vec2);

  vector<string> vec3 = {"1.2", "1.2", "6.7", "8.8", "10.09",
                         "13.01", "15.23", "15.23", "25.32", "28.32",
                         "33.18", "33.18", "33.18"};
  auto ds3 = storage->Create(DOUBLE, vec3);

  vector<string> vec4 = {"1.25", "1.27", "6.78", "8.89", "10.15",
                         "13.51", "15.83", "15.53", "25.82", "28.72",
                         "33.88", "33.98", "33.98"};
  auto ds4 = storage->Create(DOUBLE, vec4);


  storage->Match(ds1, ds2, destiny1, destiny2);
  vector<string> vec5 = {"0", "1", "2", "3", "4", "5", "6", "7", "8",
                         "9", "10", "11", "12", "13", "14", "15", "16",
                         "17", "18", "19"};
  auto expected1 = storage->Create(INT64, vec5);
  vector<string> vec6 = {"0", "1", "2", "3", "4", "5", "6", "7", "8",
                         "9", "10", "11", "12", "13", "14", "15", "16",
                         "17", "18", "19"};
  auto expected2 = storage->Create(INT64, vec6);
  REQUIRE(comparator.Compare(destiny1, expected1));
  REQUIRE(comparator.Compare(destiny2, expected2));

  storage->Match(ds1, ds3, destiny1, destiny2);
  vector<string> vec7 = {"0", "0", "4", "6", "6", "10", "10", "10"};
  expected1 = storage->Create(INT64, vec7);
  vector<string> vec8 = {"1", "0", "4", "7", "6", "12", "11", "10"};
  expected2 = storage->Create(INT64, vec8);
  REQUIRE(comparator.Compare(destiny1, expected1));
  REQUIRE(comparator.Compare(destiny2, expected2));

  DatasetPtr destiny1a, destiny2a;
  storage->Match(ds1, ds4, destiny1a, destiny2a);
  REQUIRE(destiny1a == nullptr);
  REQUIRE(destiny2a == nullptr);

  delete builder;
}