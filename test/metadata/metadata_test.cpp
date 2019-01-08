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
#include "../test_utils.h"

TEST_CASE("Test TARS storage", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  auto metadataComparator = make_shared<MetadataComparator>(storage);
  auto metadataManager = builder->BuildMetadaManager();

  TARSPtr tars1 = make_shared<TARS>();
  tars1->name = "default";
  metadataManager->SaveTARS(tars1);
  TARSPtr tars2 = metadataManager->GetTARS(tars1->id);
  REQUIRE(metadataComparator->Compare(tars1, tars2, 0));

  metadataManager->RemoveTARS(tars1);
  TARSPtr tars3 = metadataManager->GetTARS(tars1->id);
  REQUIRE(tars3 == nullptr);

  delete builder;
}

TEST_CASE("Test Type storage", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  auto metadataComparator = make_shared<MetadataComparator>(storage);
  auto metadataManager = builder->BuildMetadaManager();

  TARSPtr tars1 = make_shared<TARS>();
  tars1->name = "default";
  metadataManager->SaveTARS(tars1);

  TypePtr type1 = make_shared<Type>();
  type1->name = "fooType";

  RolePtr role1 = make_shared<Role>();
  role1->name = "r1";
  role1->is_mandatory = false;

  RolePtr role2 = make_shared<Role>();
  role2->name = "r2";
  role2->is_mandatory = true;
  type1->roles["r1"] = role1;
  type1->roles["r2"] = role2;


  metadataManager->SaveType(tars1, type1);
  auto type2 = metadataManager->GetTypeByName(tars1, type1->name);
  REQUIRE(metadataComparator->Compare(type1, type2, 0));
  type2 = nullptr;

  metadataManager->RemoveType(tars1, type1);
  auto type3 = metadataManager->GetTypeByName(tars1, type1->name);
  REQUIRE(type3 == nullptr);

  delete builder;
}


TEST_CASE("Test Dataset storage", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  auto metadataComparator = make_shared<MetadataComparator>(storage);
  auto datasetComparator = make_shared<DatasetComparator>(storage);
  auto metadataManager = builder->BuildMetadaManager();

  TARSPtr tars1 = make_shared<TARS>();
  tars1->name = "default";
  metadataManager->SaveTARS(tars1);

  vector<string> vec = {"1.2", "4.0", "5.7", "6.2", "10.09",
                         "12.01", "15.23", "19.74", "22.32", "27.32",
                         "33.18", "35.99", "37.46", "39.11", "40.12",
                         "42.12", "43.79", "46.83", "51.25", "55.39"};
  auto ds = storage->Create(DOUBLE, vec);
  metadataManager->SaveDataSet(tars1, ds);
  auto ds2 = metadataManager->GetDataSetByName(ds->GetName());
  REQUIRE(datasetComparator->Compare(ds, ds2));
  ds2 = nullptr;

  metadataManager->RemoveDataSet(tars1, ds);
  auto ds3 = metadataManager->GetDataSetByName(ds->GetName());
  REQUIRE(ds3 == nullptr);

  delete builder;
}


TEST_CASE("Test TAR storage", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  auto metadataComparator = make_shared<MetadataComparator>(storage);
  auto datasetComparator = make_shared<DatasetComparator>(storage);
  auto metadataManager = builder->BuildMetadaManager();

  TARSPtr tars1 = make_shared<TARS>();
  tars1->name = "default";
  metadataManager->SaveTARS(tars1);


  TARPtr tar = make_shared<TAR>(UNSAVED_ID, "fooTAR", nullptr);
  DimensionPtr dim1 = make_shared<Dimension>(UNSAVED_ID,
      "d1", DOUBLE, 0, 100, 2);
  DimensionPtr dim2 = make_shared<Dimension>(UNSAVED_ID,
                                             "d2", DOUBLE, 0, 100, 1);
  DimensionPtr dim3 = make_shared<Dimension>(UNSAVED_ID,
                                             "d3", DOUBLE, 0, 500, 0.5);
  tar->AddDimension(dim1);
  tar->AddDimension(dim2);
  tar->AddDimension(dim3);

  AttributePtr att1 = make_shared<Attribute>(UNSAVED_ID, "a1", DOUBLE);
  AttributePtr att2 = make_shared<Attribute>(UNSAVED_ID, "a2", INT64);
  AttributePtr att3 = make_shared<Attribute>(UNSAVED_ID, "a3", INT64);

  tar->AddAttribute(att1);
  tar->AddAttribute(att2);
  tar->AddAttribute(att3);

  metadataManager->SaveTAR(tars1, tar);
  auto tar2 = metadataManager->GetTARByName(tars1, tar->GetName());
  REQUIRE(metadataComparator->Compare(tar, tar2, 0));
  tar2 = nullptr;
  metadataManager->RemoveTar(tars1, tar);
  auto ds3 = metadataManager->GetTARByName(tars1, tar->GetName());
  REQUIRE(ds3 == nullptr);

  delete builder;
}


TEST_CASE("Test subTAR storage", "[StorageManager]") {

  auto builder = new MockModulesBuilder();
  auto storage = builder->BuildStorageManager();
  auto metadataComparator = make_shared<MetadataComparator>(storage);
  auto datasetComparator = make_shared<DatasetComparator>(storage);
  auto metadataManager = builder->BuildMetadaManager();

  TARSPtr tars1 = make_shared<TARS>();
  tars1->name = "default";
  metadataManager->SaveTARS(tars1);


  TARPtr tar = make_shared<TAR>(UNSAVED_ID, "fooTAR", nullptr);
  DimensionPtr dim1 = make_shared<Dimension>(UNSAVED_ID,
                                             "d1", DOUBLE, 0, 100, 2);
  DimensionPtr dim2 = make_shared<Dimension>(UNSAVED_ID,
                                             "d2", DOUBLE, 0, 100, 1);
  DimensionPtr dim3 = make_shared<Dimension>(UNSAVED_ID,
                                             "d3", DOUBLE, 0, 500, 0.5);
  tar->AddDimension(dim1);
  tar->AddDimension(dim2);
  tar->AddDimension(dim3);

  AttributePtr att1 = make_shared<Attribute>(UNSAVED_ID, "a1", DOUBLE);
  AttributePtr att2 = make_shared<Attribute>(UNSAVED_ID, "a2", INT64);
  AttributePtr att3 = make_shared<Attribute>(UNSAVED_ID, "a3", INT64);

  tar->AddAttribute(att1);
  tar->AddAttribute(att2);
  tar->AddAttribute(att3);


  SubtarPtr subtar = make_shared<Subtar>();
  subtar->SetTAR(tar);
  auto dimspecs1 = make_shared<DimensionSpecification>(UNSAVED_ID, dim1, 0,
      1, 12, 6);
  auto dimspecs2 = make_shared<DimensionSpecification>(UNSAVED_ID, dim2, 0,
                                                       1, 6, 3);
  auto dimspecs3 = make_shared<DimensionSpecification>(UNSAVED_ID, dim3, 0,
                                                       2, 3, 1);

  subtar->AddDimensionsSpecification(dimspecs1);
  subtar->AddDimensionsSpecification(dimspecs2);
  subtar->AddDimensionsSpecification(dimspecs3);

  vector<string> vec = {"1.2", "4.0", "5.7", "6.2", "10.09",
                        "12.01", "15.23", "19.74", "22.32", "27.32",
                        "33.18", "35.99"};
  auto ds = storage->Create(DOUBLE, vec);
  subtar->AddDataSet("a1", ds);
  subtar->AddDataSet("a2", ds);
  subtar->AddDataSet("a3", ds);

  metadataManager->SaveTAR(tars1, tar);
  metadataManager->SaveSubtar(tar, subtar);

  auto subtars1 = metadataManager->GetSubtars(tar);
  auto subtars2 = metadataManager->GetSubtars(tar->GetName());
  REQUIRE(subtars1.size() == 1);
  REQUIRE(subtars2.size() == 1);
  REQUIRE(metadataComparator->Compare(subtar,subtars2.front(), 0));

  metadataManager->RemoveSubtar(tar, subtar);
  subtars1 = metadataManager->GetSubtars(tar);
  REQUIRE(subtars1.size() == 0);

  delete builder;
}