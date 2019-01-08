#include <memory>

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

#include "include/viz_config.h"
#include "include/default_engine.h"
#include "include/viz.h"
#define HUNDRED_MS 100000
#define CATALYZE_OPERATOR "Catalyze Operator"

using namespace chrono;
using namespace SavimeViz;

SystemLoggerPtr systemLogger;
const char *invalid_param = "Invalid parameter for operator CATALYZE. Expected "
                            "CATALYZE(field_data_tar1, [geometry_tar], "
                            "[topology_tar], catalyst_script, output_path)";

void waitAll(const VizConfigPtr &vizConfig) {
  
  VizSync& sync = vizConfig->sync;
  sync.keepWorking = false;
  
  for(auto t = begin(sync.threads); t != end(sync.threads); ++t) {
    t->join();
  }
}



// Validation and configuration
//------------------------------------------------------------------------------
bool isFieldDataTar(const TARPtr &tar) {
  auto type = tar->GetType();
  if (!type)
    return false;
  return type->name == CATERSIAN_FIELD_DATA2D ||
         type->name == CATERSIAN_FIELD_DATA3D ||
         type->name == UNSTRUCTURED_FIELD_DATA;
}

bool isGeometryTar(const TARPtr &tar) {
  auto type = tar->GetType();
  if (!type)
    return false;
  return type->name == CATERSIAN_GE02D || type->name == CATERSIAN_GEO3D ||
         type->name == CATERSIAN_GEO;
}

bool isTopologyTar(const TARPtr &tar) {
  auto type = tar->GetType();
  if (!type)
    return false;
  return type->name == INCIDENCE_TOPOLOGY || type->name == ADJACENCY_TOPOLOGY;
}

void validateGeometryTar(const TARPtr &tar, const VizConfigPtr &vizConfig) {
  for (const auto &dim : tar->GetDimensions()) {
    auto roles = tar->GetRoles();
    if (roles.find(dim->GetName()) == roles.end())
      throw std::runtime_error("Unexpected dimension: " + dim->GetName() +
                               " in geometry TAR.");
  }

  for (auto entry : tar->GetRoles()) {
    auto elementName = entry.first;
    auto role = entry.second;

    if (role->name == SIMULATION_ROLE)
      vizConfig->schema.geoSimDimension = tar->GetDataElement(elementName);
    else if (role->name == TIME_ROLE)
      vizConfig->schema.geoTimeDimension = tar->GetDataElement(elementName);
    else if (role->name == X_ROLE)
      vizConfig->schema.spatialDims[X_DIM] = tar->GetDataElement(elementName);
    else if (role->name == Y_ROLE)
      vizConfig->schema.spatialDims[Y_DIM] = tar->GetDataElement(elementName);
    else if (role->name == Z_ROLE)
      vizConfig->schema.spatialDims[Z_DIM] = tar->GetDataElement(elementName);
    else if (role->name == COORDS_ROLE)
      vizConfig->schema.spatialCoords = tar->GetDataElement(elementName);
    else if (role->name == INDEX_ROLE)
      vizConfig->schema.geoIndexDimension = tar->GetDataElement(elementName);
  }

  if (vizConfig->schema.geoIndexDimension->GetDataType() != INT64) {
    throw std::runtime_error(
        "Index role for a Geometry TAR must be implemented by "
        "a LONG typed data element.");
  }

  if (vizConfig->schema.spatialCoords != nullptr) {
    if (vizConfig->schema.spatialCoords->GetDataType().type() != DOUBLE)
      throw std::runtime_error(
          "Coords role for a Geometry TAR must be implemented by "
          "a DOUBLE typed data element.");
  }
}

void validateTopologyTar(const TARPtr &tar, const VizConfigPtr &vizConfig) {
  for (const auto &dim : tar->GetDimensions()) {
    auto roles = tar->GetRoles();
    if (roles.find(dim->GetName()) == roles.end())
      throw std::runtime_error("unexpected dimension: " + dim->GetName() +
                               " in topology TAR.");
  }

  for (auto entry : tar->GetRoles()) {
    auto elementName = entry.first;
    auto role = entry.second;

    if (role->name == SIMULATION_ROLE)
      vizConfig->schema.topSimDimension = tar->GetDataElement(elementName);
    else if (role->name == TIME_ROLE)
      vizConfig->schema.topTimeDimension = tar->GetDataElement(elementName);
    else if (role->name == INCIDENT_ROLE)
      vizConfig->schema.topIncident = tar->GetDataElement(elementName);
    else if (role->name == INCIDENTEE_ROLE)
      vizConfig->schema.topIncidentee = tar->GetDataElement(elementName);
    else if (role->name == CELL_TYPE)
      vizConfig->schema.cellType = tar->GetDataElement(elementName);
  }

  if (vizConfig->schema.topIncident->GetDataType() != INT64) {
    throw std::runtime_error(
        "Incident role for Topology TAR must be implemented by "
        "a LONG/INT64 typed data element.");
  }

  if (vizConfig->schema.cellType->GetDataType() != INT32) {
    throw std::runtime_error(
        "Cell type role for Topology TAR must be implemented by "
        "a INT32 typed data element.");
  }
}

void validateFieldDataTar(const TARPtr &tar, const VizConfigPtr& vizConfig) {
  for (const auto &dim : tar->GetDimensions()) {
    auto roles = tar->GetRoles();
    //if (roles.find(dim->GetName()) == roles.end())
    //  throw std::runtime_error("unexpected dimension: " + dim->GetName() +
    //                           " in TAR.");
  }

  for (auto entry : tar->GetRoles()) {
    auto elementName = entry.first;
    auto role = entry.second;

    if (role->name == SIMULATION_ROLE)
      vizConfig->schema.simDimension = tar->GetDataElement(elementName);
    else if (role->name == TIME_ROLE)
      vizConfig->schema.timeDimension = tar->GetDataElement(elementName);
    else if (role->name == X_ROLE)
      vizConfig->schema.spatialDims[X_DIM] = tar->GetDataElement(elementName);
    else if (role->name == Y_ROLE)
      vizConfig->schema.spatialDims[Y_DIM] = tar->GetDataElement(elementName);
    else if (role->name == Z_ROLE)
      vizConfig->schema.spatialDims[Z_DIM] = tar->GetDataElement(elementName);
    else if (role->name == INDEX_ROLE)
      vizConfig->schema.index = tar->GetDataElement(elementName);
  }

  if (vizConfig->schema.index != nullptr) {
    if (vizConfig->schema.index->GetDataType() != INT64) {
      throw std::runtime_error(
          "Index role for field data TAR must be implemented by "
          "a LONG typed data element.");
    }

    if (vizConfig->schema.index->GetType() != DIMENSION_SCHEMA_ELEMENT) {
      throw std::runtime_error(
          "Index role for field data TAR must be implemented as "
          "a dimension.");
    }
  }
}

VizConfigPtr
createVizConfiguration(const OperationPtr &operation,
                       const ConfigurationManagerPtr &configurationManager,
                       const StorageManagerPtr &storageManager) {

  VizConfigPtr vizConfig = std::make_shared<VizConfiguration>();
  int32_t paramCount = 1;
  bool expectingOutdir = true;
  bool expectingGeo = true;

  auto firstParam = operation->GetParametersByName(OPERAND(0));
  if (firstParam == nullptr || firstParam->type != TAR_PARAM)
    throw std::runtime_error(invalid_param);

  auto fieldTar = firstParam->tar;
  if (!isFieldDataTar(fieldTar))
    throw std::runtime_error("Invalid field TAR. Incorrect type.");

  vizConfig->schema.fieldData = fieldTar;
  vizConfig->numCores = configurationManager->GetIntValue(MAX_THREADS);
  
  vizConfig->catalystExecutable =
      configurationManager->GetStringValue(CATALYST_EXECUTABLE);
  
  while (true) {
    auto param = operation->GetParametersByName(OPERAND(paramCount++));
    if (param == nullptr || paramCount > 5)
      break;

    if (param->type == LITERAL_STRING_PARAM) {
      if (expectingOutdir) {
        expectingOutdir = false;
        string dir = param->literal_str;
        dir.erase(std::remove(dir.begin(), dir.end(), '"'), dir.end());
        vizConfig->outputDir = dir + _FILE_SEPARATOR;

        if (!EXIST_FILE(vizConfig->outputDir))
          throw std::runtime_error("The parameter " + dir +
                                   " is neither "
                                   "a TAR nor a valid directory.");
      } else {
        string scr = param->literal_str;
        scr.erase(std::remove(scr.begin(), scr.end(), '"'), scr.end());
        vizConfig->catalistScript = scr;
      }
    } else if (param->type == TAR_PARAM) {
      if (expectingGeo) {
        expectingGeo = false;
        auto geoTar = param->tar;
        if (!isGeometryTar(geoTar))
          throw std::runtime_error("Invalid geometry TAR, incorrect type.");
        vizConfig->schema.geometry = geoTar;
      } else {
        auto topTar = param->tar;
        if (!isTopologyTar(topTar))
          throw std::runtime_error("Invalid topology TAR, incorrect type.");
        vizConfig->schema.topology = topTar;
      }
    } else {
      throw std::runtime_error(invalid_param);
    }
  }

  if (vizConfig->outputDir.empty())
    throw std::runtime_error("Output directory not informed.");

  if (!vizConfig->catalistScript.empty()) {
    if (!EXIST_FILE(vizConfig->catalistScript))
      throw std::runtime_error("Catalyst script " + vizConfig->catalistScript +
                               " not found.");
  }

  if (vizConfig->schema.fieldData->GetType()->name == UNSTRUCTURED_FIELD_DATA &&
      vizConfig->schema.geometry == nullptr) {
    throw std::runtime_error("Unstructured field data detected, but no "
                             "geometry TAR specified.");
  }
  
  vizConfig->sync.keepWorking = true;
  for(int32_t i = 0; i < vizConfig->numCores; i++){
    std::thread t = std::thread(&processGridLoop, vizConfig, storageManager);
    vizConfig->sync.threads.push_back(std::move(t));
  }
  
  return vizConfig;
}

// Aux functions
//------------------------------------------------------------------------------
Mapping createMapping(const VizConfigPtr& vizConfig, const StorageManagerPtr& storageManager,
                      const SubtarPtr& subtar, const DatasetPtr& filter) {

#define LOWER(X) X->GetLowerBound()
#define UPPER(X) X->GetUpperBound()

  VizSchema &schema = vizConfig->schema;

  if (vizConfig->type != IMAGE && vizConfig->type != STRUCTURED)
    return nullptr;

  DatasetPtr spatialDatasets[DIM3], aux[DIM3];
  int64_t totalLen = subtar->GetFilledLength();
  bool is3D = schema.spatialDims[Z_DIM] != nullptr;

  string spatialDimNames[] = {schema.spatialDims[X_DIM]->GetName(),
                              schema.spatialDims[Y_DIM]->GetName(),
                              (is3D) ? schema.spatialDims[Z_DIM]->GetName()
                                     : ""};

  savime_size_t spatialDimLen[] = {
      schema.spatialDims[X_DIM]->GetDimension()->GetCurrentLength(),
      schema.spatialDims[Y_DIM]->GetDimension()->GetCurrentLength(),
      (is3D) ? schema.spatialDims[Z_DIM]->GetDimension()->GetCurrentLength()
             : 1};

  DimSpecPtr spatialDimSpecs[] = {
      subtar->GetDimensionSpecificationFor(spatialDimNames[X_DIM]),
      subtar->GetDimensionSpecificationFor(spatialDimNames[Y_DIM]),
      subtar->GetDimensionSpecificationFor(spatialDimNames[Z_DIM])};

  int64_t preamble0 = spatialDimLen[Y_DIM];
  int64_t preamble1 = spatialDimLen[Y_DIM] * spatialDimLen[Z_DIM];
  int64_t preamble2 = spatialDimLen[Z_DIM];

  if (filter) {
    DatasetHandlerPtr handlerX, handlerY, handlerZ;
    int64_t *bufferX, *bufferY, *bufferZ;
    int32_t dimNum = (is3D) ? DIM3 : DIM2;

    for (int32_t i = 0; i < dimNum; i++) {
      storageManager->PartiatMaterializeDim(
          filter, spatialDimSpecs[i], totalLen, aux[i], spatialDatasets[i]);
    }
    int64_t length = spatialDatasets[X_DIM]->GetEntryCount();

    handlerX = storageManager->GetHandler(spatialDatasets[0]);
    bufferX = (int64_t *)handlerX->GetBuffer();

    handlerY = storageManager->GetHandler(spatialDatasets[1]);
    bufferY = (int64_t *)handlerY->GetBuffer();

    if (is3D) {
      handlerZ = storageManager->GetHandler(spatialDatasets[2]);
      bufferZ = (int64_t *)handlerZ->GetBuffer();
    }

    Mapping mapping = CREATE_MAPPING();
    mapping->resize(length);

    omp_set_num_threads(vizConfig->numCores);
    if (is3D) {
#pragma omp parallel for
      for (int64_t i = 0; i < length; i++) {
        int64_t pos =
            bufferX[i] * preamble1 + bufferY[i] * preamble2 + bufferZ[i];
        (*mapping)[i] = pos;
      }
    } else {
#pragma omp parallel for
      for (int64_t i = 0; i < length; i++) {
        int64_t pos = bufferX[i] * preamble0 + bufferY[i];
        (*mapping)[i] = pos;
      }
    }

    handlerX->Close();
    handlerY->Close();
    if (is3D)
      handlerZ->Close();

    return mapping;

  } else {

    int64_t i = 0;
    Mapping mapping = CREATE_MAPPING();
    mapping->resize(totalLen);

    omp_set_num_threads(vizConfig->numCores);

    if (is3D) {

#pragma omp parallel for collapse(3)
      for (int64_t x = LOWER(spatialDimSpecs[0]);
           x <= UPPER(spatialDimSpecs[0]); x++) {
        for (int64_t y = LOWER(spatialDimSpecs[1]);
             y <= UPPER(spatialDimSpecs[1]); y++) {
          for (int64_t z = LOWER(spatialDimSpecs[2]);
               z <= UPPER(spatialDimSpecs[2]); z++) {
            int64_t pos = x * preamble1 + y * preamble2 + z;
            (*mapping)[i++] = pos;
          }
        }
      }

    } else {

#pragma omp parallel for collapse(2)
      for (int64_t x = LOWER(spatialDimSpecs[0]);
           x <= UPPER(spatialDimSpecs[0]); x++) {
        for (int64_t y = LOWER(spatialDimSpecs[1]);
             y <= UPPER(spatialDimSpecs[1]); y++) {
          int64_t pos = x * preamble0 + y;
          (*mapping)[i++] = pos;
        }
      }
    }

    return mapping;
  }
}

// TODO[HERMANO]:Make this function more efficient
unordered_map<string, DatasetPtr>
sliceSubtars(const DataElementPtr &simDimension, const DataElementPtr& timeDimension,
             const StorageManagerPtr &storageManager, const SubtarPtr &subtar,
             int64_t simulation, int64_t time, DatasetPtr &filter) {

  unordered_map<string, DatasetPtr> datasets;
  DimSpecPtr simSpecs, timeSpecs;
  bool hasManySims = simulation != NONE;
  bool hasManyTimeSteps = time != NONE;

  for (auto entry : subtar->GetDataSets()) {
    string attributeName = entry.first;
    DatasetPtr dataset = entry.second;
    DatasetPtr filteredDataset = dataset;
    DatasetPtr filterTime, filterSim;
    double logicalTimeIndex, logicalSimIndex = 0;

    if (hasManySims && hasManyTimeSteps) {
      simSpecs = subtar->GetDimensionSpecificationFor(simDimension->GetName());
      timeSpecs =
          subtar->GetDimensionSpecificationFor(timeDimension->GetName());
      Literal _sim = storageManager->Real2Logical(simDimension->GetDimension(),
                                                  simulation);
      GET_LITERAL(logicalSimIndex, _sim, double);
      storageManager->ComparisonDim(string(_EQ), simSpecs,
                                    subtar->GetFilledLength(), logicalSimIndex,
                                    filterSim);
      Literal _time =
          storageManager->Real2Logical(timeDimension->GetDimension(), time);
      GET_LITERAL(logicalTimeIndex, _time, double);
      storageManager->ComparisonDim(string(_EQ), timeSpecs,
                                    subtar->GetFilledLength(), logicalTimeIndex,
                                    filterTime);
      storageManager->And(filterSim, filterTime, filter);
      storageManager->Filter(dataset, filter, filteredDataset);
      datasets[attributeName] = filteredDataset;
      assert(filteredDataset->GetEntryCount() != 0);
    } else if (hasManySims) {
      simSpecs = subtar->GetDimensionSpecificationFor(simDimension->GetName());
      Literal _sim = storageManager->Real2Logical(simDimension->GetDimension(),
                                                  simulation);
      GET_LITERAL(logicalSimIndex, _sim, double);
      storageManager->ComparisonDim(string(_EQ), simSpecs,
                                    subtar->GetFilledLength(), logicalSimIndex,
                                    filterSim);
      filter = filterSim;
      storageManager->Filter(dataset, filter, filteredDataset);
      datasets[attributeName] = filteredDataset;
      assert(filteredDataset->GetEntryCount() != 0);
    } else if (hasManyTimeSteps) {
      timeSpecs =
          subtar->GetDimensionSpecificationFor(timeDimension->GetName());
      Literal _time =
          storageManager->Real2Logical(timeDimension->GetDimension(), time);
      GET_LITERAL(logicalTimeIndex, _time, double);
      storageManager->ComparisonDim(string(_EQ), timeSpecs,
                                    subtar->GetFilledLength(), logicalTimeIndex,
                                    filterTime);
      filter = filterTime;
      storageManager->Filter(dataset, filterTime, filteredDataset);
      datasets[attributeName] = filteredDataset;
      assert(filteredDataset->GetEntryCount() != 0);
    } else {
      datasets[attributeName] = dataset;
    }
  }

  return datasets;
}

// Grids initialization
//------------------------------------------------------------------------------
void processGeometryForUnstructuredGrid(const VizConfigPtr& vizConfig,
                                        const StorageManagerPtr& storageManager,
                                        const DefaultEnginePtr& defaultEngine,
                                        const string& geoSimName, string& geoTimeName,
                                        const string& indexElementName) {
#define INVALID -1

  VizSchema &schema = vizConfig->schema;
  VizData &data = vizConfig->data;
  DatasetPtr interleavedCoord;
  string geoTarName = schema.geometry->GetName();
  int64_t geoSubtarCount = 0, simulation = 0, timeStep = 0;
  int32_t components = 0;
  auto geoGeneretor = defaultEngine->GetGenerators()[geoTarName];
  bool is3D = schema.spatialDims[Z_DIM] != nullptr;

  // Creating geometry
  while (true) {
    DatasetPtr spatialDatasets[DIM3], indexDataset;
    int64_t totalLen = 0;
    void *coordsFinalBuffer;

    auto subtar = geoGeneretor->GetSubtar(geoSubtarCount);
    if (subtar == nullptr)
      break;
    totalLen = subtar->GetFilledLength();

    DimSpecPtr simSpecs = subtar->GetDimensionSpecificationFor(geoSimName);
    if (simSpecs != nullptr) {
      if (simSpecs->GetFilledLength() != 1)
        throw std::runtime_error("Invalid geometry TAR, a subTAR should "
                                 "not contain the geometry for more than "
                                 "one simulation.");
      simulation = simSpecs->GetLowerBound();
    }

    DimSpecPtr timSpecs = subtar->GetDimensionSpecificationFor(geoTimeName);
    if (timSpecs != nullptr) {
      if (timSpecs->GetFilledLength() != 1)
        throw std::runtime_error("Invalid geometry TAR, a subTAR should "
                                 " not contain the geometry for more than "
                                 " one time step.");
      timeStep = timSpecs->GetLowerBound();
    }

    DimSpecPtr dimSpecs =
        subtar->GetDimensionSpecificationFor(indexElementName);
    if (dimSpecs != nullptr) {
      storageManager->MaterializeDim(dimSpecs, totalLen, indexDataset);
    } else {
      indexDataset = subtar->GetDataSetFor(indexElementName);
    }

    int64_t numberOfPoints = indexDataset->GetEntryCount();

    if (schema.spatialCoords == nullptr) {
      string spatialDataElementNames[DIM3] = {
          schema.spatialDims[X_DIM]->GetName(),
          schema.spatialDims[Y_DIM]->GetName(),
          (is3D) ? schema.spatialDims[Z_DIM]->GetName() : ""};

      for (int32_t i = 0; i < DIM3; i++) {
        dimSpecs =
            subtar->GetDimensionSpecificationFor(spatialDataElementNames[i]);

        if (dimSpecs != nullptr) {
          bool wholeExtent = dimSpecs->GetFilledLength() ==
                             dimSpecs->GetDimension()->GetLength();
          if (!wholeExtent)
            throw std::runtime_error("Invalid geometry TAR, a single "
                                     "subTAR must contain the "
                                     "definition of the entire geometry "
                                     "for a simulation and time step.");

          storageManager->MaterializeDim(dimSpecs, totalLen,
                                         spatialDatasets[i]);
        } else {
          spatialDatasets[i] =
              subtar->GetDataSetFor(spatialDataElementNames[i]);
        }
      }

      double spacings[DIM3];
      int32_t offsets[DIM3] = {0, 1, 2};

      if (is3D) {
        components = DIM3;
        spacings[X_DIM] = spacings[Y_DIM] = spacings[Z_DIM] = DIM3;
        interleavedCoord = storageManager->Create(DOUBLE, numberOfPoints * 3);

        if (interleavedCoord == nullptr)
          throw std::runtime_error(
              "Could not create dataset for storing interleaved coords.");

      } else {
        components = DIM2;
        spacings[X_DIM] = spacings[Y_DIM] = DIM2;
        interleavedCoord = storageManager->Create(DOUBLE, numberOfPoints * 2);

        if (interleavedCoord == nullptr)
          throw std::runtime_error(
              "Could not create dataset for storing interleaved coords.");
      }

      for (int32_t i = 0; i < DIM3; i++) {
        DatasetPtr matDim = spatialDatasets[i];
        if (matDim) {
          storageManager->Copy(matDim, 0, matDim->GetEntryCount() - 1, offsets[i],
                               spacings[i], interleavedCoord);
        }
      }

      DatasetHandlerPtr interleavedCoordsHandler =
          storageManager->GetHandler(interleavedCoord);
      coordsFinalBuffer = COPY_FROM_MMAP(interleavedCoordsHandler);
      interleavedCoordsHandler->Close();
    } else {
      components = schema.spatialCoords->GetDataType().vectorLength();
      interleavedCoord = subtar->GetDataSetFor(schema.spatialCoords->GetName());
      DatasetHandlerPtr interleavedCoordsHandler =
          storageManager->GetHandler(interleavedCoord);

      if (components == DIM2) {
        /*We must pad each points coords with an extra 0.0*/
        int64_t paddedCoordsSize = interleavedCoord->GetEntryCount() * 3;
        DatasetPtr paddedCoords =
            storageManager->Create(DOUBLE, paddedCoordsSize);

        if (paddedCoords == nullptr)
          throw std::runtime_error(
              "Could not create dataset for padding 2D coords.");

        DatasetHandlerPtr paddedCoordsHandler =
            storageManager->GetHandler(paddedCoords);

        double *interleavedCoordBuffer =
            (double *)interleavedCoordsHandler->GetBuffer();
        double *paddedCoordsBuffer = (double *)paddedCoordsHandler->GetBuffer();

        auto interleavedCoordEntryCount = interleavedCoord->GetEntryCount();
#pragma omp parallel for
        for (int64_t i = 0; i <  interleavedCoordEntryCount; i++) {
          paddedCoordsBuffer[i * DIM3] = interleavedCoordBuffer[i * DIM2];
          paddedCoordsBuffer[i * DIM3 + 1] =
              interleavedCoordBuffer[i * DIM2 + 1];
          paddedCoordsBuffer[i * DIM3 + 2] = 0.0;
        }

        coordsFinalBuffer = COPY_FROM_MMAP(paddedCoordsHandler);
        paddedCoordsHandler->Close();
      } else {
        coordsFinalBuffer = COPY_FROM_MMAP(interleavedCoordsHandler);
      }

      interleavedCoordsHandler->Close();
    }

    if (numberOfPoints <= 0)
      throw std::runtime_error("Invalid geometry TAR: undefined geometry for "
                               "some time steps and/or simulations.");

    /*Configuring VtkPoints*/
    auto pointsArray = vtkSmartPointer<vtkDoubleArray>::New();
    pointsArray->SetNumberOfComponents(DIM3);
    pointsArray->SetArray((double *)coordsFinalBuffer, numberOfPoints * 3, 0);
    vtkSmartPointer<vtkPoints> points = vtkSmartPointer<vtkPoints>::New();
    points->SetData(pointsArray);

    /*Creating grid and setting grid attributes*/
    auto grid = vtkSmartPointer<vtkUnstructuredGrid>::New();
    grid->SetPoints(points);
    data.grids[simulation][timeStep] = grid;
    data.totalCells[simulation][timeStep] = numberOfPoints;

    int64_t mappingLen, maxVal = 0;
    DatasetHandlerPtr indexHandler = storageManager->GetHandler(indexDataset);
    int64_t *indexBuffer = (int64_t *)indexHandler->GetBuffer();

    auto indexDatasetCount = indexDataset->GetEntryCount();
    /*Getting max index*/
    omp_set_num_threads(vizConfig->numCores);
#pragma omp parallel for reduction(max : maxVal)
    for (int64_t idx = 0; idx < indexDatasetCount; idx++)
      maxVal = maxVal > indexBuffer[idx] ? maxVal : indexBuffer[idx];
    mappingLen = maxVal + 1;
    
    Mapping mapping = CREATE_MAPPING();
    mapping->resize(mappingLen);

#pragma omp parallel for
    for (int64_t i = 0; i < mappingLen; i++) {
      (*mapping)[i] = INVALID;
    }
    
#pragma omp parallel for
    for (int64_t i = 0; i < indexDataset->GetEntryCount(); i++) {
      (*mapping)[indexBuffer[i]] = i;
    }

    data.mapping[simulation][timeStep] = mapping;
    indexHandler->Close();

    geoGeneretor->TestAndDisposeSubtar(geoSubtarCount++);
  }
}

void processTopologyForUnstructuredGrid(const VizConfigPtr& vizConfig,
                                        const StorageManagerPtr& storageManager,
                                        const DefaultEnginePtr& defaultEngine,
                                        const string& topSimName, const string& topTimeName) {
#define INVALID -1

  VizSchema &schema = vizConfig->schema;
  VizData &data = vizConfig->data;

  int32_t topSubtarCount = 0;
  int64_t simulation = 0, timeStep = 0;
  string topTarName = schema.topology->GetName();
  auto topGeneretor = defaultEngine->GetGenerators()[topTarName];

  // Creating topology
  while (true) {
    auto subtar = topGeneretor->GetSubtar(topSubtarCount);
    if (subtar == nullptr)
      break;

    DimSpecPtr simSpecs = subtar->GetDimensionSpecificationFor(topSimName);
    if (simSpecs != nullptr) {
      if (simSpecs->GetFilledLength() != 1)
        throw std::runtime_error("Invalid topology TAR, a subTAR should "
                                 "not contain the topology for more "
                                 "than one simulation.");

      simulation = simSpecs->GetLowerBound();
    }

    DimSpecPtr timSpecs = subtar->GetDimensionSpecificationFor(topTimeName);
    if (timSpecs != nullptr) {
      if (timSpecs->GetFilledLength() != 1)
        throw std::runtime_error("Invalid topology TAR, a subTAR should "
                                 "not contain the topology for more "
                                 "than one time step");

      timeStep = timSpecs->GetLowerBound();
    }
    
    auto grid = vizConfig->getGrid(simulation, timeStep);
    auto map = vizConfig->getMapping(simulation, timeStep);
    if (grid == nullptr || map == nullptr){
      topGeneretor->TestAndDisposeSubtar(topSubtarCount++);
      continue;
    }

    Mapping mapping = vizConfig->getMapping(simulation, timeStep);
    
    /*Getting schema names*/
    auto unstructuredGrid = vtkUnstructuredGrid::SafeDownCast(grid.Get());
    auto incidentName = schema.topIncident->GetName();
    auto incidenteeName = schema.topIncidentee->GetName();
    auto cellTypesName = schema.cellType->GetName();

    /*Getting datasets and dimspecs*/
    DatasetPtr incidentDs = subtar->GetDataSetFor(incidentName);
    DimSpecPtr incidenteeDimSpecs =
        subtar->GetDimensionSpecificationFor(incidenteeName);
    DatasetPtr inputTypesDs = subtar->GetDataSetFor(cellTypesName);
    int64_t numCells = incidenteeDimSpecs->GetFilledLength();
    int64_t entriesPerCell = incidentDs->GetType().vectorLength();
    DatasetPtr cellArrayDs =
        storageManager->Create(INT64, numCells * entriesPerCell);
    DatasetPtr cellTypesDs = storageManager->Create(INT32, numCells);

    /*Getting handlers*/
    DatasetHandlerPtr topologyHandler = storageManager->GetHandler(incidentDs);
    DatasetHandlerPtr cellArrayHandler =
        storageManager->GetHandler(cellArrayDs);
    DatasetHandlerPtr cellTypesHandler =
        storageManager->GetHandler(cellTypesDs);
    DatasetHandlerPtr inputCellTypesHandler =
        storageManager->GetHandler(inputTypesDs);

    // data.handlersToClose.push_back(cellArrayHandler);
    // data.handlersToClose.push_back(cellTypesHandler);

    /*Buffers*/
    // TODO[HERMANO]:Make topology indexing types more flexible
    int64_t *topologyBufer = (int64_t *)topologyHandler->GetBuffer();
    int64_t *cellArrayBuffer = (int64_t *)cellArrayHandler->GetBuffer();
    int32_t *types = (int32_t *)cellTypesHandler->GetBuffer();
    int32_t *inputTypes = (int32_t *)inputCellTypesHandler->GetBuffer();

    omp_set_num_threads(vizConfig->numCores);

#pragma omp parallel for
    for (int64_t i = 0; i < numCells; i++) {
      int64_t offset = i * entriesPerCell;
      int64_t destOffset = i * (entriesPerCell);
      int64_t destOffsetPoints = destOffset + 1;
      int64_t cellType = inputTypes[i];
      int64_t numPoints = topologyBufer[offset];
      int64_t lbPoints = offset + 1;
      int64_t upEntry = entriesPerCell - 1;

      cellArrayBuffer[destOffset] = numPoints;

      for (int64_t j = 0; j < upEntry; j++) {
        int64_t pos = destOffsetPoints + j;
        int64_t pos_top = lbPoints + j;

        if (IN_RANGE(topologyBufer[pos_top], 0, mapping->size()))
          cellArrayBuffer[pos] = (*mapping)[topologyBufer[pos_top]];
        else
          cellArrayBuffer[pos] = INVALID;

        bool invalid = cellArrayBuffer[pos] == INVALID && j < numPoints;
        if (invalid)
          cellType = 0;
      }

      types[i] = cellType;
    }

    /*Create array with cells*/
    auto idArrays = vtkSmartPointer<vtkIdTypeArray>::New();
    void *localBufferCellArray = COPY_FROM_MMAP(cellArrayHandler);
    idArrays->SetArray((vtkIdType *)localBufferCellArray,
                       numCells * entriesPerCell, 0);

    auto cellArray = vtkSmartPointer<vtkCellArray>::New();
    cellArray->SetCells(numCells, idArrays);
    unstructuredGrid->SetCells(types, cellArray);

    /*Closing handlers*/
    topologyHandler->Close();
    cellArrayHandler->Close();
    cellTypesHandler->Close();
    inputCellTypesHandler->Close();

    topGeneretor->TestAndDisposeSubtar(topSubtarCount++);
  }
}

void initializeUnstructuredGrids(const VizConfigPtr& vizConfig,
                                 const StorageManagerPtr& storageManager,
                                 const DefaultEnginePtr& defaultEngine) {

  VizSchema &schema = vizConfig->schema;
  string geoTimeName, geoSimName, topTimeName, topSimName;

  vizConfig->multiplicityType = SINGLE_GRID;
  vizConfig->type = UNSTRUCTURED;

  if (schema.geoSimDimension != nullptr) {
    geoSimName = schema.geoSimDimension->GetName();
    vizConfig->multiplicityType = SINGLE_TIME;
  }

  if (schema.geoTimeDimension != nullptr) {
    geoTimeName = schema.geoTimeDimension->GetName();
    if (vizConfig->multiplicityType == SINGLE_GRID)
      vizConfig->multiplicityType = SINGLE_SIM;
    else
      vizConfig->multiplicityType = MULTIPLE;
  }

  if (schema.topSimDimension != nullptr) {
    topSimName = schema.topSimDimension->GetName();
  }

  if (schema.topTimeDimension != nullptr) {
    topTimeName = schema.topTimeDimension->GetName();
  }

  string indexElementName = schema.geoIndexDimension->GetName();

  /*Process Geometry TAR*/
  processGeometryForUnstructuredGrid(vizConfig, storageManager, defaultEngine,
                                     geoSimName, geoTimeName, indexElementName);

  /*Process Topology TAR*/
  processTopologyForUnstructuredGrid(vizConfig, storageManager, defaultEngine,
                                     topSimName, topTimeName);
}

void initializeStructuredGrids(const VizConfigPtr& vizConfig,
                               const StorageManagerPtr& storageManager,
                               DataElementPtr dimensions[DIM3],
                               const int64_t lens[DIM3], int64_t simulationsNumber,
                               int64_t timeStepNumber) {

  VizData &data = vizConfig->data;

  int64_t lenX = lens[X_DIM], lenY = lens[Y_DIM], lenZ = lens[Z_DIM];
  DataElementPtr xDim = dimensions[X_DIM], yDim = dimensions[Y_DIM],
                 zDim = dimensions[Z_DIM];

  vtkSmartPointer<vtkDoubleArray> pointsArray =
      vtkSmartPointer<vtkDoubleArray>::New();
  DatasetPtr interleavedCoord;
  DataElementPtr dims[] = {xDim, yDim, zDim};
  int32_t components = 0;
  double spacings[DIM3];
  int32_t offsets[DIM3] = {0, 1, 2};
  double adjcency[DIM3] = {(double)(lenY * lenZ), (double)lenZ, 1};
  double stride[DIM3] = {(double)(lenX * lenY * lenZ), (double)(lenY * lenZ),
                       (double)lenZ};

  if (zDim) {
    components = DIM3;
    spacings[X_DIM] = spacings[Y_DIM] = spacings[Z_DIM] = DIM3;
    interleavedCoord = storageManager->Create(DOUBLE, lenX * lenY * lenZ * 3);
  } else {
    components = DIM2;
    spacings[X_DIM] = spacings[Y_DIM] = DIM2;
    interleavedCoord = storageManager->Create(DOUBLE, lenX * lenY * 2);
  }

  for (int32_t i = 0; i < DIM3; i++) {
    auto element = dims[i];
    DatasetPtr matDim;
    int64_t totalLen = lenX * lenY * lenZ;

    if (element) {
      auto dim = element->GetDimension();
      DimSpecPtr dimsSpecs =
          make_shared<DimensionSpecification>(UNSAVED_ID, dim,
              0, dim->GetRealUpperBound(), stride[i], adjcency[i]);

      storageManager->MaterializeDim(dimsSpecs, totalLen, matDim);
      storageManager->Copy(matDim, 0, matDim->GetEntryCount() - 1, offsets[i],
                           spacings[i], interleavedCoord);
    }
  }

  DatasetHandlerPtr handler = storageManager->GetHandler(interleavedCoord);
  data.handlersToClose.push_back(handler);
  pointsArray->SetNumberOfComponents(components);
  pointsArray->SetArray((double *)handler->GetBuffer(),
                        interleavedCoord->GetEntryCount(), 1);
  vtkSmartPointer<vtkPoints> points = vtkSmartPointer<vtkPoints>::New();
  points->SetData(pointsArray);

  vizConfig->multiplicityType = SINGLE_GRID;
  auto grid = vtkSmartPointer<vtkStructuredGrid>::New();
  grid->SetDimensions(lenX, lenY, lenZ);
  grid->SetPoints(points);
  data.grids[0][0] = grid;
  data.totalCells[0][0] = lenX * lenY * lenZ;

  vizConfig->type = STRUCTURED;
}

void initializeImageGrids(const VizConfigPtr& vizConfig,
                          const StorageManagerPtr& storageManager, int64_t lens[DIM3],
                          double spacings[DIM3], int64_t simulationsNumber,
                          int64_t timeStepNumber) {
  VizData &data = vizConfig->data;
  int64_t lenX = lens[X_DIM], lenY = lens[Y_DIM], lenZ = lens[Z_DIM];
  double spacingX = spacings[0], spacingY = spacings[1], spacingZ = spacings[2];

  vizConfig->multiplicityType = SINGLE_GRID;
  auto grid = vtkSmartPointer<vtkImageData>::New();
  grid->SetDimensions(lenX, lenY, lenZ);
  grid->SetSpacing(spacingX, spacingY, spacingZ);
  grid->SetOrigin(lenX / 2, lenY / 2, lenZ / 2);

  data.grids[0][0] = grid;
  data.totalCells[0][0] = lenX * lenY * lenZ;
  vizConfig->type = IMAGE;
}

void initializeGrids(const VizConfigPtr& vizConfig, const StorageManagerPtr& storageManager,
                     const DefaultEnginePtr& defaultEngine) {
  VizSchema &schema = vizConfig->schema;
  TARPtr fieldData = schema.fieldData;
  int64_t simulationsNumber = 1, timeStepNumber = 1;
  validateFieldDataTar(fieldData, vizConfig);

  DataElementPtr simulation = schema.simDimension;
  DataElementPtr time = schema.timeDimension;

  if ((simulation != nullptr) &&
      (simulation->GetType() == DIMENSION_SCHEMA_ELEMENT))
    simulationsNumber = simulation->GetDimension()->GetCurrentLength();

  if ((time != nullptr) && (time->GetType() == DIMENSION_SCHEMA_ELEMENT))
    timeStepNumber = time->GetDimension()->GetCurrentLength();

  if (fieldData->GetType()->name == UNSTRUCTURED_FIELD_DATA) {
    validateGeometryTar(schema.geometry, vizConfig);
    validateTopologyTar(schema.topology, vizConfig);
    initializeUnstructuredGrids(vizConfig, storageManager, defaultEngine);
  } else {
    int64_t lens[DIM3] = {1, 1, 1};
    double spacings[DIM3];
    DataElementPtr dims[DIM3] = schema.spatialDims;

    for (int32_t i = 0; i < DIM3; i++) {
      if ((dims[i] != NULL) &&
          (dims[i]->GetType() == DIMENSION_SCHEMA_ELEMENT)) {
        lens[i] = dims[i]->GetDimension()->GetCurrentLength();
        spacings[i] = dims[i]->GetDimension()->GetSpacing();
      } else if (i != Z_DIM) {
        throw std::runtime_error(
            "Spatial roles must be implemented as dimensions.");
      }
    }

    bool hasExplicitDim =
        dims[X_DIM]->GetDimension()->GetDimensionType() == EXPLICIT ||
                dims[Y_DIM]->GetDimension()->GetDimensionType() == EXPLICIT ||
                (dims[Z_DIM] != NULL)
            ? dims[Z_DIM]->GetDimension()->GetDimensionType() == EXPLICIT
            : false;

    if (hasExplicitDim) {

      initializeStructuredGrids(vizConfig, storageManager, dims, lens,
                                simulationsNumber, timeStepNumber);
    } else {
      initializeImageGrids(vizConfig, storageManager, lens, spacings,
                           simulationsNumber, timeStepNumber);
    }
  }
}



// Processing functions
//------------------------------------------------------------------------------
std::string processGrid(VizConfigPtr vizConfig,
                        StorageManagerPtr storageManager, int64_t simulation,
                        int64_t time) {
#define SIM_NAME "input"
#define BASE_NAME "savime_"
#define SUFFIX(S, T) "s" + to_string(S) + "_t" + to_string(T)
#define VTU_EXT ".vtu"
#define VTI_EXT ".vti"
#define VTS_EXT ".vts"

#define SET_ARRAY(SAV_TYPE, VTK_TYPE)                                          \
  SAV_TYPE *buffer = (SAV_TYPE *)handler->GetBuffer();                         \
  vtkSmartPointer<VTK_TYPE> vtkArray = vtkSmartPointer<VTK_TYPE>::New();       \
  vtkArray->SetName(attributeName.c_str());                                    \
  vtkArray->SetNumberOfComponents(components);                                 \
  vtkArray->SetArray(buffer, dataset->GetEntryCount() *components, 1);         \
  grid->GetPointData()->AddArray(vtkArray);

  VizSchema &schema = vizConfig->schema;
  VizData &data = vizConfig->data;
  VizSync &sync = vizConfig->sync;
  vtkSmartPointer<vtkDataSet> grid;

  GET_T1();
  
  double logicalTimeIndex = 0;
  auto timeDimension = schema.timeDimension;
  if (timeDimension) {
    Literal _time =
        storageManager->Real2Logical(timeDimension->GetDimension(), time);
    GET_LITERAL(logicalTimeIndex, _time, double);
  }

  if (vizConfig->type == UNSTRUCTURED) {
    auto _grid =
        vtkUnstructuredGrid::SafeDownCast(vizConfig->getGrid(simulation, time));  
    grid = vtkSmartPointer<vtkUnstructuredGrid>::New();
    sync.singleGridMutex.lock();
    grid->CopyStructure(_grid);
    sync.singleGridMutex.unlock();
  } else if (vizConfig->type == STRUCTURED) {
    auto _grid =
        vtkStructuredGrid::SafeDownCast(vizConfig->getGrid(simulation, time));
    grid = vtkSmartPointer<vtkStructuredGrid>::New();
    sync.singleGridMutex.lock();
    grid->CopyStructure(_grid);
    sync.singleGridMutex.unlock();
  } else if (vizConfig->type == IMAGE) {
    auto _grid =
        vtkImageData::SafeDownCast(vizConfig->getGrid(simulation, time));
    grid = vtkSmartPointer<vtkImageData>::New();
    sync.singleGridMutex.lock();
    grid->CopyStructure(_grid);
    sync.singleGridMutex.unlock();
  }

  list<DatasetHandlerPtr> localHandlersToClose;


  for (auto entry : data.data[simulation][time]) {
     
    auto attributeName = entry.first;
    auto dataset = entry.second;
    
    DatasetHandlerPtr handler = storageManager->GetHandler(dataset);
    localHandlersToClose.push_back(handler);
    int64_t components = dataset->GetType().vectorLength();

    switch (dataset->GetType().type()) {
#ifdef FULL_TYPE_SUPPORT
    case UINT8:
    case INT8: {
      SET_ARRAY(char, vtkTypeInt8Array);
      break;
    }
    case UINT16:
    case INT16: {
      SET_ARRAY(int16_t, vtkTypeInt16Array);
      break;
    }
    case UINT32:
#endif
    case INT32: {
      SET_ARRAY(int32_t, vtkTypeInt32Array);
      break;
    }
#ifdef FULL_TYPE_SUPPORT
    case UINT64:
#endif
    case INT64: {
      SET_ARRAY(long long int, vtkTypeInt64Array);
      break;
    }
    case FLOAT: {
      SET_ARRAY(float, vtkFloatArray);
      break;
    }
    case DOUBLE: {
      SET_ARRAY(double, vtkDoubleArray);
      break;
    }
    }
  }

  string fileName = vizConfig->outputDir + BASE_NAME + SUFFIX(simulation, time);
  
  switch (vizConfig->type) {
  case IMAGE: {
    vtkSmartPointer<vtkXMLImageDataWriter> writer =
        vtkSmartPointer<vtkXMLImageDataWriter>::New();
    writer->SetInputData(grid);
    fileName = fileName + VTI_EXT;
    writer->SetFileName(fileName.c_str());
    writer->Write();
    break;
  }
  case STRUCTURED: {
    vtkSmartPointer<vtkXMLStructuredGridWriter> writer =
        vtkSmartPointer<vtkXMLStructuredGridWriter>::New();
    writer->SetInputData(grid);
    fileName = fileName + VTS_EXT;
    writer->SetFileName(fileName.c_str());
    writer->Write();
    break;
  }
  case UNSTRUCTURED: {
    vtkSmartPointer<vtkXMLUnstructuredGridWriter> writer =
        vtkSmartPointer<vtkXMLUnstructuredGridWriter>::New();
    writer->SetInputData(grid);
    writer->SetCompressorTypeToNone();
    fileName = fileName + VTU_EXT;
    writer->SetFileName(fileName.c_str());
    writer->Write();
    break;
  }
  }
 
  for (auto handler : localHandlersToClose) {
    handler->Close();
  }

  //Run catalyst script (now done in processGridLoop directly).
  //if (/*simulation == 0 && */ !vizConfig->catalistScript.empty()) {
  //  int32_t returnValue = system(
  //    GET_CATALYST_COMMAND_LINE(fileName, time, logicalTimeIndex, vizConfig));
  //  
  //  remove(fileName.c_str());
  //}

  if (vizConfig->multiplicityType == MULTIPLE) {
    data.grids[simulation][time] = NULL;
    data.filledCells[simulation].erase(time);
    data.totalCells[simulation].erase(time);
  }

  data.data[simulation][time].clear();
  
  GET_T2();
  systemLogger->LogEvent(CATALYZE_OPERATOR,
                       CREATE_LOG("Process grid", simulation, time,
                                  GET_DURATION()));
  
  return fileName;
}

void processGridLoop(VizConfigPtr vizConfig, StorageManagerPtr storageManager) {
  VizSync &sync = vizConfig->sync;
  vector<tuple<string, string, string>> filesList;

  while (true) {

    sync.mutex.lock();
    if (!sync.keepWorking && sync.waitingGrids.empty()) {
      sync.mutex.unlock();
      break;
    }

    if (!sync.waitingGrids.empty()) {
      auto waitingGrid = sync.waitingGrids.front();
      sync.waitingGrids.pop_front();
      sync.mutex.unlock();

      auto fileName = processGrid(vizConfig, storageManager,
                                  waitingGrid.simulation, waitingGrid.time);

      auto tuple = make_tuple(fileName, to_string(waitingGrid.simulation),
                              to_string(waitingGrid.time));
      filesList.push_back(tuple);
    } else {
      sync.mutex.unlock();
      usleep(HUNDRED_MS);
      continue;
    }
  }

  if (!vizConfig->catalistScript.empty()) {
    auto fList = generateUniqueFileName(vizConfig->outputDir, 1);
    std::ofstream file(fList);

    for (auto tuple : filesList) {
      auto line = std::get<0>(tuple) + _COLON + std::get<1>(tuple) + _COLON
                  + std::get<2>(tuple);
      file << line << endl;
    }
    file.close();

    systemLogger->LogEvent(CATALYZE_OPERATOR, "Running catalyst script");
    int32_t returnValue =
        system(GET_CATALYST_COMMAND_LINE(fList, 0, 0, vizConfig));
  }
}


void processSubtar(const VizConfigPtr& vizConfig, SubtarPtr subtar,
                   const StorageManagerPtr& storageManager) {
  
  SavimeTime t1, t2;
  VizSchema &schema = vizConfig->schema;
  VizData &data = vizConfig->data;
  VizSync &sync = vizConfig->sync;

  DatasetPtr filter;
  int64_t simLower = 0, simUpper = 0;
  int64_t timeLower = 0, timeUpper = 0;

  for (auto entry : subtar->GetDimSpecs()) {
    auto dimSpecs = entry.second;
    if (dimSpecs->GetSpecsType() != ORDERED)
      throw std::runtime_error(
          "SubTARs must be implemented with ORDERED specifications.");
  }

  if (schema.simDimension != nullptr) {
    auto simSpecs =
        subtar->GetDimensionSpecificationFor(schema.simDimension->GetName());
    simLower = simSpecs->GetLowerBound();
    simUpper = simSpecs->GetUpperBound();
  }

  if (schema.timeDimension != nullptr) {
    auto timeSpecs =
        subtar->GetDimensionSpecificationFor(schema.timeDimension->GetName());
    timeLower = timeSpecs->GetLowerBound();
    timeUpper = timeSpecs->GetUpperBound();
  }

  unordered_map<string, DatasetPtr> datasets;
  bool manySims = simLower != simUpper;
  bool manyTimeSteps = timeLower != timeUpper;
  Mapping mapping = nullptr;
  DataElementPtr simDimension = schema.simDimension;
  DataElementPtr timeDimension = schema.timeDimension;

  for (int64_t simulation = simLower; simulation <= simUpper; simulation++) {
    for (int64_t time = timeLower; time <= timeUpper; time++) {
    
      GET_T1_LOCAL();
      auto grid = vizConfig->getGrid(simulation, time);
      auto map = vizConfig->getMapping(simulation, time);
      if(grid == nullptr || map == nullptr) continue;
    
      int64_t filledCells = 0;
      filter = nullptr;
      if (manySims && manyTimeSteps) {
        datasets = sliceSubtars(simDimension, timeDimension, storageManager,
                                subtar, simulation, time, filter);
      } else if (manySims) {
        datasets = sliceSubtars(simDimension, timeDimension, storageManager,
                                subtar, simulation, NONE, filter);
      } else if (manyTimeSteps) {
        datasets = sliceSubtars(simDimension, timeDimension, storageManager,
                                subtar, NONE, time, filter);
      } else {
        datasets = sliceSubtars(simDimension, timeDimension, storageManager,
                                subtar, NONE, NONE, filter);
      }

      if (vizConfig->type == IMAGE || vizConfig->type == STRUCTURED) {
        if (mapping == nullptr) {
          mapping = createMapping(vizConfig, storageManager, subtar, filter);
        }
      } else if (vizConfig->type == UNSTRUCTURED) {
        string indexElementName = schema.index->GetName();
        Mapping fullMapping;
        DimSpecPtr dimSpecs =
            subtar->GetDimensionSpecificationFor(indexElementName);

        fullMapping = vizConfig->getMapping(simulation, time);
        mapping = CREATE_MAPPING();
        mapping->resize(dimSpecs->GetFilledLength());

#define INVALID -1
#pragma omp parallel for
        for (int64_t i = dimSpecs->GetLowerBound(); i <= dimSpecs->GetUpperBound();
             i++) {
          int64_t pos = (i - dimSpecs->GetLowerBound());
          if(fullMapping->size()-1 < pos)
            (*mapping)[pos] = INVALID;
          else
            (*mapping)[pos] = (*fullMapping)[i];
        }
      }

      for (auto entry : datasets) {
        string attributeName = entry.first;
        DatasetPtr dataset = entry.second;
 
        sync.dataMutex.lock();
        if (data.data[simulation][time][attributeName] == nullptr) {
          sync.dataMutex.unlock();
          
          int64_t totalSize = vizConfig->getTotalCells(simulation, time);
          DatasetPtr newDataset =
              storageManager->Create(dataset->GetType(), totalSize);
          
          sync.dataMutex.lock();
          data.data[simulation][time][attributeName] = newDataset;
          sync.dataMutex.unlock();
          
        }

        if (mapping != nullptr) {
          
          sync.dataMutex.lock();
          auto gridDataset = data.data[simulation][time][attributeName];
          sync.dataMutex.unlock();

          storageManager->Copy(dataset, mapping,
                               gridDataset,
                               filledCells);
        } else {
          sync.dataMutex.lock();
          data.data[simulation][time][attributeName] = dataset;
          sync.dataMutex.unlock();
          filledCells = dataset->GetEntryCount();
        }
      }

      int64_t totalCells = vizConfig->getTotalCells(simulation, time);
      int64_t filled = data.filledCells[simulation][time];
      data.filledCells[simulation][time] = filled + filledCells;

      bool totallyFilled = data.filledCells[simulation][time] >= totalCells;
      if (totallyFilled) {
        
        WaitingGrid waitingGrid = {simulation, time};
        
        while(true)
        {
          sync.mutex.lock();
          if(sync.waitingGrids.size() <= vizConfig->numCores){
            
            sync.waitingGrids.push_back(waitingGrid);
            sync.mutex.unlock();
            break;
          } else{
            sync.mutex.unlock();
            //usleep(HUNDRED_MS);
          }
        }
      }
      
      GET_T2_LOCAL();   
      systemLogger->LogEvent(CATALYZE_OPERATOR,
                           CREATE_LOG("Process subtar", simulation, time,
                                      GET_DURATION()));
    }
  }
}

void check(const VizConfigPtr &vizConfig) {
  VizData &data = vizConfig->data;

  for (auto entry1 : data.filledCells) {
    for (auto entry2 : entry1.second) {
      auto simulation = entry1.first;
      auto timeStep = entry2.first;

      if (data.filledCells[simulation][timeStep] <
              vizConfig->getTotalCells(
                  simulation,
                  timeStep) 
          && data.filledCells[simulation][timeStep] > 0 &&
          vizConfig->getTotalCells(simulation, timeStep) > 0) {
        throw std::runtime_error("Could not create all viz "
                                 "files due to incomplete data.");
      }
    }
  }
}

void cleanUp(const VizConfigPtr &vizConfiguration) {
  for (DatasetHandlerPtr handler : vizConfiguration->data.handlersToClose) {
    handler->Close();
  }
}

/*catalyze(field_data_tar1, [geometry_tar], [topology_tar], catalyst_script,
 * output_path)*/
int catalyze(SubTARIndex subtarIndex, OperationPtr operation,
             ConfigurationManagerPtr configurationManager,
             QueryDataManagerPtr queryDataManager,
             MetadataManagerPtr metadataManager,
             StorageManagerPtr storageManager, EnginePtr engine) {
    
  VizConfigPtr vizConfiguration;
  SubtarPtr subtar;
  int32_t subtarCount = 0;
  DefaultEnginePtr defaultEngine = DEFAULT_ENGINE(engine);
  systemLogger =  queryDataManager->GetSystemLogger();
    
  try {
    vizConfiguration = createVizConfiguration(operation, configurationManager,
                                              storageManager);
    initializeGrids(vizConfiguration, storageManager, defaultEngine);
    string fieldTarName = vizConfiguration->schema.fieldData->GetName();
    auto generator = defaultEngine->GetGenerators()[fieldTarName];

    while (true) {
      subtar = generator->GetSubtar(subtarCount);
      if (subtar == nullptr)
        break;
      processSubtar(vizConfiguration, subtar, storageManager);
      generator->TestAndDisposeSubtar(subtarCount);
      subtarCount++;
    }

    check(vizConfiguration);
    waitAll(vizConfiguration);
    cleanUp(vizConfiguration);

  } catch (std::exception &e) {

    if (vizConfiguration != nullptr) {
      waitAll(vizConfiguration);
      cleanUp(vizConfiguration);
    }

    throw e;
  }

  return SAVIME_SUCCESS;
}