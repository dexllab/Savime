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
#ifndef VIZ_CONFIG_H
#define VIZ_CONFIG_H


#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <unistd.h>
#include <unordered_map>
#include <vtkCPDataDescription.h>
#include <vtkCPInputDataDescription.h>
#include <vtkCPProcessor.h>
#include <vtkCPPythonScriptPipeline.h>
#include <vtkCellArray.h>
#include <vtkCellData.h>
#include <vtkCellType.h>
#include <vtkDoubleArray.h>
#include <vtkDoubleArray.h>
#include <vtkFloatArray.h>
#include <vtkImageData.h>
#include <vtkIntArray.h>
#include <vtkLongArray.h>
#include <vtkNew.h>
#include <vtkPointData.h>
#include <vtkPoints.h>
#include <vtkPoints2D.h>
#include <vtkStructuredGrid.h>
#include <vtkTypeInt16Array.h>
#include <vtkTypeInt32Array.h>
#include <vtkTypeInt64Array.h>
#include <vtkTypeInt8Array.h>
#include <vtkUnsignedCharArray.h>
#include <vtkUnstructuredGrid.h>
#include <vtkXMLImageDataWriter.h>
#include <vtkXMLStructuredGridWriter.h>
#include <vtkXMLUnstructuredGridWriter.h>
#include "../core/include/metadata.h"
#include "../core/include/storage_manager.h"

namespace SavimeViz{

#define CATERSIAN_FIELD_DATA2D "CartesianFieldData2d"
#define CATERSIAN_FIELD_DATA3D "CartesianFieldData3d"
#define UNSTRUCTURED_FIELD_DATA "UnstructuredFieldData"
#define CATERSIAN_GEO "CartesianGeometry"
#define CATERSIAN_GE02D "CartesianGeometry2d"
#define CATERSIAN_GEO3D "CartesianGeometry3d"
#define INCIDENCE_TOPOLOGY "IncidenceTopology"
#define ADJACENCY_TOPOLOGY "AdjacenceTopology"
#define SIMULATION_ROLE "simulation"
#define TIME_ROLE "time"
#define CELL_TYPE "celltype"
#define COORDS_ROLE "coords"
#define X_ROLE "x"
#define Y_ROLE "y"
#define Z_ROLE "z"
#define INDEX_ROLE "index"
#define INCIDENT_ROLE "incident"
#define INCIDENTEE_ROLE "incidentee"

#define NONE -1
#define DIM3 3
#define DIM2 2
#define X_DIM 0
#define Y_DIM 1
#define Z_DIM 2

class semaphore {
private:
  mutex mtx;
  mutex semaphore_mutex;
  condition_variable cv;
  condition_variable cv_all;
  volatile int count;
  volatile int max;
  volatile int waiting = 0;

public:
  void setCount(int c) { count = max = c; }

  void notify() {
    // unique_lock<mutex> lck(mtx);

    semaphore_mutex.lock();
    ++count;
    --waiting;
    semaphore_mutex.unlock();

    cv.notify_one();
  }

  void wait() {
    unique_lock<mutex> lck(mtx);
    while (count == 0)
      cv.wait(lck);

    semaphore_mutex.lock();
    ++waiting;
    --count;
    semaphore_mutex.unlock();
  }
};
typedef semaphore Semaphore;

typedef enum { UNSTRUCTURED, STRUCTURED, IMAGE } GridType ;
typedef enum  {SINGLE_GRID, SINGLE_SIM, SINGLE_TIME, MULTIPLE} 
    GridMultiplicityType;

typedef unordered_map<int64_t,
                      unordered_map<int64_t, vtkSmartPointer<vtkDataSet>>>
    Gridset;
typedef unordered_map<int64_t, unordered_map<int64_t, int64_t>>
    GridsetAttribute;
typedef unordered_map<int64_t,
                      unordered_map<int64_t, unordered_map<string, DatasetPtr>>>
    GridsetData;
typedef unordered_map<int64_t, unordered_map<int64_t, Mapping>>
    PointsMapping;
typedef unordered_map<int64_t, unordered_map<int64_t, std::future<void>>>
    AssyncHandlers;
typedef vector<std::future<void>> AssyncHandlersList;
typedef vector<DatasetHandlerPtr> HandlerSet;

typedef std::mutex Mutex;

/*Struct that holds data of grid ready to be generated.*/
typedef struct {
  int64_t simulation;
  int64_t time;
} WaitingGrid;

typedef list<WaitingGrid> WaitingGrids;
typedef list<std::thread> Threads;
typedef std::atomic<bool> Flag;

/*Struct that holds schema info for the visualization code*/
typedef struct {
   /*TARs*/
  TARPtr fieldData;
  TARPtr geometry;
  TARPtr topology;
  
  /*Special data types*/
  DataElementPtr index;
  DataElementPtr spatialDims[DIM3];
  DataElementPtr spatialCoords;
  DataElementPtr timeDimension;
  DataElementPtr simDimension;
  DataElementPtr geoTimeDimension;
  DataElementPtr geoSimDimension;
  DataElementPtr geoIndexDimension;
  DataElementPtr topTimeDimension;
  DataElementPtr topSimDimension;
  DataElementPtr topIncident;
  DataElementPtr topIncidentee;
  DataElementPtr cellType;
  DataElementPtr topValue;
} VizSchema;

/*Struct that holds data for the visualization code*/
typedef struct {
   /*TARs*/
  Gridset grids;
  GridsetAttribute filledCells;
  GridsetAttribute totalCells;
  GridsetData data;
  HandlerSet handlersToClose;
  PointsMapping mapping;
  
} VizData;

/*Struct that holds synchronization primitives for visualization code*/
typedef struct {
  Mutex mutex;
  Mutex singleGridMutex;
  Mutex dataMutex;
  WaitingGrids waitingGrids;
  Threads threads;
  Flag keepWorking;
} VizSync;

/*Struct that holds all data for the visualization code*/
struct VizConfiguration {

  GridType type;
  GridMultiplicityType multiplicityType;
  string outputDir;
  string catalistScript;
  string catalystExecutable;
  int32_t numCores;
  VizSchema schema;
  VizData data;
  VizSync sync;
          
  vtkSmartPointer<vtkDataSet> getGrid(int32_t simulation, int32_t time) {
    switch(multiplicityType){
      case SINGLE_GRID : return data.grids[0][0]; break;
      case SINGLE_SIM : return data.grids[0][time]; break;
      case SINGLE_TIME : return data.grids[simulation][0]; break;
      case MULTIPLE : return data.grids[simulation][time]; break;
    }
  }

  int64_t getTotalCells(int32_t simulation, int32_t time) {
    switch(multiplicityType){
      case SINGLE_GRID : return data.totalCells[0][0]; break;
      case SINGLE_SIM : return data.totalCells[0][time]; break;
      case SINGLE_TIME : return data.totalCells[simulation][0]; break;
      case MULTIPLE : return data.totalCells[simulation][time]; break;
    }
  }

  Mapping getMapping(int32_t simulation, int32_t time) {
    switch(multiplicityType){
      case SINGLE_GRID : return data.mapping[0][0]; break;
      case SINGLE_SIM : return data.mapping[0][time]; break;
      case SINGLE_TIME : return data.mapping[simulation][0]; break;
      case MULTIPLE : return data.mapping[simulation][time]; break;
    }
  }
};
typedef std::shared_ptr<VizConfiguration> VizConfigPtr;

#define GET_CATALYST_COMMAND_LINE(F, T, LT, C)                                 \
  (C->catalystExecutable + " '" + C->catalistScript + "' '" + F + "' '" +      \
   C->outputDir + "' " + to_string(T) + " " + to_string(LT))                   \
      .c_str()

#define CREATE_LOG(O, S, T, TIME)                                              \
  std::string(O) + " took " + std::to_string(TIME) + " ms for simulation "     \
      + std::to_string(S) + " and time step " + std::to_string(T) + "."

};

void processGridLoop(SavimeViz::VizConfigPtr vizConfig, 
                     StorageManagerPtr storageManager);

typedef vtkSmartPointer<vtkCPProcessor> Processor;

std::string
processGrid(SavimeViz::VizConfigPtr vizConfig, StorageManagerPtr storageManager,
                 int64_t time, int64_t simulation);

/*Handler must be created and point to a valid dataset.*/
inline void * COPY_FROM_MMAP(DatasetHandlerPtr handler){
  
  int64_t len = handler->GetDataSet()->GetLength();
  void * buffer = malloc(len);
  
  if(buffer == nullptr)
    throw std::runtime_error("Could not allocate heap memory "
                             "in catalyze operator.");
  
  memcpy((char*)buffer, (char*)handler->GetBuffer(), len);
  
  return buffer;
}

#endif /* VIZ_H */