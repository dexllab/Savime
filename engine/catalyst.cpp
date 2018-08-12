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
#include <cstdlib>
#include <memory>
#include <mutex>
#include <unistd.h>
#include <condition_variable>
#include <unordered_map>
#include <vtkCellData.h>
#include <vtkCellType.h>
#include <vtkCPDataDescription.h>
#include <vtkCPInputDataDescription.h>
#include <vtkCPProcessor.h>
#include <vtkCPPythonScriptPipeline.h>
#include <vtkCellArray.h>
#include <vtkUnsignedCharArray.h>
#include <vtkDoubleArray.h>
#include <vtkFloatArray.h>
#include <vtkDoubleArray.h>
#include <vtkIntArray.h>
#include <vtkLongArray.h>
#include <vtkImageData.h>
#include <vtkStructuredGrid.h>
#include <vtkUnstructuredGrid.h>
#include <vtkNew.h>
#include <vtkPoints.h>
#include <vtkPointData.h>
#include <vtkXMLImageDataWriter.h>
#include <vtkXMLImageDataReader.h>
#include <vtkXMLStructuredGridWriter.h>
#include <vtkXMLStructuredGridReader.h>
#include <vtkXMLUnstructuredGridWriter.h>
#include <vtkXMLUnstructuredGridReader.h>
#include <unordered_map>
#include <future>
#include <math.h>
#include <../core/include/util.h>

using namespace std;
typedef vtkSmartPointer<vtkCPProcessor> Processor;
enum Params {
  PROG,
  SCRIPT,
  INPUT_GRIDS,
  OUTPUT_DIR,
  TIME_STEP,
  REAL_TIME_STEP
};
unordered_map<int32_t, string> simulation_dirs;

inline string get_simulation_dir(string outputdir, int32_t simulation) {

  if (simulation_dirs.find(simulation) != simulation_dirs.end()) {
    return simulation_dirs[simulation];
  }
  //auto simulation_dir = (outputdir + "/" + to_string(simulation));
  auto simulation_dir = pathAppend(outputdir, to_string(simulation));

  mkdir(simulation_dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  simulation_dirs[simulation] = simulation_dir;
  return simulation_dir;
}

string get_file_extension(const string &file_name) {
  if (file_name.find_last_of('.') != std::string::npos)
    return file_name.substr(file_name.find_last_of('.') + 1);
  return "";
}

vector<tuple<string, int32_t, int32_t>> read_grids_file(string file) {

  vector<tuple<string, int32_t, int32_t>> grids_data;
  std::string line;
  std::ifstream grids_file(file);

  while (std::getline(grids_file, line)) {
    auto splittedLine = split(line, ':');
    auto fileName = splittedLine[0];
    auto simulation = atoi(splittedLine[1].c_str());
    auto time = atoi(splittedLine[2].c_str());
    auto tuple = make_tuple(fileName, simulation, time);
    grids_data.push_back(tuple);
  }

  grids_file.close();
  remove(file.c_str());
  return grids_data;
}

inline vtkSmartPointer<vtkDataSet> read_grid(string file) {
#define VTI "vti"
#define VTS "vts"
#define VTU "vtu"

  vtkSmartPointer<vtkDataSet> dataset;
  string ext = get_file_extension(file);

  if (ext == VTI) {
    vtkSmartPointer<vtkXMLImageDataReader> reader =
        vtkSmartPointer<vtkXMLImageDataReader>::New();
    reader->SetFileName(file.c_str());
    reader->Update();
    reader->GetOutput()->Register(reader);
    dataset.TakeReference(vtkDataSet::SafeDownCast(reader->GetOutput()));
  } else if (ext == VTS) {
    vtkSmartPointer<vtkXMLStructuredGridReader> reader =
        vtkSmartPointer<vtkXMLStructuredGridReader>::New();
    reader->SetFileName(file.c_str());
    reader->Update();
    reader->GetOutput()->Register(reader);
    dataset.TakeReference(vtkDataSet::SafeDownCast(reader->GetOutput()));
  } else if (ext == VTU) {
    vtkSmartPointer<vtkXMLUnstructuredGridReader> reader =
        vtkSmartPointer<vtkXMLUnstructuredGridReader>::New();
    reader->SetFileName(file.c_str());
    reader->Update();
    reader->GetOutput()->Register(reader);
    dataset.TakeReference(vtkDataSet::SafeDownCast(reader->GetOutput()));
  }

  return dataset;
}

void process_grids(vector<tuple<string, int32_t, int32_t>> grids_data,
                   string script, string output_dir) {
#define SIM_NAME "input"

  // Initialize catalyst CoProcessor
  Processor processor = Processor::New();
  processor->Initialize();
  vtkNew<vtkCPPythonScriptPipeline> pipeline;
  pipeline->Initialize(script.c_str());
  processor->AddPipeline(pipeline.GetPointer());

  for (auto grid_data : grids_data) {

    // Getting tuple values
    auto grid_file = std::get<0>(grid_data);
    auto simulation = std::get<1>(grid_data);
    auto time = std::get<2>(grid_data);

    // Set output file dir
    auto simulation_output_dir = get_simulation_dir(output_dir, simulation);
    chdir(simulation_output_dir.c_str());

    // Reading grid
    auto grid = read_grid(grid_file);

    // Coprocessing
    auto dataDescription = vtkSmartPointer<vtkCPDataDescription>::New();
    dataDescription->ForceOutputOn();
    dataDescription->AddInput(SIM_NAME);
    dataDescription->SetTimeData(time, time);
    dataDescription->GetInputDescriptionByName(SIM_NAME)->SetGrid(grid);
    processor->CoProcess(dataDescription);

    // Removing input grid file
    remove(grid_file.c_str());
  }

  processor->Finalize();
}

void remove_file(string file) { remove(file.c_str()); }

int main(int argc, char **argv) {
#define ARGS_NUMBER 6
  assert(argc == ARGS_NUMBER);
  string script = string(argv[SCRIPT]);
  string grids_file = string(argv[INPUT_GRIDS]);
  string output_dir = string(argv[OUTPUT_DIR]);
  auto grids_data = read_grids_file(grids_file);
  process_grids(grids_data, script, output_dir);
  return 0;
}