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
#include <sstream>
#include <iomanip>
#include "mock_engine.h"

string buffer2binaryString(unsigned char *buffer, int64_t size) {
  std::stringstream binaryStringStream;

  for (int64_t i = 0; i < size; i++) {
    binaryStringStream << "0x" << setfill('0') << setw(2) << hex
                       << (unsigned short) buffer[i] << _COMMA;
  }

  return binaryStringStream.str();
}

vector<uint8_t> buffer2vector(unsigned char *buffer, int64_t size) {

  vector<uint8_t> vec(size);
  for (int64_t i = 0; i < size; i++) {
    vec[i] = buffer[i];
  }
  return vec;
}

bool Compare(std::map<std::string, std::vector<std::vector<uint8_t>>> binData1,
             std::map<std::string, std::vector<std::vector<uint8_t>>> binData2,
             string& dataElement, int64_t& i_error, int64_t& j_error ) {

  if(binData1.size() != binData2.size()) {
    return false;
  }

  for(auto entry1 : binData2) {

    dataElement = entry1.first;


    if(binData2.find(entry1.first) == binData2.end())
      return false;

    auto vec1 = binData1[entry1.first];
    auto vec2 = binData2[entry1.first];

    if(vec1.size() != vec2.size())
      return false;

    for(int64_t i = 0; i < vec1.size(); i++) {
      i_error = i;
      auto subVec1 = vec1[i];
      auto subVec2 = vec2[i];

      if(subVec1.size() != subVec2.size())
        return false;

      for(int64_t j = 0; j < subVec2.size(); j++) {
        j_error = j;
        if(subVec1[j] != subVec2[j]){
          return false;
        }
      }
    }
  }

  return true;
}

//------------------------------------------------------------------------------

void MockDefaultEngine::SendResultingTAR(EngineListener *caller, TARPtr tar) {
  DatasetPtr dataset;
  int32_t subtarCounter = 0;
  bool isFirst = true, isLast = false;

  auto generator = _generators[tar->GetName()];
  TestCaseMatcher *testCaseCreator = (TestCaseMatcher *) caller;

  while (true) {

    auto subtar = generator->GetSubtar(subtarCounter);

    if (subtar == nullptr)
      break;
    int64_t totalLength = subtar->GetFilledLength();
    subtar->RemoveTempDataElements();

    if (_sendResult == SAVIME_FAILURE) {
      throw std::runtime_error("Problem while sending resulting TAR.");
    }

    for (auto entry : subtar->GetDimSpecs()) {
      _storageManager->MaterializeDim(entry.second, totalLength, dataset);
      testCaseCreator->AddDataset(entry.first, dataset);
    }

    for (auto entry : subtar->GetDataSets()) {
      auto dataset = entry.second;
      testCaseCreator->AddDataset(entry.first, dataset);
    }

    generator->TestAndDisposeSubtar(subtarCounter++);
    isFirst = false;
  }

  if (WaitSendBlocksCompletion() != SAVIME_SUCCESS) {
    throw std::runtime_error("Problem while sending resulting TAR.");
  }
}

//------------------------------------------------------------------------------
void TestCaseMatcher::StartNewQuery() {
  _binData.clear();
  _binStrData.clear();
}

void TestCaseMatcher::AddDataset(string blockName, DatasetPtr dataset) {
  auto handler = _storageManager->GetHandler(dataset);

  string
      binaryString = buffer2binaryString((unsigned char *) handler->GetBuffer(),
                                         dataset->GetLength());

  auto vector = buffer2vector((unsigned char *) handler->GetBuffer(),
                              dataset->GetLength());

  _binData[blockName].push_back(vector);
  _binStrData[blockName].push_back(binaryString);
  handler->Close();
}

string TestCaseMatcher::GetBinaryString() {

  int64_t numBlocks = (_binStrData.begin())->second.size();
  string binaryString = _LEFT_CURLY_BRACKETS;

  for (auto entry : _binStrData) {

    auto blockName = entry.first;
    binaryString +=
        _LEFT_CURLY_BRACKETS _DOUBLE_QUOTE + blockName + _DOUBLE_QUOTE _COMMA
                                                         _LEFT_CURLY_BRACKETS;

    for (int64_t i = 0; i < numBlocks; i++) {

      auto blocks = entry.second;
      binaryString += _LEFT_CURLY_BRACKETS + blocks[i] +
                      _RIGHT_CURLY_BRACKETS _COMMA _SPACE;
    }

    binaryString += _RIGHT_CURLY_BRACKETS _RIGHT_CURLY_BRACKETS _COMMA;

  }

  binaryString += _RIGHT_CURLY_BRACKETS;
  return binaryString;
}