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
*    ANDERSON C. SILVA				JANUARY 2020
*/

#include <string>
#include <default_config_manager.h>
#include <core/include/metadata.h>

#ifndef SAVIME_PREDICTIONMODEL_H
#define SAVIME_PREDICTIONMODEL_H

using namespace std;

class PredictionModel {
  public:
    PredictionModel(string modelName);
    void checkInputDimensions(SubtarPtr subtar);
    string getInputDimensionString();
    int getNumberOfInputDimensions();
    string getAttributeString();
    vector<string> getAttributeList();
    string getOutputDimensionString();
    ~PredictionModel();
    list<DimensionPtr> getOutputDimensionList();
    list<pair<string, savime_size_t>> getOutputAttributeList();
 private:
    DefaultConfigurationManager* _modelConfigurationManager;
    void loadModelConfig(string modelName);
};

#endif //SAVIME_PREDICTIONMODEL_H
