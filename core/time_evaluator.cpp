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
*    ANDERSON SILVA				JUNE 2020
*/

#include "time_evaluator.h"
#include <map>
#include <chrono>
#include <iterator>
#include <iterator>
#include <utility>

using namespace std;


pair <int, SavimeTime> PAIR1 ;


void TimeEvaluator::registerTime(int tag) {
    pair <int, SavimeTime> pair(tag, high_resolution_clock::now());
    this->tagMap.insert(pair);
}

SavimeTime TimeEvaluator::getDuration(int tag) {
    SavimeTime t2 = high_resolution_clock::now();
    return this->tagMap[tag];
}

int TimeEvaluator::getDurationInMicrosseconds(int tag) {
    auto lastTimeMeasured = this->tagMap[tag];
    auto now = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(now - lastTimeMeasured).count() / 1000;
    return duration;
}