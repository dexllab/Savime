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

#ifndef SAVIME_TIME_EVALUATOR_H
#define SAVIME_TIME_EVALUATOR_H

#include <chrono>
#include <util.h>
#include <map>

class TimeEvaluator {
public:
    int getDurationInMicrosseconds(int tag);
    void registerTime(int tag);
private:
    std::map<int, SavimeTime> tagMap;

    SavimeTime getDuration(int tag);
};


#endif //SAVIME_TIME_EVALUATOR_H
