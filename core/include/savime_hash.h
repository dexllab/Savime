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
#ifndef SAVIME_HASH_H
#define SAVIME_HASH_H
#include <unordered_map>
#include <utility>
#include <functional>
#include "metadata.h"

#ifdef TBB_SUPPORT
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_hash_map.h>
#endif

typedef std::shared_ptr<std::map<uint64_t, vector<int64_t>>>
    SavimeIndexesHashPtr;

class BaseSavimeHash {
public:
  vector<int64_t> multipliers;
 
  virtual inline void calculateMultipliers(vector<DimensionPtr> dimensions) {
    auto numDims = static_cast<uint32_t>(dimensions.size());
    for (uint32_t i = 0; i < numDims; i++) {
      multipliers.push_back(1);
      for (int32_t j = i + 1; j < numDims; j++) {
        int64_t preamble = multipliers[i];
        multipliers[i] = preamble * dimensions[j]->GetCurrentLength();
      }
    }

    if (numDims == 0) {
      multipliers.push_back(1);
    }
  }

  virtual inline uint64_t GetHash(vector<int64_t>& indexes) {
    uint64_t r = 0;
    for (int32_t i = 0; i < indexes.size(); i++)
      r += multipliers[i] * indexes[i];
    return r;
  }
};

template <class T> class SavimeHashMap : public BaseSavimeHash{

public:
  SavimeIndexesHashPtr indexesMap;
  std::map<uint64_t, T> map;

  SavimeHashMap(const vector<DimensionPtr> &dimensions) {
    indexesMap =  make_shared<std::map<uint64_t, vector<int64_t>>>();
    calculateMultipliers(dimensions);
  }
  
  inline uint64_t GetHash(vector<int64_t>& indexes) {
    uint64_t r = BaseSavimeHash::GetHash(indexes);
    (*indexesMap)[r] = indexes;
    return r;
  }

  inline T &operator[](uint64_t index) { return map[index]; }
  inline T &Get(int64_t index) { return map[index]; }
  inline T &Get(vector<int64_t> indexes) { return map[GetHash(indexes)]; }
};

typedef shared_ptr< SavimeHashMap<double> > SavimeHashMapDbl;

template <class T> class SavimeDimHashMap : public BaseSavimeHash {

public:
  T *buffer;
  vector<int64_t> multipliers;

  SavimeDimHashMap(vector<DimensionPtr> dimensions, T *buffer) {
    this->buffer = buffer;
    calculateMultipliers(dimensions);
  }

  inline T &Get(vector<int64_t> indexes) { return buffer[GetHash(indexes)]; }
  inline T &operator[](uint64_t index) { return buffer[index]; }
};

typedef std::shared_ptr<SavimeDimHashMap<double> > SavimeDimHashMapDblPtr;


#ifdef TBB_SUPPORT
template <class T1, class T2> class ParallelSavimeHashMap {

public:

  tbb::concurrent_unordered_multimap<T1, T2> map;

  inline void Put(T1 key, T2 val) {
    auto p = std::make_pair(key, val);
    map.insert(p);
  }
};
#else
template <class T1, class T2> class ParallelSavimeHashMap {

  std::mutex mtx;
public:

  unordered_multimap<T1, T2> map;

  inline void Put(T1 key, T2 val) {
    mtx.lock();
    auto p = std::make_pair(key, val);
    map.insert(p);
    mtx.unlock();
  }
};
#endif



#endif /* SAVIME_HASH_H */