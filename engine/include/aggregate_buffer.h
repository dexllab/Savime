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
#ifndef AGGREGATE_BUFFER_H
#define AGGREGATE_BUFFER_H

#include "../core/include/savime_hash.h"

class AggregateBuffer {
public:
  virtual int64_t GetHash(vector<int64_t> indexes) {
    throw runtime_error("Calling GetHash in AggregateBuffer.");
  }
  virtual double &operator[](uint64_t index) = 0;
  virtual double &operator[](vector<int64_t> indexes) = 0;
  virtual SavimeIndexesHashPtr getIndexesMap() = 0;
  virtual map<uint64_t, double> &getMap() = 0;
  virtual BitsetPtr Bitmask(){};
  virtual void SetBit(int64_t index){};
  virtual bool GetBit(int64_t index){};
  virtual void dbg_print(){};
};
typedef std::shared_ptr<AggregateBuffer> AggregateBufferPtr;

class VectorAggregateBuffer : public AggregateBuffer {
public:
  SavimeDimHashMapDblPtr hashMap;
  vector<double> buffer;
  BitsetPtr bitmask;
  int64_t size;

  VectorAggregateBuffer(vector<DimensionPtr> dimensions, void *buffer,
                        int64_t size) {
    hashMap = std::make_shared<SavimeDimHashMap<double>>(dimensions,
                                                         (double *)buffer);
    bitmask = make_shared<boost::dynamic_bitset<>>(size);
    if(bitmask == nullptr)
      throw runtime_error("Could not allocate bitmask");

    this->size = size;
  }

  VectorAggregateBuffer(vector<DimensionPtr> dimensions, int64_t size,
                        double fill) {
    buffer.resize((size_t)size);
    std::fill(buffer.begin(), buffer.end(), fill);
    hashMap = std::make_shared<SavimeDimHashMap<double>>(dimensions,&buffer[0]);
    bitmask = make_shared<boost::dynamic_bitset<>>(size);
    if(bitmask == nullptr)
      throw runtime_error("Could not allocate bitmask");

    this->size = size;
  }

  int64_t GetHash(vector<int64_t> indexes) override { return hashMap->GetHash(indexes); }

  double &operator[](uint64_t index) override { return (*hashMap)[index]; }

  double &operator[](vector<int64_t> indexes) override {
    auto index = GetHash(indexes);
    return (*hashMap)[index];
  }

  SavimeIndexesHashPtr getIndexesMap() override {
    throw runtime_error(
        "Calling getIndexesMap from VectorWithBitmaskAggregateBuffer.");
  }

  map<uint64_t, double> &getMap() override {
    throw runtime_error(
        "Calling getMap from VectorWithBitmaskAggregateBuffer.");
  }

  int64_t GetSize() { return size; }

  BitsetPtr Bitmask() override { return bitmask; }

  void SetBit(int64_t index) override { (*bitmask)[index] = true; }

  bool GetBit(int64_t index) override { return (*bitmask)[index]; }

  void dbg_print() override {
    printf("Print buffer %ld \n", size);
    printf("-----------------\n");
    for (int64_t i = 0; i < size; i++) {
      printf("-- %lf set: %d\n", hashMap->buffer[i], (*bitmask)[i] == true);
    }
  }
};
typedef std::shared_ptr<VectorAggregateBuffer> VectorAggregateBufferPtr;

class MapAggregateBuffer;
typedef std::shared_ptr<MapAggregateBuffer> MapAggregateBufferPtr;

class MapAggregateBuffer : public AggregateBuffer {
  SavimeHashMapDbl hashMap;
  double defaultValue;

public:
  MapAggregateBuffer(vector<DimensionPtr> dimensions, double fill) {
    hashMap = std::make_shared<SavimeHashMap<double>>(dimensions);
    this->defaultValue = fill;
  }

  int64_t GetHash(vector<int64_t> indexes) override { return hashMap->GetHash(indexes); }

  double &operator[](uint64_t index) override {
    auto pair = (*hashMap).map.find(index);
    if (pair == (*hashMap).map.end()) {
      (*hashMap).map[index] = defaultValue;
    }
    return (*hashMap).map[index];
  }

  double &operator[](vector<int64_t> indexes) override {
    return (*this)[GetHash(indexes)];
  }

  SavimeIndexesHashPtr getIndexesMap() override { return hashMap->indexesMap; }

  map<uint64_t, double> &getMap() override { return hashMap->map; }

  int64_t GetSize() { return 0; }

  void dbg_print() override {
    printf("Print buffer\n");
    printf("-----------------\n");
    for (auto entry : hashMap->map) {
      printf("- %lu -> %lf\n", entry.first, entry.second);
    }
  }
};


#endif /* AGGREGATE_BUFFER_H */
