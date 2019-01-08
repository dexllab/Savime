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
#ifndef TYPES_H
#define TYPES_H

#include <string.h>
#include <string>
#include <algorithm>
#include "symbols.h"
#include "util.h"

using namespace std;

/**
* Auxiliary types in SAVIME.
*/
typedef uint64_t TARPosition;
typedef int64_t SubTARPosition;
typedef int64_t SubTARIndex;
typedef int64_t RealIndex;
typedef struct {RealIndex inf; RealIndex sup;} IndexPair;
typedef uint64_t savime_size_t;

#define INVALID_SUBTAR_POSITION -1
#define INVALID_REAL_INDEX -1
#define INVALID_EXACT_REAL_INDEX -1
#define BELOW_OFFBOUNDS_REAL_INDEX -2
#define ABOVE_OFFBOUNDS_REAL_INDEX -3

#ifdef FULL_TYPE_SUPPORT
/**
* Enum with codes for the basic data types supported by SAVIME.
*/
enum EnumDataType {
  CHAR,   /*!<UTF-8 char.*/
  UCHAR,  /*!<UTF-16 hars.*/
  INT8,   /*!<8-bit integer number.*/
  INT16,  /*!<16-bit integer number.*/
  INT32,  /*!<32-bit integer number.*/
  INT64,  /*!<64-bit integer number.*/
  UINT8,  /*!<8-bit unsigned integer number.*/
  UINT16, /*!<16-bit unsigned integer number.*/
  UINT32, /*!<32-bit unsigned integer number.*/
  UINT64, /*!<64-bit unsigned integer number.*/
  FLOAT,  /*!<32-bit floating point number.*/
  DOUBLE, /*!<64-bit floating point number.*/
  TAR_POSITION, /*!<tType that represents a TAR location, i. e. a linearized
 * position of a cell.*/
  REAL_INDEX, /*!<Type that represents the real indexes for a dimension.*/
  SUBTAR_POSITION, /*!<Type that represents a position within a subtar,
 * or them offset from the subtar initial position in the TAR.*/
  NO_TYPE /*!<No type codes for invalid types.*/
};
#else
/**
* Enum with codes for the basic data types supported by SAVIME.
*/
enum EnumDataType {
  CHAR,   /*!<UTF-8 char.*/
  INT32,  /*!<32-bit integer number.*/
  INT64,  /*!<64-bit integer number.*/
  FLOAT,  /*!<32-bit floating point number.*/
  DOUBLE, /*!<64-bit floating point number.*/
  TAR_POSITION, /*!<tType that represents a TAR location, i. e. a linearized
 * position of a cell.*/
  REAL_INDEX, /*!<Type that represents the real indexes for a dimension.*/
  SUBTAR_POSITION, /*!<Type that represents a position within a subtar,
 * or them offset from the subtar initial position in the TAR.*/
  NO_TYPE /*!<No type codes for invalid types.*/
};
#endif

class DataType {
  EnumDataType _type;
  uint32_t _vector_length;
  uint32_t _selected;

public:
  DataType() {
    _type = NO_TYPE;
    _vector_length = 1;
    _selected = 0;
  }

  DataType(EnumDataType type) {
    _type = type;
    _vector_length = 1;
    _selected = 0;
  }

  DataType(EnumDataType type, uint32_t length) {
    _type = type;
    _vector_length = length;
    _selected = 0;
  }

  DataType(EnumDataType type, uint32_t length, uint32_t selected) {
    _type = type;
    _vector_length = length;
    _selected = selected;
  }

  inline EnumDataType type() { return _type; }

  inline int32_t vectorLength() { return _vector_length; }

  inline int32_t selected() { return _selected; }

  size_t getElementSize() {
    switch (_type) {
    case CHAR:
      return sizeof(char);
      break;
#ifdef FULL_TYPE_SUPPORT
    case UCHAR:
      return sizeof(char) * 2;
      break;
    case INT8:
      return sizeof(int8_t);
      break;
    case INT16:
      return sizeof(int16_t);
      break;
#endif
    case INT32:
      return sizeof(int32_t);
      break;
    case INT64:
      return sizeof(int64_t);
      break;
#ifdef FULL_TYPE_SUPPORT
    case UINT8:
      return sizeof(uint8_t);
      break;
    case UINT16:
      return sizeof(uint16_t);
      break;
    case UINT32:
      return sizeof(uint32_t);
      break;
    case UINT64:
      return sizeof(uint64_t);
      break;
#endif
    case FLOAT:
      return sizeof(float);
      break;
    case DOUBLE:
      return sizeof(double);
      break;
    case TAR_POSITION:
      return sizeof(uint64_t);
      break;
    case REAL_INDEX:
      return sizeof(int64_t);
      break;
    case SUBTAR_POSITION:
      return sizeof(int64_t);
      break;
    case NO_TYPE:
      return 0;
      break;
    }
  }

  size_t getSize() {
    switch (_type) {
    case CHAR:
      return sizeof(char) * _vector_length;
      break;
#ifdef FULL_TYPE_SUPPORT
    case UCHAR:
      return sizeof(char) * 2 * _vector_length;
      break;
    case INT8:
      return sizeof(int8_t) * _vector_length;
      break;
    case INT16:
      return sizeof(int16_t) * _vector_length;
      break;
#endif
    case INT32:
      return sizeof(int32_t) * _vector_length;
      break;
    case INT64:
      return sizeof(int64_t) * _vector_length;
      break;
#ifdef FULL_TYPE_SUPPORT
    case UINT8:
      return sizeof(uint8_t) * _vector_length;
      break;
    case UINT16:
      return sizeof(uint16_t) * _vector_length;
      break;
    case UINT32:
      return sizeof(uint32_t) * _vector_length;
      break;
    case UINT64:
      return sizeof(uint64_t) * _vector_length;
      break;
#endif
    case FLOAT:
      return sizeof(float) * _vector_length;
      break;
    case DOUBLE:
      return sizeof(double) * _vector_length;
      break;
    case TAR_POSITION:
      return sizeof(uint64_t) * _vector_length;;
      break;
    case REAL_INDEX:
      return sizeof(int64_t) * _vector_length;;
      break;
    case SUBTAR_POSITION:
      return sizeof(int64_t) * _vector_length;;
      break;
    case NO_TYPE:
      return 0;
      break;
    }
  }

  bool isNumeric() {
    bool isString = _type == CHAR;
#ifdef FULL_TYPE_SUPPORT
    isString != _type == UCHAR;
#endif
    return !isString;
  }

  bool isIntegerType() {
    bool isString = _type == CHAR;
#ifdef FULL_TYPE_SUPPORT
    isString != _type == UCHAR;
#endif
    bool isReal = _type == FLOAT || _type == DOUBLE;
    return !isString && !isReal;
  }

  bool isVector() { return _vector_length > 1; }

  string toString() {

#ifdef FULL_TYPE_SUPPORT
    const char *dataTypeNames[] = {
        "char",   "uchar",  "int8",   "int16", "int32", "int64", "uint8",
        "uint16", "uint32", "uint64", "float", "double", "tar_position",
        "real_index", "subtar_position", "no type"};
#else
    const char *dataTypeNames[] = {
        "char", "int32", "int64", "float", "double", "tar_position",
        "real_index", "subtar_position", "no type"};
#endif

    string str(dataTypeNames[_type]);
    if (_vector_length > 1)
      str = str + _COLON + to_string(_vector_length);
    return str;
  }

  inline DataType &operator=(EnumDataType arg) {
    _vector_length = 1;
    _selected = 0;
    _type = arg;
    return *this;
  }

  inline bool operator==(EnumDataType arg) { return _type == arg; }

  inline bool operator!=(EnumDataType arg) { return _type != arg; }

  inline bool operator==(DataType arg) {
    return _type == arg._type && _vector_length == arg.vectorLength();
  }

  inline bool operator!=(DataType arg) {
    return _type != arg._type || _vector_length != arg.vectorLength();
  }
};

inline EnumDataType SelectType(DataType t1, DataType t2, string op) {
  if (op == _ADDITION || op == _SUBTRACTION || op == _MULTIPLICATION
      || op == _MODULUS || op == _DIV) {
    if (t1 == DOUBLE || t2 == DOUBLE)
      return DOUBLE;

    if (t1 == FLOAT || t2 == FLOAT)
      return FLOAT;

    if (t1.getSize() >= t2.getSize()) {
      return t1.type();
    } else {
      return t2.type();
    }
  } else if (op == _DIVISION) {
    if (t1.getSize() > sizeof(float) || t2.getSize() > sizeof(float)) {
      return DOUBLE;
    } else {
      return FLOAT;
    }
  } else {
    return DOUBLE;
  }
}

// Misc functions
inline int32_t TYPE_SIZE(DataType type) { return type.getSize(); }

inline EnumDataType STR2TYPE(const char *type) {
  string sType(type);
  transform(sType.begin(), sType.end(), sType.begin(), ::tolower);

  if (!strcmp(sType.c_str(), "char")) {
    return CHAR;
  }

#ifdef FULL_TYPE_SUPPORT
  if (!strcmp(sType.c_str(), "uchar")) {
    return UCHAR;
  }

  if (!strcmp(sType.c_str(), "int8")) {
    return INT8;
  }

  if (!strcmp(sType.c_str(), "int16")) {
    return INT16;
  }
#endif

  if (!strcmp(sType.c_str(), "int32") || !strcmp(sType.c_str(), "int")) {
    return INT32;
  }

  if (!strcmp(sType.c_str(), "int64") || !strcmp(sType.c_str(), "long")) {
    return INT64;
  }

#ifdef FULL_TYPE_SUPPORT
  if (!strcmp(sType.c_str(), "uint8")) {
    return UINT8;
  }

  if (!strcmp(sType.c_str(), "uint16")) {
    return UINT16;
  }

  if (!strcmp(sType.c_str(), "uint32")) {
    return UINT32;
  }

  if (!strcmp(sType.c_str(), "uint64")) {
    return UINT64;
  }
#endif

  if (!strcmp(sType.c_str(), "float")) {
    return FLOAT;
  }

  if (!strcmp(sType.c_str(), "double")) {
    return DOUBLE;
  }

  if (!strcmp(sType.c_str(), "tar_position")) {
    return TAR_POSITION;
  }

  if (!strcmp(sType.c_str(), "real_index")) {
    return REAL_INDEX;
  }

  if (!strcmp(sType.c_str(), "subtar_position")) {
    return SUBTAR_POSITION;
  }

  return NO_TYPE;
}

inline EnumDataType STR2TYPEV(const char *type, int32_t &size) {
  string sType(type);
  transform(sType.begin(), sType.end(), sType.begin(), ::tolower);
  auto splitType = split(string(type), ':');

  if (splitType.size() == 1) {
    size = 1;
    return STR2TYPE(type);
  } else if (splitType.size() == 2) {
    size = static_cast<int32_t>(strtol(splitType[1].c_str(), nullptr, 10));
    return STR2TYPE(splitType[0].c_str());
  }

  return NO_TYPE;
}

struct Literal {
  DataType type;
  union {
    char chr;
    int16_t uchr;
    int8_t int8;
    int16_t int16;
    int32_t int32;
    int64_t int64;
    uint8_t uint8;
    uint16_t uint16;
    uint32_t uint32;
    uint64_t uint64;
    float flt;
    double dbl;
  };
  string str;

  Literal() { type = DataType(); }

  Literal(double d) {
    dbl = d;
    type = DataType(DOUBLE);
  }

  Literal(string s) {
    str = s;
    type = DataType(CHAR);
  }

  void Simplify(double d) {
    if (d == ((int32_t)d)) {
      int32 = ((int32_t)d);
      type = INT32;
    } else if (d == ((int64_t)d)) {
      int64 = ((int64_t)d);
      type = INT64;
    } else if (d == ((float)d)) {
      flt = ((float)d);
      type = FLOAT;
    } else {
      dbl = d;
      type = DOUBLE;
    }
  }
  
  inline bool operator==(Literal arg) {
    if(type != arg.type)
      return false;
    
    switch (type.type()) {
    case CHAR:
      return arg.chr == chr;
#ifdef FULL_TYPE_SUPPORT
    case UCHAR:
      return arg.int16 == int16;
    case INT8:
      return arg.int8 == int8;
    case INT16:
      return arg.int16 == int16;
#endif
    case INT32:
      return arg.int32 == int32;
    case INT64:
      return arg.int64 == int64;
#ifdef FULL_TYPE_SUPPORT
    case UINT8:
      return arg.uint8 == uint8;
    case UINT16:
      return arg.uint16 == uint16;
    case UINT32:
      return arg.uint32 == uint32;
    case UINT64:
      return arg.uint64 == uint64;
#endif
    case FLOAT:
      return arg.flt == flt;
    case DOUBLE:
      return arg.dbl == dbl;
    case TAR_POSITION:
      return arg.uint64 == uint64;
    case REAL_INDEX:
      return arg.int64 == int64;
    case SUBTAR_POSITION:
      return arg.int64 == int64;
    }
    return false;
  }
};
//typedef Literal Literal;

#ifdef FULL_TYPE_SUPPORT
#define GET_LITERAL(X, T, Z)                                                   \
  switch (T.type.type()) {                                                     \
  case CHAR:                                                                   \
    X = (Z)T.chr;                                                              \
    break;                                                                     \
  case UCHAR:                                                                  \
    X = (Z)T.uchr;                                                             \
    break;                                                                     \
  case INT8:                                                                   \
    X = (Z)T.int8;                                                             \
    break;                                                                     \
  case INT16:                                                                  \
    X = (Z)T.int16;                                                            \
    break;                                                                     \
  case INT32:                                                                  \
    X = (Z)T.int32;                                                            \
    break;                                                                     \
  case INT64:                                                                  \
    X = (Z)T.int64;                                                            \
    break;                                                                     \
  case UINT8:                                                                  \
    X = (Z)T.uint8;                                                            \
    break;                                                                     \
  case UINT16:                                                                 \
    X = (Z)T.uint16;                                                           \
    break;                                                                     \
  case UINT32:                                                                 \
    X = (Z)T.uint32;                                                           \
    break;                                                                     \
  case UINT64:                                                                 \
    X = (Z)T.uint64;                                                           \
    break;                                                                     \
  case FLOAT:                                                                  \
    X = (Z)T.flt;                                                              \
    break;                                                                     \
  case DOUBLE:                                                                 \
    X = (Z)T.dbl;                                                              \
    break;                                                                     \
  case TAR_POSITION:                                                           \
     X = (Z)T.uint64;                                                          \
    break;                                                                     \
  case REAL_INDEX:                                                             \
     X = (Z)T.int64;                                                           \
    break;                                                                     \
  case SUBTAR_POSITION:                                                        \
    X = (Z)T.int64;                                                            \
    break;                                                                     \
  }

#define SET_LITERAL(X, T, V)                                                   \
  switch (T.type()) {                                                          \
  case CHAR:                                                                   \
    X.chr = (char)V;                                                           \
    break;                                                                     \
  case UCHAR:                                                                  \
    X.uchr = (int16_t)V;                                                       \
    break;                                                                     \
  case INT8:                                                                   \
    X.int8 = (int8_t)V;                                                        \
    break;                                                                     \
  case INT16:                                                                  \
    X.int16 = (int16_t)V;                                                      \
    break;                                                                     \
  case INT32:                                                                  \
    X.int32 = (int32_t)V;                                                      \
    break;                                                                     \
  case INT64:                                                                  \
    X.int64 = (int64_t)V;                                                      \
    break;                                                                     \
  case UINT8:                                                                  \
    X.uint8 = (uint8_t)V;                                                      \
    break;                                                                     \
  case UINT16:                                                                 \
    X.uint16 = (uint16_t)V;                                                    \
    break;                                                                     \
  case UINT32:                                                                 \
    X.uint32 = (uint32_t)V;                                                    \
    break;                                                                     \
  case UINT64:                                                                 \
    X.uint64 = (uint64_t)V;                                                    \
    break;                                                                     \
  case FLOAT:                                                                  \
    X.flt = (float)V;                                                          \
    break;                                                                     \
  case DOUBLE:                                                                 \
    X.dbl = (double)V;                                                         \
    break;                                                                     \
  case TAR_POSITION:                                                           \
    X.uint64 = (uint64_t)V;                                                    \
    break;                                                                     \
  case REAL_INDEX:                                                             \
    X.int64 = (int64_t)V;                                                      \
    break;                                                                     \
  case SUBTAR_POSITION:                                                        \
    X.int64 = (int64_t)V;                                                      \
    break;                                                                     \
  }                                                                            \
  X.type = T;

#else

  #define GET_LITERAL(X, T, Z)                                                 \
  switch (T.type.type()) {                                                     \
  case CHAR:                                                                   \
    X = (Z)T.chr;                                                              \
    break;                                                                     \
  case INT32:                                                                  \
    X = (Z)T.int32;                                                            \
    break;                                                                     \
  case INT64:                                                                  \
    X = (Z)T.int64;                                                            \
    break;                                                                     \
  case FLOAT:                                                                  \
    X = (Z)T.flt;                                                              \
    break;                                                                     \
  case DOUBLE:                                                                 \
    X = (Z)T.dbl;                                                              \
    break;                                                                     \
  case TAR_POSITION:                                                           \
     X = (Z)T.uint64;                                                          \
    break;                                                                     \
  case REAL_INDEX:                                                             \
     X = (Z)T.int64;                                                           \
    break;                                                                     \
  case SUBTAR_POSITION:                                                        \
    X = (Z)T.int64;                                                            \
    break;                                                                     \
  }

#define SET_LITERAL(X, T, V)                                                   \
  switch (T.type()) {                                                          \
  case CHAR:                                                                   \
    X.chr = (char)V;                                                           \
    break;                                                                     \
  case INT32:                                                                  \
    X.int32 = (int32_t)V;                                                      \
    break;                                                                     \
  case INT64:                                                                  \
    X.int64 = (int64_t)V;                                                      \
    break;                                                                     \
  case FLOAT:                                                                  \
    X.flt = (float)V;                                                          \
    break;                                                                     \
  case DOUBLE:                                                                 \
    X.dbl = (double)V;                                                         \
    break;                                                                     \
  case TAR_POSITION:                                                           \
    X.uint64 = (uint64_t)V;                                                    \
    break;                                                                     \
  case REAL_INDEX:                                                             \
    X.int64 = (int64_t)V;                                                      \
    break;                                                                     \
  case SUBTAR_POSITION:                                                        \
    X.int64 = (int64_t)V;                                                      \
    break;                                                                     \
  }                                                                            \
  X.type = T;                                                                  \

#endif

template<class T>
inline bool _check_precision_loss(double source) {
  T dest = (T)source;
  double residual = source - (double)dest;
  return residual != 0;
}

inline bool check_precision_loss(double source, DataType type) {

  switch (type.type()) {
  case CHAR:
    return _check_precision_loss<char>(source);
#ifdef FULL_TYPE_SUPPORT
    case UCHAR:
      return _check_precision_loss<int16_t>(source);
    case INT8:
      return _check_precision_loss<int8_t>(source);
    case INT16:
      return _check_precision_loss<int16_t>(source);
#endif
  case INT32:
    return _check_precision_loss<int32_t>(source);
  case INT64:
    return _check_precision_loss<int64_t>(source);
#ifdef FULL_TYPE_SUPPORT
    case UINT8:
      return _check_precision_loss<uint8_t>(source);
    case UINT16:
      return _check_precision_loss<uint16_t>(source);
    case UINT32:
      return _check_precision_loss<uint32_t>(source);
    case UINT64:
      return _check_precision_loss<uint64_t>(source);
#endif
  case FLOAT:
    return _check_precision_loss<float>(source);
  case DOUBLE:
    return false;
  case TAR_POSITION:
    return _check_precision_loss<uint64_t >(source);
  case REAL_INDEX:
    return _check_precision_loss<int64_t >(source);
  case SUBTAR_POSITION:
    return _check_precision_loss<int64_t >(source);
  }
  return false;
}

#endif /* TYPES_H */
