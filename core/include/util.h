#ifndef UTIL_H
#define UTIL_H
//Auxiliary functions

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <fstream>
#include <algorithm>
#include <vector>
#include <string>
#include <omp.h>
#include <chrono>
#include <atomic>
#include <memory>
#include <string>
#include <time.h>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <cxxabi.h>
#include "symbols.h"

using namespace std;
using namespace chrono;

//------------------------------------------------------------------------------
// TIME MEASUREMENTES MACROS
#define GET_T1() auto t1 = high_resolution_clock::now()
#define GET_T2() auto t2 = high_resolution_clock::now()
#define GET_T1_LOCAL() t1 = high_resolution_clock::now()
#define GET_T2_LOCAL() t2 = high_resolution_clock::now()
#define GET_DURATION() duration_cast<microseconds>(t2 - t1).count() / 1000
typedef std::chrono::time_point<std::chrono::high_resolution_clock> SavimeTime;

//------------------------------------------------------------------------------
// COMPARSION MACRO
#define IN_RANGE(X, Y, Z) ((X >= Y) && (X <= Z))

//------------------------------------------------------------------------------
// EXECPTIONS
#define OMP_EXCEPTION_ENV() std::mutex ___ex_mtx;\
                            vector<std::string> ___omp_temp_exceptions;\

#define TRY() try{

#define CATCH() } catch(std::exception& e) { \
                 ___ex_mtx.lock();\
                 ___omp_temp_exceptions.push_back(e.what());\
                 ___ex_mtx.unlock();\
                }

#define RETHROW() if(!___omp_temp_exceptions.empty()){\
                    string whats = ___omp_temp_exceptions[0];\
                    throw runtime_error(whats);\
                  }

//------------------------------------------------------------------------------
// FILES FUNCTIONS
inline std::ifstream::pos_type FILE_SIZE(const char *filename) {
  std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
  return in.tellg();
}

inline bool EXIST_FILE(const std::string &name) {
  struct stat buffer;
  return (stat(name.c_str(), &buffer) == 0);
}

inline string pathAppend(const string& p1, const string& p2) {
  char sep = '/';
  string tmp = p1;

#ifdef _WIN32
  sep = '\\';
#endif

  if (p1[p1.length()] != sep) { 
    tmp += sep;               
    return(tmp + p2);
  }
  else
    return(p1 + p2);
}

inline std::string generateUniqueFileName(std::string path, int32_t dirs) {
#define MAX_CHAR 1024
  static std::atomic<uint32_t> uid{0};
  char number[MAX_CHAR];
  std::string uniqueFile;

  do {
    int32_t _uid = uid++;
    int32_t dirNumber = _uid%dirs;
    snprintf(number, MAX_CHAR, "%x%x%x", _uid, omp_get_thread_num(), getpid());
    path = pathAppend(path, std::to_string(dirNumber));
    
    if(!EXIST_FILE(path))
      mkdir(path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    
    uniqueFile = pathAppend(path, string(number));
  } while (EXIST_FILE(uniqueFile));

  return uniqueFile;
}

inline int fd_set_blocking(int fd, int blocking) {
  /* Save the current flags */
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return 0;

  if (blocking)
    flags &= ~O_NONBLOCK;
  else
    flags |= O_NONBLOCK;
  return fcntl(fd, F_SETFL, flags) != -1;
}

//------------------------------------------------------------------------------
// STRINGS FUNCTIONS
inline std::string between(std::string source, std::string delimiter1,
                           std::string delimiter2) {
  unsigned first = source.find(delimiter1);
  unsigned last = source.find(delimiter2);

  if (first == std::string::npos || last == std::string::npos) {
    return "";
  }

  return source.substr(first + 1, last - first - 1);
}

inline std::vector<std::string> split(const std::string &str, const char &ch) {
  std::string next;
  std::vector<std::string> result;

  for (std::string::const_iterator it = str.begin(); it != str.end(); it++) {
    if (*it == ch) {
      if (!next.empty()) {
        result.push_back(next);
        next.clear();
      }
    } else {
      next += *it;
    }
  }

  if (!next.empty())
    result.push_back(next);

  return result;
}

inline std::string &ltrim(std::string &str) {
  auto it2 = std::find_if(str.begin(), str.end(), [](char ch) {
    return !std::isspace<char>(ch, std::locale::classic());
  });
  str.erase(str.begin(), it2);
  return str;
}

inline std::string &rtrim(std::string &str) {
  auto it1 = std::find_if(str.rbegin(), str.rend(), [](char ch) {
    return !std::isspace<char>(ch, std::locale::classic());
  });
  str.erase(it1.base(), str.end());
  return str;
}

inline std::string &trim(std::string &str) { return ltrim(rtrim(str)); }

inline std::string &remove_str(std::string &str, char c) {
  str.erase(std::remove(str.begin(), str.end(), c), str.end());
  return str;
}

inline std::string &trim_delimiters(std::string &str) {
  return remove_str(remove_str(str, '"'), '\'');
}

////MISC FUNCTIONS
//------------------------------------------------------------------------------
// Function original source:
// https://www.codeproject.com/Articles/17480/Optimizing-integer-divisions-with-Multiply-Shift-i
// val/div == (val*mul) >> shift;
inline void fast_division(int64_t max, int64_t div, int64_t &mul,
                          int64_t &shift) {
  bool found = false;
  mul = -1;
  shift = -1;

  // zero divider error
  if (div == 0)
    return;

  // this division would always return 0 from 0..max
  if (max < div) {
    mul = 0;
    shift = 0;
    return;
  }

  // catch powers of 2
  for (int s = 0; s <= 63; s++) {
    if (div == (1L << s)) {
      mul = 1;
      shift = s;
      return;
    }
  }

  // start searching for a valid mul/shift pair
  for (shift = 1; shift <= 62; shift++) {
    // shift factor is at least 2log(div), skip others
    if ((1L << shift) <= div)
      continue;

    // we calculate a candidate for mul
    mul = (1L << shift) / div + 1;

    // assume it is a good one
    found = true;

    // test if it works for the range 0 .. max
    // Note: takes too much time for large values of max.
    if (max < 1000000) {
      for (int64_t i = max; i >= 1;
           i--) // testing large values first fails faster
      {
        if ((i / div) != ((i * mul) >> shift)) {
          found = false;
          break;
        }
      }
    } else {
      // very fast test, no mathematical proof yet but it seems to work well
      // test highest number-pair for which the division must 'jump' correctly
      // test max, to be sure;
      int64_t t = (max / div + 1) * div;
      if ((((t - 1) / div) != (((t - 1) * mul) >> shift)) ||
          ((t / div) != ((t * mul) >> shift)) ||
          ((max / div) != ((max * mul) >> shift))) {
        found = false;
      }
    }

    // are we ready?
    if (found) {
      break;
    }
  }
}

#define DIVIDE(N, MUL, SHIFT) (N*MUL) >> SHIFT
#define REMAINDER(N, MUL, SHIFT, DIV) N - ((N*MUL) >> SHIFT)*DIV

#define SET_THREADS(WKLD, MIN, THREADS)                                        \
  int64_t startPositionPerCore[THREADS];                                       \
  int64_t finalPositionPerCore[THREADS];                                       \
  SetWorkloadPerThread(WKLD, MIN, startPositionPerCore, finalPositionPerCore,  \
                       THREADS);

#define RESET_THREADS(WKLD, MIN, THREADS)                                      \
  SetWorkloadPerThread(WKLD, MIN, startPositionPerCore, finalPositionPerCore,  \
                       THREADS);

#define SET_THREADS_ALIGNED(WKLD, MIN, THREADS, ALIGN)                         \
  int64_t startPositionPerCore[THREADS];                                       \
  int64_t finalPositionPerCore[THREADS];                                       \
  SetWorkloadPerThread(WKLD, MIN, startPositionPerCore, finalPositionPerCore,  \
                       THREADS, ALIGN);


#define THREAD_FIRST() startPositionPerCore[omp_get_thread_num()]
#define THREAD_LAST() finalPositionPerCore[omp_get_thread_num()]


inline int SetWorkloadPerThread(int64_t workloadSize, int32_t minWorkPerThread,
                                int64_t startPositionPerCore[],
                                int64_t finalPositionPerCore[],
                                int32_t numThreads) {

  int32_t numMinChunks = workloadSize / minWorkPerThread;
  if(numMinChunks == 0) 
    numThreads = 1;
  else if(numMinChunks <= numThreads)
    numThreads = numMinChunks;

  for (int64_t i = 0; i < numThreads; i++){
    startPositionPerCore[i] = -1;
    finalPositionPerCore[i] = 0;
  }

  int64_t chunk = workloadSize / numThreads;

  if (chunk > minWorkPerThread) {
    for (int64_t i = 0; i < numThreads - 1; i++) {
      finalPositionPerCore[i] = startPositionPerCore[i + 1] = (i + 1) * chunk;
    }

    startPositionPerCore[0] = 0;
    finalPositionPerCore[numThreads - 1] = workloadSize;
  } else {
    numThreads = 1;
    startPositionPerCore[0] = 0;
    finalPositionPerCore[0] = workloadSize;
  }
  
  omp_set_num_threads(numThreads);
  return numThreads;
}

inline int SetWorkloadPerThread(int64_t workloadSize, int32_t minWorkPerThread,
                                int64_t startPositionPerCore[],
                                int64_t finalPositionPerCore[],
                                int32_t numThreads, int32_t alignment) {

  int32_t numMinChunks = workloadSize / minWorkPerThread;
  if(numMinChunks == 0) 
    numThreads = 1;
  else if(numMinChunks <= numThreads)
    numThreads = numMinChunks;

  for (int64_t i = 0; i < numThreads; i++){
    startPositionPerCore[i] = -1;
    finalPositionPerCore[i] = 0;
  }

  int64_t chunk = ((workloadSize / alignment) / numThreads) * alignment;

  if (chunk > minWorkPerThread) {
    for (int64_t i = 0; i < numThreads - 1; i++) {
      finalPositionPerCore[i] = startPositionPerCore[i + 1] = (i + 1) * chunk;
    }

    startPositionPerCore[0] = 0;
    finalPositionPerCore[numThreads - 1] = workloadSize;
  } else {
    numThreads = 1;
    startPositionPerCore[0] = 0;
    finalPositionPerCore[0] = workloadSize;
  }

  omp_set_num_threads(numThreads);
  return numThreads;
}

#endif /* UTIL_H */
