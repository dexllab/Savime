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

#ifndef TEMPLATE_BUILDER_H
#define TEMPLATE_BUILDER_H

#include "default_template.h"
#include "default_apply_template.h"
#include "../core/include/abstract_storage_manager.h"

typedef unordered_map<int32_t,
                      unordered_map<int32_t,
                                    unordered_map<int32_t,
                                                  AbstractStorageManagerPtr>>>
    TemplateBuffer;

//Basic type support macros
#define DT1_APPLY_DEF(t1, t2, t3)                                              \
  if (t1.type() == CHAR) {                                                     \
    DT2_APPLY_DEF(t1, int8_t, t2, t3)                                          \
  } else if (t1.type() == INT32) {                                             \
    DT2_APPLY_DEF(t1, int32_t, t2, t3)                                         \
  } else if (t1.type() == INT64) {                                             \
    DT2_APPLY_DEF(t1, int64_t, t2, t3)                                         \
  } else if (t1.type() == FLOAT) {                                             \
    DT2_APPLY_DEF(t1, float, t2, t3)                                           \
  } else if (t1.type() == DOUBLE) {                                            \
    DT2_APPLY_DEF(t1, double, t2, t3)                                          \
  } else if (t1.type() == TAR_POSITION) {                                      \
    DT2_APPLY_DEF(t1, uint64_t, t2, t3)                                        \
  } else if (t1.type() == REAL_INDEX) {                                        \
    DT2_APPLY_DEF(t1, int64_t, t2, t3)                                         \
  } else if (t1.type() == SUBTAR_POSITION) {                                   \
    DT2_APPLY_DEF(t1, int64_t, t2, t3)                                         \
  }

#define DT2_APPLY_DEF(t1, def1, t2, t3)                                        \
  if (t2.type() == CHAR) {                                                     \
    DT3_APPLY_DEF(t1, def1, t2, char, t3)                                      \
  } else if (t2.type() == INT32) {                                             \
    DT3_APPLY_DEF(t1, def1, t2, int32_t, t3)                                   \
  } else if (t2.type() == INT64) {                                             \
    DT3_APPLY_DEF(t1, def1, t2, int64_t, t3)                                   \
  } else if (t2.type() == FLOAT) {                                             \
    DT3_APPLY_DEF(t1, def1, t2, float, t3)                                     \
  } else if (t2.type() == DOUBLE) {                                            \
    DT3_APPLY_DEF(t1, def1, t2, double, t3)                                    \
  } else if (t2.type() == TAR_POSITION) {                                      \
    DT3_APPLY_DEF(t1, def1, t2, uint64_t, t3)                                  \
  } else if (t2.type() == REAL_INDEX) {                                        \
    DT3_APPLY_DEF(t1, def1, t2, int64_t, t3)                                   \
  } else if (t2.type() == SUBTAR_POSITION) {                                   \
    DT3_APPLY_DEF(t1, def1, t2, int64_t, t3)                                   \
  }

#define DT3_APPLY_DEF(t1, def1, t2, def2, t3)                                  \
  if (t3.type() == CHAR) {                                                     \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, char>(                     \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == INT32) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int32_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == INT64) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int64_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == FLOAT) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, float>(                    \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == DOUBLE) {                                            \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, double>(                   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == TAR_POSITION) {                                      \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, uint64_t>(                 \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == REAL_INDEX) {                                        \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int64_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == SUBTAR_POSITION) {                                   \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int64_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  }

#define DT1_DEF(t1, t2)                                                        \
  if (t1.type() == CHAR) {                                                     \
    DT2_DEF(t1, char, t2)                                                      \
  } else if (t1.type() == INT32) {                                             \
    DT2_DEF(t1, int32_t, t2)                                                   \
  } else if (t1.type() == INT64) {                                             \
    DT2_DEF(t1, int64_t, t2)                                                   \
  } else if (t1.type() == FLOAT) {                                             \
    DT2_DEF(t1, float, t2)                                                     \
  } else if (t1.type() == DOUBLE) {                                            \
    DT2_DEF(t1, double, t2)                                                    \
  } else if (t1.type() == TAR_POSITION) {                                      \
    DT2_DEF(t1, uint64_t, t2)                                                  \
  } else if (t1.type() == REAL_INDEX) {                                        \
    DT2_DEF(t1, int64_t, t2)                                                   \
  } else if (t1.type() == SUBTAR_POSITION) {                                   \
    DT2_DEF(t1, int64_t, t2)                                                   \
  }

#define DT2_DEF(t1, def1, t2)                                                  \
  if (t2.type() == CHAR) {                                                     \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, char>(      \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == INT32) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int32_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == INT64) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int64_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == FLOAT) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, float>(     \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == DOUBLE) {                                            \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, double>(    \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == TAR_POSITION) {                                      \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, uint64_t>(  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == REAL_INDEX) {                                        \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int64_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == SUBTAR_POSITION) {                                   \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int64_t>(   \
            storageManager, configurationManager, systemLogger));              \
  }

// Full type support macros macros

#define T1_APPLY_DEF(t1, t2, t3)                                               \
  if (t1.type() == CHAR) {                                                     \
    T2_APPLY_DEF(t1, int8_t, t2, t3)                                           \
  } else if (t1.type() == UCHAR) {                                             \
    T2_APPLY_DEF(t1, int16_t, t2, t3)                                          \
  } else if (t1.type() == INT8) {                                              \
    T2_APPLY_DEF(t1, int8_t, t2, t3)                                           \
  } else if (t1.type() == INT16) {                                             \
    T2_APPLY_DEF(t1, int16_t, t2, t3)                                          \
  } else if (t1.type() == INT32) {                                             \
    T2_APPLY_DEF(t1, int32_t, t2, t3)                                          \
  } else if (t1.type() == INT64) {                                             \
    T2_APPLY_DEF(t1, int64_t, t2, t3)                                          \
  } else if (t1.type() == UINT8) {                                             \
    T2_APPLY_DEF(t1, uint8_t, t2, t3)                                          \
  } else if (t1.type() == UINT16) {                                            \
    T2_APPLY_DEF(t1, uint16_t, t2, t3)                                         \
  } else if (t1.type() == UINT32) {                                            \
    T2_APPLY_DEF(t1, uint32_t, t2, t3)                                         \
  } else if (t1.type() == UINT64) {                                            \
    T2_APPLY_DEF(t1, uint64_t, t2, t3)                                         \
  } else if (t1.type() == FLOAT) {                                             \
    T2_APPLY_DEF(t1, float, t2, t3)                                            \
  } else if (t1.type() == DOUBLE) {                                            \
    T2_APPLY_DEF(t1, double, t2, t3)                                           \
  } else if (t1.type() == TAR_POSITION) {                                      \
    T2_APPLY_DEF(t1, uint64_t, t2, t3)                                         \
  } else if (t1.type() == REAL_INDEX) {                                        \
    T2_APPLY_DEF(t1, int64_t, t2, t3)                                          \
  } else if (t1.type() == SUBTAR_POSITION) {                                   \
    T2_APPLY_DEF(t1, int64_t, t2, t3)                                          \
  }

#define T2_APPLY_DEF(t1, def1, t2, t3)                                         \
  if (t2.type() == CHAR) {                                                     \
    T3_APPLY_DEF(t1, def1, t2, char, t3)                                       \
  } else if (t2.type() == UCHAR) {                                             \
    T3_APPLY_DEF(t1, def1, t2, int16_t, t3)                                    \
  } else if (t2.type() == INT8) {                                              \
    T3_APPLY_DEF(t1, def1, t2, int8_t, t3)                                     \
  } else if (t2.type() == INT16) {                                             \
    T3_APPLY_DEF(t1, def1, t2, int16_t, t3)                                    \
  } else if (t2.type() == INT32) {                                             \
    T3_APPLY_DEF(t1, def1, t2, int32_t, t3)                                    \
  } else if (t2.type() == INT64) {                                             \
    T3_APPLY_DEF(t1, def1, t2, int64_t, t3)                                    \
  } else if (t2.type() == UINT8) {                                             \
    T3_APPLY_DEF(t1, def1, t2, uint8_t, t3)                                    \
  } else if (t2.type() == UINT16) {                                            \
    T3_APPLY_DEF(t1, def1, t2, uint16_t, t3)                                   \
  } else if (t2.type() == UINT32) {                                            \
    T3_APPLY_DEF(t1, def1, t2, uint32_t, t3)                                   \
  } else if (t2.type() == UINT64) {                                            \
    T3_APPLY_DEF(t1, def1, t2, uint64_t, t3)                                   \
  } else if (t2.type() == FLOAT) {                                             \
    T3_APPLY_DEF(t1, def1, t2, float, t3)                                      \
  } else if (t2.type() == DOUBLE) {                                            \
    T3_APPLY_DEF(t1, def1, t2, double, t3)                                     \
  } else if (t2.type() == TAR_POSITION) {                                      \
    T3_APPLY_DEF(t1, def1, t2, uint64_t, t3)                                   \
  } else if (t2.type() == REAL_INDEX) {                                        \
    T3_APPLY_DEF(t1, def1, t2, int64_t, t3)                                    \
  } else if (t2.type() == SUBTAR_POSITION) {                                   \
    T3_APPLY_DEF(t1, def1, t2, int64_t, t3)                                    \
  }

#define T3_APPLY_DEF(t1, def1, t2, def2, t3)                                   \
  if (t3.type() == CHAR) {                                                     \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, char>(                     \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == UCHAR) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int16_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == INT8) {                                              \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int8_t>(                   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == INT16) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int16_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == INT32) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int32_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == INT64) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int64_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == UINT8) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, uint8_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == UINT16) {                                            \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, uint16_t>(                 \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == UINT32) {                                            \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, uint32_t>(                 \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == UINT64) {                                            \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, uint64_t>(                 \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == FLOAT) {                                             \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, float>(                    \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == DOUBLE) {                                            \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, double>(                   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == TAR_POSITION) {                                      \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, uint64_t>(                 \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == REAL_INDEX) {                                        \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int64_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t3.type() == SUBTAR_POSITION) {                                   \
    _template = AbstractStorageManagerPtr(                                     \
        new TemplateApplyStorageManager<def1, def2, int64_t>(                  \
            storageManager, configurationManager, systemLogger));              \
  }


#define T1_DEF(t1, t2)                                                         \
  if (t1.type() == CHAR) {                                                     \
    T2_DEF(t1, char, t2)                                                       \
  } else if (t1.type() == UCHAR) {                                             \
    T2_DEF(t1, int16_t, t2)                                                    \
  } else if (t1.type() == INT8) {                                              \
    T2_DEF(t1, int8_t, t2)                                                     \
  } else if (t1.type() == INT16) {                                             \
    T2_DEF(t1, int16_t, t2)                                                    \
  } else if (t1.type() == INT32) {                                             \
    T2_DEF(t1, int32_t, t2)                                                    \
  } else if (t1.type() == INT64) {                                             \
    T2_DEF(t1, int64_t, t2)                                                    \
  } else if (t1.type() == UINT8) {                                             \
    T2_DEF(t1, uint8_t, t2)                                                    \
  } else if (t1.type() == UINT16) {                                            \
    T2_DEF(t1, uint16_t, t2)                                                   \
  } else if (t1.type() == UINT32) {                                            \
    T2_DEF(t1, uint32_t, t2)                                                   \
  } else if (t1.type() == UINT64) {                                            \
    T2_DEF(t1, uint64_t, t2)                                                   \
  } else if (t1.type() == FLOAT) {                                             \
    T2_DEF(t1, float, t2)                                                      \
  } else if (t1.type() == DOUBLE) {                                            \
    T2_DEF(t1, double, t2)                                                     \
  } else if (t1.type() == TAR_POSITION) {                                      \
    T2_DEF(t1, uint64_t, t2)                                                   \
  } else if (t1.type() == REAL_INDEX) {                                        \
    T2_DEF(t1, int64_t, t2)                                                    \
  } else if (t1.type() == SUBTAR_POSITION) {                                   \
    T2_DEF(t1, int64_t, t2)                                                    \
  }

#define T2_DEF(t1, def1, t2)                                                   \
  if (t2.type() == CHAR) {                                                     \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, char>(      \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == UCHAR) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int16_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == INT8) {                                              \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int8_t>(    \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == INT16) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int16_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == INT32) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int32_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == INT64) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int64_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == UINT8) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, uint8_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == UINT16) {                                            \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, uint16_t>(  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == UINT32) {                                            \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, uint32_t>(  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == UINT64) {                                            \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, uint64_t>(  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == FLOAT) {                                             \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, float>(     \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == DOUBLE) {                                            \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, double>(    \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == TAR_POSITION) {                                      \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, uint64_t>(  \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == REAL_INDEX) {                                        \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int64_t>(   \
            storageManager, configurationManager, systemLogger));              \
  } else if (t2.type() == SUBTAR_POSITION) {                                   \
    _template =                                                                \
        AbstractStorageManagerPtr(new TemplateStorageManager<def1, int64_t>(   \
            storageManager, configurationManager, systemLogger));              \
  }

class TemplateBuilder {
public:

  static std::mutex templateBuilderMutex;
  static AbstractStorageManagerPtr
  Build(StorageManagerPtr storageManager,
        ConfigurationManagerPtr configurationManager,
        SystemLoggerPtr systemLogger, DataType t1, DataType t2, DataType t3) {

    templateBuilderMutex.lock();
    static TemplateBuffer buffer;
    AbstractStorageManagerPtr _template;
    if(buffer[t1.type()][t2.type()][t3.type()] == nullptr) {
#ifdef FULL_TYPE_SUPPORT
      T1_DEF(t1, t2)
#else
      DT1_DEF(t1, t2)
#endif
      buffer[t1.type()][t2.type()][t3.type()] = _template;
      templateBuilderMutex.unlock();
      return _template;
    } else {
      templateBuilderMutex.unlock();
      return buffer[t1.type()][t2.type()][t3.type()];
    }
  }

  static AbstractStorageManagerPtr
  BuildApply(StorageManagerPtr storageManager,
             ConfigurationManagerPtr configurationManager,
             SystemLoggerPtr systemLogger, DataType t1, DataType t2,
             DataType t3) {
    templateBuilderMutex.lock();
    static TemplateBuffer buffer;
    AbstractStorageManagerPtr _template;
    if (buffer[t1.type()][t2.type()][t3.type()] == nullptr) {
#ifdef FULL_TYPE_SUPPORT
      T1_APPLY_DEF(t1, t2, t3)
#else
      DT1_APPLY_DEF(t1, t2, t3)
#endif
      buffer[t1.type()][t2.type()][t3.type()] = _template;
      templateBuilderMutex.unlock();
      return _template;
    } else {
      templateBuilderMutex.unlock();
      return buffer[t1.type()][t2.type()][t3.type()];
    }
  }
};

#endif /* TEMPLATE_BUILDER_H */

