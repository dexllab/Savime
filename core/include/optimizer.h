#include <utility>

#ifndef OPTIMIZER_H
#define OPTIMIZER_H
/*! \file */
#include "parser.h"
#include "savime.h"

/**
 * The module responsible for receiving a query plan and optimize it.
 */
class Optimizer : public SavimeModule {
public:
  /**
  * Constructor.
  * @param configurationManager is a reference to the standard system
  * ConfigurationManager.
  * @param configurationManager is a reference to the standard SystemLogger.
  */
  Optimizer(ConfigurationManagerPtr configurationManager,
            SystemLoggerPtr systemLogger, MetadataManagerPtr metadataManager)
      : SavimeModule("Optimizer", std::move(configurationManager), systemLogger) {

  }

  virtual void SetParser(ParserPtr parser) = 0;

  /**
  * Optimizes a query plan.
  * @param queryDataManager is a QueryDataManager reference containing the
  * QueryPlan.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Optimize(QueryDataManagerPtr queryDataManager) = 0;
};
typedef std::shared_ptr<Optimizer> OptimizerPtr;

#endif /* OPTIMIZER_H */

