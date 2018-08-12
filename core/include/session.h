#ifndef JOB_MANAGER_H
#define JOB_MANAGER_H
/*! \file */
#include <memory>
#include "optimizer.h"
#include "savime.h"
#include "engine.h"
#include "connection_manager.h"

using namespace std;

/**
 * A Session is created to execute the clients request.
 */
class Session {
public:
  /**
  * Executes the protocol for communicating with a client and calls other
  * modules,
  * such as the Parser, Optimizer and the Engine for answering the user's
  * requests.
  */
  virtual void Run() = 0;
};
typedef Session *SessionPtr;

/**
 * The SessionManager is responsible for subscribe the ConnectionManager for new
 * connections, and then creates and runs a Session to answer the connection
 * request.
 */
class SessionManager : public SavimeModule, public ConnectionListener {

public:
  /**
  * Constructor.
  * @param configurationManager is the standard ConfigurationManager.
  * @param systemLogger is the standard SystemLogger.
  */
  SessionManager(ConfigurationManagerPtr configurationManager,
                 SystemLoggerPtr systemLogger)
      : SavimeModule("Session Manager", configurationManager, systemLogger) {}

  /**
  * Sets the standard Engine.
  * @param engine is the standard Engine.
  */
  virtual void SetEngine(EnginePtr engine) = 0;

  /**
  * Sets the standard Parser.
  * @param parser is the standard Parser.
  */
  virtual void SetParser(ParserPtr parser) = 0;

  /**
  * Sets the standard Optimizer.
  * @param optimizer is the standard Optimizer.
  */
  virtual void SetOptmizer(OptimizerPtr optimizer) = 0;

  /**
  * Sets the standard MetadataManager.
  * @param metadaManager is the standard MetadataManager.
  */
  virtual void SetMetadaManager(MetadataManagerPtr metadaManager) = 0;

  /**
  * Starts the SessionManager instance.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult Start() = 0;

  /**
  * Signalizes the Session that it must be ended.
  * @param session is Session that must be ended.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult EndSession(SessionPtr session) = 0;

  /**
  * Signalizes the SessionManager that all Sessions must be stopped.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult EndAllSessions() = 0;

  /**
  * Stops the SessionManager instance.
  * @return SAVIME_SUCCESS on success or SAVIME_FAILURE on failure.
  */
  virtual SavimeResult End() = 0;
};
typedef shared_ptr<SessionManager> SessionManagerPtr;

#endif /* JOB_MANAGER_H */
