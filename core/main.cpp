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
#include <config.h>
#include <stdio.h>
#include <string>
#include <stdexcept>
#include <mutex>
#include <condition_variable>
#include "include/savime.h"
#include "include/builder.h"
#include "include/connection_manager.h"

#define LOOP() while(true){conditionVar.wait(locker);}

int main(int argc, char **args) {

  std::condition_variable conditionVar;
  std::mutex lock;
  std::unique_lock<std::mutex> locker(lock);
  ModulesBuilder *builder = nullptr;

  try {
    builder = new ModulesBuilder(argc, args);
    ConnectionManagerPtr connectionManager = builder->BuildConnectionManager();
    SessionManagerPtr sessionManager = builder->BuildSessionManager();

    if (sessionManager->Start() == SAVIME_FAILURE) {
      throw std::runtime_error(
          "Error during session manager initialization. Abort!");
    }

    if (connectionManager->Start() == SAVIME_FAILURE) {
      throw std::runtime_error(
          "Error during connection manager initialization. Abort!");
    }

    LOOP();
  } catch (std::exception &e) {
    if (builder)
      builder->BuildSystemLogger()->LogEvent("SAVIME", e.what());
  }

  return 0;
}