#include <utility>

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
#include <ucontext.h>
#include <execinfo.h>
#include <cxxabi.h>
#include <fstream>
#include <string>
#include <iostream>
#include "include/symbols.h"
#include "include/system_logger.h"

ConfigurationManagerPtr DefaultSystemLogger::_configurationManager;

const char *signame[] = {"INVALID", "SIGHUP", "SIGINT", "SIGQUIT", "SIGILL",
                   "SIGTRAP", "SIGABRT", "SIGBUS", "SIGFPE", "SIGKILL",
                   "SIGUSR1", "SIGSEGV", "SIGUSR2", "SIGPIPE", "SIGALRM",
                   "SIGTERM", "SIGSTKFLT", "SIGCHLD", "SIGCONT", "SIGSTOP",
                   "SIGTSTP", "SIGTTIN", "SIGTTOU", "SIGURG", "SIGXCPU",
                   "SIGXFSZ", "SIGVTALRM", "SIGPROF", "SIGWINCH", "SIGPOLL",
                   "SIGPWR", "SIGSYS", nullptr};

DefaultSystemLogger::DefaultSystemLogger(
    ConfigurationManagerPtr configurationManager) {
  _configurationManager = std::move(configurationManager);
  signal(SIGSEGV, SystemLoggerHandler);
  signal(SIGHUP, SystemLoggerHandler);
  signal(SIGINT, SystemLoggerHandler);
  signal(SIGILL, SystemLoggerHandler);
  signal(SIGFPE, SystemLoggerHandler);
  signal(SIGPIPE, SystemLoggerHandler);
  signal(SIGSTKFLT, SystemLoggerHandler);
  signal(SIGBUS, SystemLoggerHandler);
}

string DefaultSystemLogger::GatherStackTrace() {

  unsigned int max_frames = 63;
  string stackTrace = "Stack trace:\n";
  void *addrlist[max_frames + 1];
  int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void *));

  if (addrlen == 0) {
    stackTrace += "  <empty, possibly corrupt>\n";
    return stackTrace;
  }

  char **symbollist = backtrace_symbols(addrlist, addrlen);
  size_t funcnamesize = 256;
  char *funcname = (char *) malloc(funcnamesize);

  for (int i = 1; i < addrlen; i++) {
    char *begin_name = nullptr, *begin_offset = nullptr, *end_offset = nullptr;

    for (char *p = symbollist[i]; *p; ++p) {
      if (*p == '(')
        begin_name = p;
      else if (*p == '+')
        begin_offset = p;
      else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset && begin_name < begin_offset) {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      int status;
      char *ret =
          abi::__cxa_demangle(begin_name, funcname, &funcnamesize, &status);
      if (status == 0) {
        funcname = ret; //use possibly realloc()-ed string
        stackTrace += "         " + string(symbollist[i]) + " : " +
            string(funcname) + _ADDITION + string(begin_offset) + "\n";
      } else {
        stackTrace += "         " + string(symbollist[i]) + " : " +
            string(begin_name) + _ADDITION + string(begin_offset) + "\n";
      }
    } else {
      stackTrace += " " + string(symbollist[i]) + "\n";
    }
  }

  free(funcname);
  free(symbollist);
  return stackTrace;
}

void DefaultSystemLogger::SystemLoggerHandler(int sig) {
  string stack = GatherStackTrace();
  string log_file = _configurationManager->GetStringValue(LOG_TRACE_FILE);
  std::ofstream out(log_file);
  out << stack;
  out.close();

  WriteLog("System Logger", "Signal " + string(signame[sig]) + " caught.");
  if (sig != SIGPIPE) {
    WriteLog("System Logger",
             "Shutting down Savime. Stack trace logged at " + log_file);
#ifdef DEBUG
    WriteLog("System Logger", stack);
#endif
    exit(EXIT_FAILURE);
  }
}

void DefaultSystemLogger::WriteLog(std::string module, std::string message) {
  time_t t = time(nullptr);
  struct tm tm = *localtime(&t);
  printf("%02d-%02d-%02d %02d:%02d:%02d [%s]: %s\n",
         tm.tm_year + 1900,
         tm.tm_mon + 1,
         tm.tm_mday,
         tm.tm_hour,
         tm.tm_min,
         tm.tm_sec,
         module.c_str(),
         message.c_str());
}

void DefaultSystemLogger::LogEvent(std::string module, std::string message) {
  WriteLog(module, message);
}