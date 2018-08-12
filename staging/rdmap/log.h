#ifndef LOG_H
#define LOG_H

#ifndef DEBUG

inline void log(const std::string& s)
{
    (void)s;
}

inline void debug(const std::string& s)
{
    (void)s;
}

#else

#include <iostream>

inline void log(const std::string& s)
{
    std::cout << s << '\n';
}

inline void debug(const std::string& s)
{
    std::cerr << s << '\n';
}

#endif // DEBUG

#endif // LOG_H
