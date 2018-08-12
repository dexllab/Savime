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
*    ALLAN MATHEUS				JANUARY 2018
*/


#include <iostream>
#include <stdexcept>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "mapped_memory.h"

#define HUGE_PAGESIZE (2 << 21)

inline size_t align_len(size_t psize, size_t len) {
  size_t new_len = psize * (len / psize + (len % psize != 0));
  // std::cerr << "align" << len << " => " << new_len << '\n';
  return new_len;
}

mapped_memory::mapped_memory(int fd, size_t offset, size_t len, char *buffer,
                             bool hugetlb)
    : _len{len - offset}, _off{offset}, _should_close{false}, _fd{fd} {
  // std::cerr << "mm ctor " << fd << '\n';
  if (_fd < 0) {
    throw std::runtime_error(std::string("Invalid filedescriptor"));
  }
  if (!len) {
    _cap = get_filesize(_fd);
    _len = _cap - offset;
  } else {
    _cap = len;
  }
  if (hugetlb) {
    _cap = align_len(HUGE_PAGESIZE, _cap);
  }
  _buf = do_map(_fd, buffer, 0, _cap, hugetlb);
}

inline int open_rdwr(const char *name) {
  return open(name, O_CREAT | O_RDWR, 0644);
}

mapped_memory::mapped_memory(const std::string &name, size_t offset, size_t len,
                             char *buffer, bool hugetlb)
    : mapped_memory{open_rdwr(name.c_str()), offset, len, buffer, hugetlb} {
  // std::cerr << "mm str ctor\n";
  _should_close = true;
}

mapped_memory::mapped_memory(mapped_memory &&mm)
    : _buf{mm._buf}, _len{mm._len}, _off{mm._off},
      _should_close{mm._should_close}, _fd{mm._fd}, _cap{mm._cap} {
  // std::cerr << "mm move ctor\n";
  mm._buf = nullptr;
  mm._len = 0;
  mm._off = 0;
  mm._should_close = false;
  mm._fd = -1;
  mm._cap = 0;
}

mapped_memory::~mapped_memory() {
  // std::cerr << "mm dtor\n";
  if (_buf) {
    munmap(_buf, _cap);
  }
  if (_fd > 0) {
    fsync(_fd);
    if (_should_close) {
      close(_fd);
    }
    _fd = -1;
  }
}

mapped_memory &mapped_memory::operator=(mapped_memory &&mm) {
  // std::cerr << "mm move operator=\n";
  if (this != &mm) {
    if (_buf) {
      munmap(_buf, _cap);
    }
    if (_fd > 0) {
      fsync(_fd);
      if (_should_close) {
        close(_fd);
      }
      _fd = -1;
    }
    _buf = mm._buf;
    _len = mm._len;
    _off = mm._off;
    _fd = mm._fd;
    _cap = mm._cap;
    _should_close = mm._should_close;
    mm._buf = nullptr;
    mm._len = 0;
    mm._off = 0;
    mm._fd = -1;
    mm._should_close = false;
    mm._cap = 0;
  }
  return *this;
}

char *mapped_memory::get() const {
  // std::cerr << "mm get '" << (void *)_buf << "' + " << _off << " ; " << _len
  // << " / " << _cap << "\n";
  return _buf + _off;
}

size_t mapped_memory::size() const { return _len; }

size_t mapped_memory::capacity() const { return _cap; }

char *do_map(int fd, void *buffer, size_t offset, size_t len, bool hugetlb) {
  // std::cerr << "mmap: " << fd << ' ' << offset << ' ' << len << ' ' <<
  // hugetlb << '\n';
  constexpr int prot = PROT_READ | PROT_WRITE;
  constexpr int flags = MAP_SHARED;
  char *buf = (char *)mmap(buffer, len, prot, flags, fd, (off_t)offset);
  if (buf == MAP_FAILED) {
    throw std::runtime_error(std::string("mmap: ") + strerror(errno));
  }
  return buf;
}

size_t get_filesize(int fd) {
  struct stat sb;
  if (fstat(fd, &sb) == -1) {
    throw std::runtime_error(std::string("fstat: ") + strerror(errno));
  }
  // std::cerr << "filesize (" << fd << ") = " << sb.st_size << '\n';
  return (size_t)sb.st_size;
}

size_t get_filesize(const std::string &name) {
  size_t size = 0;
  auto fd = open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, (mode_t)0644);
  if (fd < 0) {
    throw std::runtime_error(std::string("open:") + strerror(errno));
  }
  try {
    size = get_filesize(fd);
  } catch (...) {
    close(fd);
    throw;
  }
  close(fd);
  return size;
}

void do_truncate(int fd, size_t len) {
  // std::cerr << "truncating " << fd << " to " << len << '\n';
  if (ftruncate(fd, (off_t)len) == -1) {
    throw std::runtime_error(std::string("fstat: ") + strerror(errno));
  }
}

void do_truncate(const std::string &name, size_t len) {
  // std::cerr << "truncating '" << name << "' to " << len << '\n';
  auto fd = open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, (mode_t)0644);
  if (fd < 0) {
    throw std::runtime_error(std::string("open:") + strerror(errno));
  }
  try {
    do_truncate(fd, len);
  } catch (...) {
    close(fd);
    throw;
  }
  close(fd);
}
