#ifndef MAPPED_MEMORY_H
#define MAPPED_MEMORY_H

#include <string>

class mapped_memory {
public:
  mapped_memory(int fd, size_t offset = 0, size_t len = 0, char *buf = nullptr,
                bool hugetlb = false);
  mapped_memory(const std::string &name, size_t offset = 0, size_t len = 0,
                char *buf = nullptr, bool hugetlb = false);
  mapped_memory(mapped_memory &&mm);
  ~mapped_memory();

  mapped_memory &operator=(mapped_memory &&mm);

  char *get() const;
  size_t size() const;
  size_t capacity() const;

private:
  char *_buf;
  size_t _len;
  size_t _off;
  bool _should_close;
  int _fd;
  size_t _cap; // can be page aligned, if using hugepages
};

char *do_map(int fd, void *buffer, size_t offset, size_t len, bool hugetlb);

size_t get_filesize(int fd);
size_t get_filesize(const std::string &name);

void do_truncate(int fd, size_t len);
void do_truncate(const std::string &name, size_t len);

#endif // MAPPED_MEMORY_H
