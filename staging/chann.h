#ifndef CHANN_H
#define CHANN_H

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <list>
#include <ostream>
#include <stdexcept>

#if __cplusplus
extern "C" {
#endif

/**
 * chann_version - Returns the current version of this library as a static
 * string.  Its callable from C, making easy to verify the existence and
 * usability of this library with GNU Autotools (autoconf).
 */
const char *chann_version(void);

#if __cplusplus
}
#endif

template <typename T> class chann;

class closed_channel : public std::logic_error {
public:
  closed_channel(const std::string &s) : std::logic_error{s} {}
};

template <typename T>
std::ostream &operator<<(std::ostream &o, const chann<T> &c);

template <typename T> class chann {
public:
  chann();

  ~chann();

  /**
   * push - Inserts a copy of el on this channel.
   */
  void push(const T &el);

  /**
   * push - Inserts el (move it) on this channel.
   */
  void push(T &&el);

  /**
   * pop - Removes the first element from the channel.
   *
   * Returns the removed element.
   */
  T pop();

  /**
   * pop - Removes the first element from the channel.
   *
   * Returns the true if there is a valid  element, false if this channel is
   * closed.  I.e., no need to catch a closed_channel exception.
   */
  bool pop(T &el);

  /**
   * close - Closes a channel, so no one can write to it.
   */
  void close();

  /**
   * flush - Blocks until this channel becomes empty.
   */
  void flush();

  /**
   * reset - Reopen this channel if its closed, otherwise do nothing.
   */
  void reset();

  bool is_closed() const;

  std::size_t size() const;

private:
  bool _closed;
  std::list<T> _queue;
  std::mutex _queue_mtx;
  std::condition_variable _queue_cv;

  friend std::ostream &operator<<<>(std::ostream &o, const chann &c);
};

template <typename T> chann<T>::chann() : _closed{false} {}

template <typename T> chann<T>::~chann() {}

template <typename T> void chann<T>::push(const T &el) {
  {
    std::lock_guard<std::mutex> lk{_queue_mtx};

    if (_closed) {
      throw closed_channel("Can't push to a closed channel.");
    }

    _queue.push_back(el);
    // std::cerr << "[push] queue size: " << _queue.size() << std::endl;
  }

  _queue_cv.notify_one();
}

template <typename T> void chann<T>::push(T &&el) {
  {
    std::lock_guard<std::mutex> lk{_queue_mtx};

    if (_closed) {
      throw closed_channel("Can't push to a closed channel.");
    }

    _queue.push_back(std::move(el));
    // std::cerr << "[push] queue size: " << _queue.size() << std::endl;
  }

  _queue_cv.notify_one();
}

template <typename T> T chann<T>::pop() {
  std::unique_lock<std::mutex> lk{_queue_mtx};
  _queue_cv.wait(lk, [this] { return _closed || !_queue.empty(); });

  if (_queue.empty()) {
    throw closed_channel("Can't pop from a closed empty channel.");
  }

  T el{std::move(_queue.front())};
  _queue.pop_front();

  // std::cerr << "[pop] queue size: " << _queue.size() << std::endl;
  lk.unlock();
  _queue_cv.notify_one();

  return el;
}

template <typename T> bool chann<T>::pop(T &el) {
  std::unique_lock<std::mutex> lk{_queue_mtx};
  _queue_cv.wait(lk, [this] { return _closed || !_queue.empty(); });

  if (_queue.empty()) {
    return false;
  }

  el = std::move(_queue.front());
  _queue.pop_front();

  // std::cerr << "[pop] queue size: " << _queue.size() << std::endl;
  lk.unlock();
  _queue_cv.notify_one();

  return true;
}

template <typename T> std::size_t chann<T>::size() const {
  return _queue.size();
}

template <typename T> void chann<T>::close() {
  std::unique_lock<std::mutex> lk{_queue_mtx};
  _closed = true;
  lk.unlock();
  _queue_cv.notify_one();
  // std::cerr << "closed" << std::endl;
}

template <typename T> void chann<T>::flush() {
  std::unique_lock<std::mutex> lk{_queue_mtx};
  _queue_cv.wait(lk, [this] { return _queue.empty(); });
  // std::cerr << "flushed" << std::endl;
}

template <typename T> void chann<T>::reset() {
  std::unique_lock<std::mutex> lk{_queue_mtx};
  _closed = true;
  lk.unlock();
  _queue_cv.notify_one();
}

template <typename T> bool chann<T>::is_closed() const { return _closed; }

template <typename T>
std::ostream &operator<<(std::ostream &o, const chann<T> &c) {
  o << "{ ";
  for (auto e = std::begin(c._queue); e != std::end(c._queue); ++e) {
    o << *e << ' ';
  }
  o << '}';
  return o;
}

#endif // CHANN_H
