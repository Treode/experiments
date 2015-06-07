#ifndef LOCK_HPP
#define LOCK_HPP

#include <algorithm>
#include <vector>

#include "table.hpp"

class Lock {

  virtual uint32_t time() = 0;

  virtual void read(uint32_t time) = 0;

  virtual uint32_t write(uint32_t time) = 0;

  virtual void release(uint32_t time) = 0;
};

#endif // LOCK_HPP
