#ifndef CONDITION_LOCK_HPP
#define CONDITION_LOCK_HPP

#import <condition_variable>
#import <mutex>

#import "Lock.hpp"

class ConditionLock: public Lock {

  public:

    uint32_t time();

    void read(uint32_t time);

    uint32_t write(uint32_t time);

    void release(uint32_t time);

  private:

    uint32_t getTime(uint32_t s) {
      return s >> 1;
    }

    bool isHeld(uint32_t s) {
      return s & 1;
    }

    uint32_t makeState (uint32_t time, bool held) {
      if (held)
        return (time << 1) | 1;
      else
        return time << 1;
    }

    std::mutex lock;
    std::condition_variable readers;
    std::condition_variable writers;
    uint32_t state = 0;
    uint32_t future = 0;
};

#endif // CONDITION_LOCK_HPP
