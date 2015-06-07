#include "ConditionLock.hpp"

using std::lock_guard;
using std::mutex;
using std::unique_lock;

uint32_t ConditionLock::time() {
  lock_guard<mutex> acqn(lock);
  return getTime(state);
}

void ConditionLock::read(uint32_t time) {
  unique_lock<mutex> acqn(lock);
  while (true) {
    if (time <= getTime(state))
      return;
    if (!isHeld(state)) {
      state = makeState(time, false);
      return;
    }
    if (future < time)
      future = time;
    readers.wait(acqn);
  }
}

uint32_t ConditionLock::write(uint32_t time) {
  unique_lock<mutex> acqn(lock);
  while (isHeld(state))
    writers.wait(acqn);
  uint32_t now = getTime(state);
  if (now < time)
    now = time;
  state = makeState(now, true);
  return now;
}

void ConditionLock::release(uint32_t time) {
  lock_guard<mutex> acqn(lock);
  if (future < time)
    future = time;
  state = makeState(future, false);
  readers.notify_all();
  writers.notify_one();
}
