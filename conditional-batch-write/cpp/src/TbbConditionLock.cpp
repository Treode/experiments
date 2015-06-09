/*
 * Copyright 2015 Treode, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "TbbConditionLock.hpp"

using tbb::spin_mutex;

uint32_t TbbConditionLock::time() {
  spin_mutex::scoped_lock acqn(lock);
  return getTime(state);
}

void TbbConditionLock::read(uint32_t time) {
  lock.lock();
  while (true) {
    if (time <= getTime(state))
      break;
    if (!isHeld(state)) {
      state = makeState(time, false);
      break;
    }
    if (future < time)
      future = time;
    readers.wait(lock);
  }
  lock.unlock();
}

uint32_t TbbConditionLock::write(uint32_t time) {
  lock.lock();
  while (isHeld(state))
    writers.wait(lock);
  uint32_t now = getTime(state);
  if (now < time)
    now = time;
  state = makeState(now, true);
  lock.unlock();
  return now;
}

void TbbConditionLock::release(uint32_t time) {
  spin_mutex::scoped_lock acqn(lock);
  if (future < time)
    future = time;
  state = makeState(future, false);
  readers.notify_all();
  writers.notify_one();
}
