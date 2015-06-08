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
