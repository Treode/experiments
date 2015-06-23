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

#include <random>
#include <memory>

#include "async.hpp"

using std::chrono::high_resolution_clock;
using std::function;
using std::unique_ptr;
using tbb::task;
using tbb::task_list;

class Broker: public task {

  public:

    Broker(AsyncTable &_table, const Params &params):
      racct(0, params.naccounts),
      ramt(0, 1000),
      table(_table),
      time(0),
      state(State::read),
      count(params.ntransfers / params.nbrokers)
    {}

    task *execute() {
      switch (state) {

        case State::read:
          if (count-- == 0)
            return nullptr;

          a1 = racct(reng);
          while ((a2 = racct (reng)) == a1);
          n = ramt(reng);
          //cout << "transfer " << n << " from " << a1 << " to " << a2 << endl;

          ks[0] = a1;
          ks[1] = a2;
          set_ref_count(1);
          recycle_as_continuation();
          state = State::write;
          return table.read (time, 2, ks, vs, this);

        case State::write:
          rs[0].k = a1;
          rs[0].v = vs[0].v - n;
          rs[1].k = a2;
          rs[1].v = vs[1].v - n;
          set_ref_count(1);
          recycle_as_continuation();
          state = State::read;
          return table.write (time, 2, rs, time, this);
      }
    }

  private:

    enum State { read, write };

    std::default_random_engine reng;
    std::uniform_int_distribution<int> racct;
    std::uniform_int_distribution<int> ramt;
    AsyncTable &table;
    uint32_t time;

    State state;
    unsigned count;
    int a1, a2, n;
    int ks[2];
    Value vs[2];
    Row rs[2];
};

high_resolution_clock::duration
async_brokers(const function<AsyncTable*(void)> &new_table, const Params &params) {
  unique_ptr<AsyncTable> table (new_table());
  task_list brokers;
  for (int i = 0; i < params.nbrokers; ++i) {
    auto &t = *new(task::allocate_root()) Broker(*table, params);
    brokers.push_back(t);
  }
  auto start = high_resolution_clock::now();
  task::spawn_root_and_wait(brokers);
  auto end = high_resolution_clock::now();
  return end - start;
}
