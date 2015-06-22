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

#ifndef CPP_CAS_LIST_HPP
#define CPP_CAS_LIST_HPP

#include "table.hpp"

class CppCasList: public Shard {

  public:

    void read (uint32_t t, int k, Value &v) const {
      auto i = head.load();
      while (i != nullptr) {
        if (i->k == k && i->t <= t) {
          v = Value(i->v, i->t);
          return;
        }
        i = i->n;
      }
      v = Value(0, 0);
    }

    uint32_t prepare(int k) const {
      auto i = head.load();
      while (i != nullptr) {
        if (i->k == k)
          return i->t;
        i = i->n;
      }
      return 0;
    }

    void commit(uint32_t t, int k, int v) {
      auto n = new Node(k, v, t, head.load());
      while(!head.compare_exchange_weak(n->n, n));
    }

    void scan (uint32_t t, std::vector<Cell> &cs) const {
      auto i = head.load();
      while (i != nullptr) {
        if (i->t <= t)
          cs.emplace_back(i->k, i->v, i->t);
        i = i->n;
      }
    }

  private:

    struct Node {

      constexpr Node(): k(0), v(0), t(0), n(nullptr) {}

      constexpr Node(int _k, int _v, uint32_t _t, Node *_n): k(_k), v(_v), t(_t), n(_n) {}

      int k, v;
      uint32_t t;
      Node *n;
    };

    std::atomic<Node*> head;
};

#endif // CPP_CAS_LIST_HPP
