#ifndef CPP_UNORDERED_MAP_OF_MAP_HPP
#define CPP_UNORDERED_MAP_OF_MAP_HPP

#include <map>
#include <unordered_map>
#include <vector>

#include "table.hpp"

class CppUnorderedMapOfMap: public Table {

  public:

    uint32_t time() const {
      return clock;
    }

    std::vector<Value> read(uint32_t t, std::initializer_list<int> ks);

    uint32_t write(uint32_t t, std::initializer_list<Row> rs);

    std::vector<Cell> scan() const;

  private:

    uint32_t clock = 0;

    std::unordered_map<int, std::map<uint32_t, int>> table;

    void raise(uint32_t t) {
      if (clock < t)
        clock = t;
    }

    Value read(uint32_t t, int k) const;

    int prepare(const Row &r) const;

    void prepare(uint32_t t, std::initializer_list<Row> rs) const;

    void commit(uint32_t t, const Row &r);

    uint32_t commit(const std::initializer_list<Row> rs);
};

#endif // CPP_UNORDERED_MAP_OF_MAP_HPP
