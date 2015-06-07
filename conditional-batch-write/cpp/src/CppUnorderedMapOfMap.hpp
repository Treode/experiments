#ifndef CPP_UNORDERED_MAP_OF_MAP_HPP
#define CPP_UNORDERED_MAP_OF_MAP_HPP

#include <map>
#include <unordered_map>
#include <vector>

#include "table.hpp"

class CppUnorderedMapOfMap: public Table, Shard {

  public:

    uint32_t time() const {
      return clock;
    }

    Value read (uint32_t t, int k) const;

    void read(uint32_t t, size_t n, const int *ks, Value *vs) const;

    uint32_t prepare(const Row &r) const;

    void commit(uint32_t t, const Row &r);

    uint32_t write(uint32_t t, size_t n, const Row *rs);

    std::vector<Cell> scan (uint32_t t) const;

    std::vector<Cell> scan() const;

  private:

    mutable uint32_t clock = 0;

    std::unordered_map<int, std::map<uint32_t, int>> table;

    void raise(uint32_t t) const {
      if (clock < t)
        clock = t;
    }

    void prepare(uint32_t t, size_t n, const Row *rs) const;

    uint32_t commit(size_t n, const Row *rs);

    std::vector<Cell> _scan (uint32_t t) const;
};

#endif // CPP_UNORDERED_MAP_OF_MAP_HPP
