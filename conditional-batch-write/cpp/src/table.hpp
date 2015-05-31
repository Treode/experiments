#ifndef TABLE_HPP
#define TABLE_HPP

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

struct Value {

  int v;
  uint32_t t;

  Value() {}

  constexpr Value(int _v, uint32_t _t): v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Value &v);

bool operator==(const Value &x, const Value &y);

inline bool operator!=(const Value &x, const Value &y) {
  return !(x == y);
}

struct Row {

  int k, v;

  constexpr Row(int _k, int _v): k(_k), v(_v) {}
};

std::ostream &operator<<(std::ostream &os, const Row &r);

bool operator==(const Row &x, const Row &y);

inline bool operator!=(const Row &x, const Row &y) {
  return !(x == y);
}

struct Cell {

  int k, v;
  uint32_t t;

  constexpr Cell(int _k, int _v, uint32_t _t): k(_k), v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Cell &c);

bool operator==(const Cell &x, const Cell &y);

inline bool operator!=(const Cell &x, const Cell &y) {
  return !(x == y);
}

class stale_exception: public std::runtime_error {

  public:

    uint32_t cond, max;

    stale_exception (uint32_t _cond, uint32_t _max):
      runtime_error ("stale"),
      cond (_cond),
      max (_max)
    {}
};

class Table {

  public:

    virtual ~Table() = default;

    virtual uint32_t time() const = 0;

    virtual void read(uint32_t t, size_t n, const int *ks, Value *vs) = 0;

    virtual uint32_t write(uint32_t t, size_t n, const Row *rs) = 0;

    virtual std::vector<Cell> scan() const =  0;

    std::vector<Value> read(uint32_t t, const std::vector<int> &ks) {
      auto vs = std::vector<Value>(ks.size());
      read(t, ks.size(), ks.data(), vs.data());
      return vs;
    }

    uint32_t write(uint32_t t, const std::vector<Row> &rs) {
      return write(t, rs.size(), rs.data());
    }
};

std::ostream &operator<<(std::ostream &os, const Table &table);

void broker(Table &table, unsigned ntransfers);

#endif // TABLE_HPP
