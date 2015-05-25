#ifndef TABLE_HPP
#define TABLE_HPP

#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <vector>

/** A table (or key-value table, hash table) using conditional batch write. Keys and values are
  * integers to keep this simple, because our focus is on comparing performance of different
  * implementation options.
  */
// class Table {

//   int time();

//   /** Read keys `ks` as of time `t`. */
//   vector<value> read(int t, vector<int> ks);

//   * Write rows `rs` if they haven't changed since time `t`.
//   int write(int t, vector<row> rs);

//   /** Scan the entire history. This is not part of the performance timings. */
//   vector<cell> scan();

//   void close();
// };

struct Value {

  const int v;
  const uint32_t t;

  constexpr Value(int _v, uint32_t _t): v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Value &v);

bool operator==(const Value &x, const Value &y);

inline bool operator!=(const Value &x, const Value &y) {
  return !(x == y);
}

struct Row {

  const int k, v;

  constexpr Row(int _k, int _v): k(_k), v(_v) {}
};

std::ostream &operator<<(std::ostream &os, const Row &r);

bool operator==(const Row &x, const Row &y);

inline bool operator!=(const Row &x, const Row &y) {
  return !(x == y);
}

struct Cell {

  const int k, v;
  const uint32_t t;

  constexpr Cell(int _k, int _v, uint32_t _t): k(_k), v(_v), t(_t) {}
};

std::ostream &operator<<(std::ostream &os, const Cell &c);

bool operator==(const Cell &x, const Cell &y);

inline bool operator!=(const Cell &x, const Cell &y) {
  return !(x == y);
}

class stale_exception: public std::runtime_error {

  public:

    uint32_t condition, maximum;

    stale_exception (uint32_t _condition, uint32_t _maximum):
      runtime_error ("stale"),
      condition (_condition),
      maximum (_maximum)
    {}
};

class Table {

  public:

    virtual ~Table() = default;

    virtual uint32_t time() const = 0;

    virtual std::vector<Value> read(uint32_t t, std::initializer_list<int> ks) = 0;

    virtual uint32_t write(uint32_t t, std::initializer_list<Row> rs) = 0;

    virtual std::vector<Cell> scan() const =  0;
};

std::ostream &operator<<(std::ostream &os, const Table &table);

void broker(Table &table);

#endif // TABLE_HPP
