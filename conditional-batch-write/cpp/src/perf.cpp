#include <ctime>
#include <iostream>

#include "CppUnorderedMapOfMap.hpp"

using std::cout;
using std::endl;

int main() {

  int nhits = 20;
  int ntrials = 2000;
  int nclocks = 60 * CLOCKS_PER_SEC;
  double tolerance = 0.05;

  double sum = 0.0;

  for (
    unsigned long hits = 0, trial = 0, limit = clock() + nclocks;
    hits < nhits && trial < ntrials && clock() < limit;
    ++trial
  ) {
    CppUnorderedMapOfMap table;
    auto start = clock();
    for (int i = 0; i < 8; ++i)
      broker(table);
    auto end = clock();
    double us = end - start;
    double ops = 1000 * 8;
    double x = ops / us * (CLOCKS_PER_SEC / 1000);
    sum += x;
    double n = trial + 1;
    double mean = sum / n;
    double dev = std::abs (x - mean) / mean;
    if (dev <= tolerance) {
      cout << trial << " " << x << " ops/ms (" << mean << ")" << endl;
      ++hits;
    } else {
      cout << trial << " " << x << " ops/ms (" << mean << " nope)" << endl;
    }
  }
}
