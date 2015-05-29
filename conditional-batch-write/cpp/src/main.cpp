#include <ctime>
#include <iostream>

#include "CppUnorderedMapOfMap.hpp"

using std::cout;
using std::endl;

int main() {

  unsigned nhits = 20;
  unsigned ntrials = 2000;
  unsigned nclocks = 60 * CLOCKS_PER_SEC;
  unsigned ntransfers = 1000;
  unsigned nbrokers = 8;
  double tolerance = 0.05;
  double ops = ntransfers * nbrokers;

  double sum = 0.0;

  for (
    unsigned long hits = 0, trial = 0, limit = clock() + nclocks;
    hits < nhits && trial < ntrials && clock() < limit;
    ++trial
  ) {
    CppUnorderedMapOfMap table;
    auto start = clock();
    for (int i = 0; i < nbrokers; ++i)
      broker(table, ntransfers);
    auto end = clock();
    double us = end - start;
    double x = ops / us * (CLOCKS_PER_SEC / 1000);
    sum += x;
    double n = trial + 1;
    double mean = sum / n;
    double dev = std::abs (x - mean) / mean;
    if (dev <= tolerance) {
      cout << trial << " " << x << " ops/ms (" << mean << ")" << endl;
      ++hits;
    }
  }
}
