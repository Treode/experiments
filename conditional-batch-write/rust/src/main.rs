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

#![feature(collections)]
#![feature(slice_patterns)]

extern crate rand;
extern crate time;

use std::collections::{BTreeMap, HashMap};
use std::collections::Bound::{Included, Unbounded};
use std::vec::Vec;
use rand::thread_rng;
use rand::distributions::Sample;
use rand::distributions::range::Range;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Value {
  pub v: i32,
  pub t: u32
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Row {
  pub k: i32,
  pub v: i32
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Cell {
  pub k: i32,
  pub v: i32,
  pub t: u32
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteResult {
  Ok { t: u32 },
  Stale { cond: u32, max: u32 }
}

trait Table {

  fn time(&self) -> u32;

  fn read(&mut self, t: u32, ks: &[i32], vs: &mut [Value]);

  fn write(&mut self, t: u32, rs: &[Row]) -> WriteResult;

  fn scan(&self) -> Vec<Cell>;
}

fn broker(table: &mut Table, ntransfers: u32) {

  let mut rng = thread_rng();
  let mut ract = Range::new(0, 100);
  let mut ramt = Range::new(0, 1000);
  let mut nstale = 0;

  for _ in 0..ntransfers {
    let a1 = ract.sample(&mut rng);
    let mut a2 = ract.sample(&mut rng);
    while a2 == a1 {
      a2 = ract.sample(&mut rng);
    }
    let n = ramt.sample(&mut rng);

    let rt = table.time();
    let mut vs = [Value::default(); 2];
    table.read(rt, &[a1, a2], &mut vs);
    let r1 = Row { k: a1, v: vs[0].v - n };
    let r2 = Row { k: a2, v: vs[1].v + n };
    match table.write(rt, &[r1, r2]) {
      WriteResult::Stale { .. } => nstale += 1,
      _ => ()
    }
  }
  assert!(nstale < ntransfers / 2);
}

fn expect_money_conserved(table: &Table) {
  let mut history: BTreeMap<u32, Vec<Row>> = BTreeMap::new();
  for c in table.scan() {
    history.entry(c.t).or_insert(Vec::new()).push(Row { k: c.k, v: c.v });
  }
  let mut tracker: HashMap<i32, i32> = HashMap::new();
  for (_, rs) in history {
    for r in rs {
      tracker.insert(r.k, r.v);
    }
    let mut sum = 0;
    for (_, v) in tracker.clone() {
      sum += v;
    }
    assert! (sum == 0);
  }
}

struct HashMapOfTreeMap {
  time: u32,
  table: HashMap<i32, BTreeMap<u32, i32>>
}

impl HashMapOfTreeMap {

  pub fn new() -> HashMapOfTreeMap {
    HashMapOfTreeMap {
      time: 0,
      table: HashMap::new()
    }
  }

  fn raise(&mut self, t: u32) {
    if self.time < t {
      self.time = t;
    }
  }

  fn read_row(&self, t: u32, k: i32) -> Value {
    let x = u32::max_value() - t;
    match self.table.get(&k) {
      Some(vs) =>
        match vs.range(Included(&x), Unbounded).min() {
          Some((t, v)) => Value { v: *v, t: u32::max_value() - *t },
          None => Value::default()
        },
      None => Value::default()
    }
  }

  fn prepare_row(&self, k: i32) -> u32 {
    match self.table.get(&k) {
      Some(vs) =>
        match vs.iter().next() {
          Some((x, _)) => u32::max_value() - x,
          None => 0
        },
      None => 0
    }
  }

  fn prepare_rows(&self, rs: &[Row]) -> u32 {
    let mut max = 0;
    for i in 0..rs.len() {
      let t = self.prepare_row(rs[i].k);
      if max < t {
        max = t
      }
    }
    max
  }

  fn commit_row(&mut self, t: u32, r: Row) {
    let mut vs = self.table.entry(r.k).or_insert_with(BTreeMap::new);
    vs.insert(u32::max_value() - t, r.v);
  }

  fn commit_rows(&mut self, rs: &[Row]) -> u32 {
    self.time += 1;
    let t = self.time;
    for i in 0..rs.len() {
      self.commit_row(t, rs[i])
    }
    t
  }
}

impl Table for HashMapOfTreeMap {

  fn time(&self) -> u32 {
    self.time
  }

  fn read(&mut self, t: u32, ks: &[i32], vs: &mut [Value]) {
    self.raise(t);
    for i in 0..ks.len() {
      vs[i] = self.read_row(t, ks[i]);
    }
  }

  fn write (&mut self, t: u32, rs: &[Row]) -> WriteResult {
    self.raise(t);
    let m = self.prepare_rows(rs);
    if m > t {
      return WriteResult::Stale { cond: t, max: m };
    }
    let w = self.commit_rows(rs);
    WriteResult::Ok { t: w }
  }

  fn scan(&self) -> Vec<Cell> {
    let mut cs: Vec<Cell> = Vec::new();
    for (k, vs) in self.table.clone() {
      for (t, v) in vs {
        cs.push(Cell { k: k, v: v, t: u32::max_value() - t });
      }
    }
    return cs;
  }
}

#[test]
fn a_table_should_read_0_for_any_key() {
  let mut table = HashMapOfTreeMap::new();
  let mut vs = [Value::default(); 1];
  table.read (0, &[0], &mut vs);
  match vs {
    [Value { v: 0, t: 0}] => (),
    _ => assert!(false)
  }
}

#[test]
fn a_table_should_read_what_was_put() {
  let mut table = HashMapOfTreeMap::new();
  table.write(0, &[Row { k: 0, v: 1 }]);
  let mut vs = [Value::default(); 1];
  table.read (1, &[0], &mut vs);
  assert_eq!(vs, [Value { v: 1, t: 1 }]);
}

#[test]
fn a_table_should_read_and_write_batches() {
  let mut table = HashMapOfTreeMap::new();
  table.write(0, &[Row { k: 0, v: 1 }, Row { k: 1, v: 2 }]);
  let mut vs = [Value::default(); 2];
  table.read (1, &[0, 1], &mut vs);
  assert_eq!(vs, [Value { v: 1, t: 1 }, Value { v: 2, t: 1 }]);
}

#[test]
fn a_table_should_reject_a_stale_write() {
  let mut table = HashMapOfTreeMap::new();
  table.write(0, &[Row { k: 0, v: 1 }]);
  assert_eq!(table.write(0, &[Row { k: 0, v: 2 }]), WriteResult::Stale { cond: 0, max: 1 });
  let mut vs = [Value::default(); 1];
  table.read (1, &[0], &mut vs);
  assert_eq!(vs, [Value { v: 1, t: 1 }]);
}

#[test]
fn a_table_should_preserve_the_money_supply() {
  let mut table = HashMapOfTreeMap::new();
  broker(&mut table, 1000);
  expect_money_conserved(&table);
}

fn main() {

  let nhits = 20;
  let ntrials = 2000;
  let nnanos = 60 * 1000 * 1000 * 1000;
  let ntransfers = 1000;
  let nbrokers = 8;
  let tolerance = 0.05;
  let ops = (ntransfers * nbrokers) as f64;
  let million = (1000 * 1000) as f64;

  let mut sum = 0.0;

  let mut hits = 0;
  let mut trial = 0;
  let limit = time::precise_time_ns() + nnanos;
  while hits < nhits && trial < ntrials && time::precise_time_ns() < limit {
    let mut table = HashMapOfTreeMap::new();
    let start = time::precise_time_ns();
    for _ in 0..nbrokers {
      broker(&mut table, ntransfers);
    }
    let end = time::precise_time_ns();
    let ns = (end - start) as f64;
    let x = ops / ns * million;
    sum += x;
    let n = (trial + 1) as f64;
    let mean = sum / n;
    let dev = (x - mean).abs() / mean;
    if dev <= tolerance {
      println!("{:5} {:8.2} ops/ms ({:8.2})", trial, x, mean);
      hits += 1;
    }
    trial += 1;
  }
}
