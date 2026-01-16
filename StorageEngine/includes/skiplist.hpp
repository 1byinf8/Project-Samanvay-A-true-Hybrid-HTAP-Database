// skiplist.hpp - Lock-free concurrent skiplist for HTAP Database
#pragma once
#ifndef SKIPLIST_HPP
#define SKIPLIST_HPP

#include <atomic>
#include <random>
#include <string>
#include <vector>

namespace skiplist {

struct Entry {
  std::string key;
  std::string value;
  bool isDeleted = false;
  uint64_t seq;

  Entry() : seq(0) {}

  Entry(const std::string &k, const std::string &v, uint64_t s,
        bool del = false)
      : key(k), value(v), seq(s), isDeleted(del) {}
};

struct SkiplistNode {
  Entry value;
  const int topLevel;
  std::atomic<bool> marked;
  std::vector<std::atomic<SkiplistNode *>> forward;

  SkiplistNode(int level, const Entry &val)
      : value(val), topLevel(level), marked(false), forward(level + 1) {
    for (int i = 0; i <= level; ++i) {
      forward[i].store(nullptr, std::memory_order_relaxed);
    }
  }
};

// Forward declaration
class SkiplistIterator;

class Skiplist {
private:
  using Node = SkiplistNode;

  static const int MAX_LEVEL = 16;
  static constexpr float P_FACTOR = 0.5f;

  Node *header;
  std::atomic<int> currentMaxLevel;

  // For random level generation (thread_local for concurrency)
  thread_local static std::mt19937 gen;

  int randomLevel() {
    int lvl = 0;
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);
    while (dis(gen) < P_FACTOR && lvl < MAX_LEVEL) {
      lvl++;
    }
    return lvl;
  }

  // Sentinel value factory
  static Entry sentinel() { return Entry("", "", 0); }

  // Comparison helpers
  static bool less_than(const Entry &a, const Entry &b) {
    return a.key < b.key;
  }

  static bool equal(const Entry &a, const Entry &b) { return a.key == b.key; }

  // Fills preds and succs arrays for a given key.
  // Returns true if a node with the key is found (regardless of mark).
  bool find(const Entry &key, std::vector<Node *> &preds,
            std::vector<Node *> &succs) {
    Node *pred = nullptr, *curr = nullptr, *succ = nullptr;
  retry:
    pred = header;
    for (int level = MAX_LEVEL; level >= 0; --level) {
      curr = pred->forward[level].load(std::memory_order_acquire);
      while (curr != nullptr) {
        succ = curr->forward[level].load(std::memory_order_acquire);
        while (curr->marked.load(std::memory_order_acquire)) {
          // Help with physical removal (snipping)
          if (!pred->forward[level].compare_exchange_strong(curr, succ)) {
            goto retry; // Restart from the top if CAS fails
          }
          curr = succ;
          if (curr == nullptr)
            break;
          succ = curr->forward[level].load(std::memory_order_acquire);
        }

        if (curr != nullptr && less_than(curr->value, key)) {
          pred = curr;
          curr = succ;
        } else {
          break;
        }
      }
      preds[level] = pred;
      succs[level] = curr;
    }
    return (curr != nullptr && equal(curr->value, key));
  }

public:
  Skiplist() : currentMaxLevel(0) {
    // Create header with sentinel value
    header = new SkiplistNode(MAX_LEVEL, sentinel());
  }

  ~Skiplist() {
    Node *curr = header->forward[0].load();
    while (curr != nullptr) {
      Node *next = curr->forward[0].load();
      delete curr;
      curr = next;
    }
    delete header;
  }

  bool search(const Entry &target, Entry *result = nullptr) {
    std::vector<Node *> preds(MAX_LEVEL + 1);
    std::vector<Node *> succs(MAX_LEVEL + 1);
    bool found = find(target, preds, succs);
    if (found && result && !succs[0]->marked.load(std::memory_order_acquire)) {
      *result = succs[0]->value;
    }
    return found && !succs[0]->marked.load(std::memory_order_acquire);
  }

  bool add(const Entry &key) {
    int topLevel = randomLevel();
    std::vector<Node *> preds(MAX_LEVEL + 1);
    std::vector<Node *> succs(MAX_LEVEL + 1);

    while (true) {
      bool found = find(key, preds, succs);
      if (found) {
        // To prevent race with erase, check if found node is marked.
        // If not marked, it truly exists. If marked, retry.
        if (!succs[0]->marked.load(std::memory_order_acquire)) {
          return false; // Already exists and not marked
        }
        // If marked, another thread is erasing it, so we can retry inserting.
        continue;
      }

      Node *newNode = new Node(topLevel, key);

      // Link bottom level first (linearization point)
      newNode->forward[0].store(succs[0], std::memory_order_relaxed);
      if (!preds[0]->forward[0].compare_exchange_strong(succs[0], newNode)) {
        delete newNode; // Failed, clean up and retry
        continue;
      }

      // Link the remaining levels
      for (int level = 1; level <= topLevel; ++level) {
        while (true) {
          newNode->forward[level].store(succs[level],
                                        std::memory_order_relaxed);
          if (preds[level]->forward[level].compare_exchange_strong(succs[level],
                                                                   newNode)) {
            break; // Success, move to next level
          }
          // Failed CAS, pointers have changed. Re-find predecessors.
          find(key, preds, succs);
        }
      }
      return true;
    }
  }

  bool erase(const Entry &key) {
    std::vector<Node *> preds(MAX_LEVEL + 1);
    std::vector<Node *> succs(MAX_LEVEL + 1);

    while (true) {
      bool found = find(key, preds, succs);
      if (!found) {
        return false; // Not in the list
      }

      Node *nodeToDelete = succs[0];

      // If already marked by another thread, consider deletion successful.
      // Or, try to mark it ourselves.
      bool expected = false;
      if (!nodeToDelete->marked.compare_exchange_strong(expected, true)) {
        // Another thread already marked it. Our job is done.
        return true;
      }

      // After logical deletion, help with physical removal and return
      find(key, preds, succs);
      return true;
    }
  }

  void clear() {
    Node *curr = header->forward[0].load();
    while (curr != nullptr) {
      Node *next = curr->forward[0].load();
      delete curr;
      curr = next;
    }
    for (auto &ptr : header->forward) {
      ptr.store(nullptr, std::memory_order_relaxed);
    }
    currentMaxLevel.store(0);
  }

  // Iterator methods
  SkiplistIterator begin();
  SkiplistIterator end();
  SkiplistIterator lowerBound(const std::string &key);
  SkiplistIterator upperBound(const std::string &key);

  // Range query implementation
  std::vector<std::pair<std::string, std::string>>
  rangeQuery(const std::string &startKey, const std::string &endKey);

  // Scan all entries
  std::vector<std::pair<std::string, std::string>> scanAll();
};

// Iterator class definition
class SkiplistIterator {
private:
  SkiplistNode *current;

public:
  SkiplistIterator(SkiplistNode *node) : current(node) {}

  bool isValid() const {
    return current != nullptr &&
           !current->marked.load(std::memory_order_acquire);
  }

  Entry &operator*() { return current->value; }

  Entry *operator->() { return &current->value; }

  SkiplistIterator &operator++() {
    if (current) {
      current = current->forward[0].load(std::memory_order_acquire);
      // Skip marked nodes
      while (current && current->marked.load(std::memory_order_acquire)) {
        current = current->forward[0].load(std::memory_order_acquire);
      }
    }
    return *this;
  }

  SkiplistIterator operator++(int) {
    SkiplistIterator temp = *this;
    ++(*this);
    return temp;
  }

  bool operator==(const SkiplistIterator &other) const {
    return current == other.current;
  }

  bool operator!=(const SkiplistIterator &other) const {
    return current != other.current;
  }
};

// Implementation of iterator methods (inline)
inline SkiplistIterator Skiplist::begin() {
  SkiplistNode *first = header->forward[0].load(std::memory_order_acquire);
  // Skip marked nodes
  while (first && first->marked.load(std::memory_order_acquire)) {
    first = first->forward[0].load(std::memory_order_acquire);
  }
  return SkiplistIterator(first);
}

inline SkiplistIterator Skiplist::end() { return SkiplistIterator(nullptr); }

inline SkiplistIterator Skiplist::lowerBound(const std::string &key) {
  std::vector<Node *> preds(MAX_LEVEL + 1);
  std::vector<Node *> succs(MAX_LEVEL + 1);
  Entry target(key, "", 0);
  find(target, preds, succs);

  SkiplistNode *node = succs[0];
  // Skip marked nodes
  while (node && node->marked.load(std::memory_order_acquire)) {
    node = node->forward[0].load(std::memory_order_acquire);
  }
  return SkiplistIterator(node);
}

inline SkiplistIterator Skiplist::upperBound(const std::string &key) {
  auto it = lowerBound(key);
  if (it.isValid() && it->key == key) {
    ++it;
  }
  return it;
}

inline std::vector<std::pair<std::string, std::string>>
Skiplist::rangeQuery(const std::string &startKey, const std::string &endKey) {
  std::vector<std::pair<std::string, std::string>> results;

  auto it = lowerBound(startKey);
  while (it.isValid() && it->key <= endKey) {
    if (!it->isDeleted) {
      results.push_back({it->key, it->value});
    }
    ++it;
  }
  return results;
}

inline std::vector<std::pair<std::string, std::string>> Skiplist::scanAll() {
  std::vector<std::pair<std::string, std::string>> results;

  for (auto it = begin(); it != end(); ++it) {
    if (!it->isDeleted) {
      results.push_back({it->key, it->value});
    }
  }
  return results;
}

// Define thread_local static member (C++17 inline variable)
// This ensures single definition across all translation units
inline thread_local std::mt19937 Skiplist::gen(std::random_device{}());

} // namespace skiplist

#endif // SKIPLIST_HPP