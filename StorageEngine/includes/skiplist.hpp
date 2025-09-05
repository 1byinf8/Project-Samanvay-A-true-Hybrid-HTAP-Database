#include <atomic>
#include <memory>
#include <random>
#include <vector>
#include <limits>

namespace skiplist {

template<typename T>
struct SkiplistNode {
    T value;
    const int topLevel;
    std::atomic<bool> marked;
    std::vector<std::atomic<SkiplistNode*>> forward;

    SkiplistNode(int level, const T& val)
        : value(val), 
          topLevel(level), 
          marked(false), 
          forward(level + 1) {
        for (int i = 0; i <= level; ++i) {
            forward[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    // No explicit destructor needed due to std::vector
};

template<typename T>
class Skiplist {
private:
    using Node = SkiplistNode<T>;

    static const int MAX_LEVEL = 16;
    static constexpr float P_FACTOR = 0.5f;

    Node* header;
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

    // Fills preds and succs arrays for a given key.
    // Returns true if a node with the key is found (regardless of mark).
    bool find(const T& key, std::vector<Node*>& preds, std::vector<Node*>& succs) {
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
                    if (curr == nullptr) break;
                    succ = curr->forward[level].load(std::memory_order_acquire);
                }

                if (curr != nullptr && curr->value < key) {
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }
            preds[level] = pred;
            succs[level] = curr;
        }
        return (curr != nullptr && curr->value == key);
    }

public:
    Skiplist() : currentMaxLevel(0) {
        // Using a sentinel key for the header
        header = new Node(MAX_LEVEL, std::numeric_limits<T>::min()); 
    }

    // NOTE: Destructor is not thread-safe and leaks erased nodes.
    ~Skiplist() {
        Node* curr = header->forward[0].load();
        while (curr != nullptr) {
            Node* next = curr->forward[0].load();
            delete curr;
            curr = next;
        }
        delete header;
    }

    bool search(const T& target) {
        std::vector<Node*> preds(MAX_LEVEL + 1);
        std::vector<Node*> succs(MAX_LEVEL + 1);
        bool found = find(target, preds, succs);
        return found && !succs[0]->marked.load(std::memory_order_acquire);
    }

    bool add(const T& key) {
        int topLevel = randomLevel();
        std::vector<Node*> preds(MAX_LEVEL + 1);
        std::vector<Node*> succs(MAX_LEVEL + 1);

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

            Node* newNode = new Node(topLevel, key);

            // Link bottom level first (linearization point)
            newNode->forward[0].store(succs[0], std::memory_order_relaxed);
            if (!preds[0]->forward[0].compare_exchange_strong(succs[0], newNode)) {
                delete newNode; // Failed, clean up and retry
                continue;
            }

            // Link the remaining levels
            for (int level = 1; level <= topLevel; ++level) {
                while (true) {
                    newNode->forward[level].store(succs[level], std::memory_order_relaxed);
                    if (preds[level]->forward[level].compare_exchange_strong(succs[level], newNode)) {
                        break; // Success, move to next level
                    }
                    // Failed CAS, pointers have changed. Re-find predecessors.
                    find(key, preds, succs);
                }
            }
            return true;
        }
    }

    bool erase(const T& key) {
        std::vector<Node*> preds(MAX_LEVEL + 1);
        std::vector<Node*> succs(MAX_LEVEL + 1);

        while (true) {
            bool found = find(key, preds, succs);
            if (!found) {
                return false; // Not in the list
            }

            Node* nodeToDelete = succs[0];

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
};

template<typename T>
thread_local std::mt19937 Skiplist<T>::gen(std::random_device{}());

} // namespace skiplist