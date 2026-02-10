#pragma once
#include <queue>
#include <vector>
#include <functional>

template<typename Value, typename Compare = std::greater<Value>>
class PriorityQueue : std::priority_queue<Value, std::vector<Value>, Compare>{
public:
    PriorityQueue(): std::priority_queue<Value, std::vector<Value>, Compare>(){}

    explicit PriorityQueue(Compare& comp) : std::priority_queue<Value, std::vector<Value>, Compare>(comp) {}

    void Add(const Value& value) {
        this->push(value);
    }

    void Add(Value&& value) {
        this->push(std::move(value));
    }

    void Remove() {
        this->pop();
    }

    const Value& Top() const {
        return this->top();
    }

    bool Empty() const {
        return this->empty();
    }

    size_t Size() const {
        return this->size();
    }

    void Clear() {
        while (!this->empty()) {
            this->pop();
        }
    }
};