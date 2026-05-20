#include "../../../include/Guards/Mutex.h"

namespace MultiThreading {

bool Mutex::TrySharedFast() {
    auto s = this->state.load(std::memory_order_acquire);

    while (s >= 0) {
        // Optional writer preference:
        if (this->waitingWriters.load(std::memory_order_relaxed) > 0)
            return false;

        if (this->upgradePending.load(std::memory_order_relaxed))
            return false;

        if (this->state.compare_exchange_weak(
            s,
            s + 1,
            std::memory_order_acquire,
            std::memory_order_relaxed
        )) return true;
    }

    return false;
}

bool Mutex::TryUniqueFast() {
    auto expected = 0;
    return this->state.compare_exchange_strong(
        expected,
        -1,
        std::memory_order_acquire,
        std::memory_order_relaxed
    );
}

bool Mutex::TryPromoteLock() {
    auto expected = false;
    // only one upgrader allowed
    if (!this->upgradePending.compare_exchange_strong(
        expected,
        true,
        std::memory_order_acquire,
        std::memory_order_relaxed))
        return false;

    int expectedState = 1;

    bool success =
        this->state.compare_exchange_strong(
            expectedState,
            -1,
            std::memory_order_acquire,
            std::memory_order_relaxed);

    this->upgradePending.store(false, std::memory_order_release);

    return success;
}

Mutex::Mutex()
    : state(0)
    , waitingReaders(0)
    , waitingWriters(0)
    , upgradePending(false){}

void Mutex::SharedLock() {
    // FAST PATH
    if (TrySharedFast())
        return;

    // ADAPTIVE SPIN
    for (int i = 0; i < SPIN_COUNT; ++i) {
        if (TrySharedFast())
            return;

        cpu_pause();
    }

    // SLOW PATH (park thread)
    this->waitingReaders.fetch_add(1, std::memory_order_relaxed);

    std::unique_lock lock(waitMutex);

    this->readersCV.wait(lock, [this]() {
        if (this->waitingWriters.load(std::memory_order_relaxed) > 0)
            return false;

        const auto s = this->state.load(std::memory_order_acquire);

        return s >= 0;
    });

    this->waitingReaders.fetch_sub(1, std::memory_order_relaxed);

    // acquire reader slot
    while (true) {
        auto s = this->state.load(std::memory_order_acquire);

        if (s < 0)
            continue;

        if (this->state.compare_exchange_weak(
            s,
            s + 1,
            std::memory_order_acquire,
            std::memory_order_relaxed
        )) return;
    }
}

void Mutex::SharedUnlock() {

    const auto prev = this->state.fetch_sub(
        1,
        std::memory_order_release
    );

    // last reader wakes writer
    if (prev == 1) {
        std::lock_guard lock(waitMutex);

        if (this->waitingWriters.load(std::memory_order_relaxed) > 0)
            this->writersCV.notify_one();
    }
}

void Mutex::UniqueLock() {
    // FAST PATH
    if (TryUniqueFast())
        return;

    // ADAPTIVE SPIN
    for (int i = 0; i < SPIN_COUNT; ++i) {
        if (TryUniqueFast())
            return;

        cpu_pause();
    }

    // SLOW PATH
    this->waitingWriters.fetch_add(1, std::memory_order_relaxed);

    std::unique_lock lock(waitMutex);

    this->writersCV.wait(lock, [this]() {
        return this->state.load(std::memory_order_acquire) == 0;
    });

    this->waitingWriters.fetch_sub(1, std::memory_order_relaxed);

    int expected = 0;

    while (!this->state.compare_exchange_weak(
        expected,
        -1,
        std::memory_order_acquire,
        std::memory_order_relaxed))
    {
        expected = 0;
    }
}

void Mutex::UniqueUnlock() {
    this->state.store(0, std::memory_order_release);

    std::lock_guard lock(waitMutex);

    // Prefer writers first
    if (this->waitingWriters.load(std::memory_order_relaxed) > 0) {
        this->writersCV.notify_one();
        return;
    }

    // Otherwise wake all readers
    this->readersCV.notify_all();
}

bool Mutex::SharedTryLock() {
    return TrySharedFast();
}

    bool Mutex::UniqueTryLock() {
        return TryUniqueFast();
    }

void Mutex::PromoteLock() {
    bool expected = false;

    // wait until upgrade slot available
    while (!this->upgradePending.compare_exchange_weak(
        expected,
        true,
        std::memory_order_acquire,
        std::memory_order_relaxed))
    {
        expected = false;
        cpu_pause();
    }

    this->waitingWriters.fetch_add(1, std::memory_order_relaxed);

    // wait until we're sole reader
    while (true) {
        auto expectedState = 1;
        if (this->state.compare_exchange_weak(
            expectedState,
            -1,
            std::memory_order_acquire,
            std::memory_order_relaxed))
            break;

        cpu_pause();
    }

    this->waitingWriters.fetch_sub(1, std::memory_order_relaxed);

    this->upgradePending.store(false, std::memory_order_release);
}

}