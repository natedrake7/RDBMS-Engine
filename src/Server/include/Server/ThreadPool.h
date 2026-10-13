#pragma once
#include <condition_variable>
#include <vector>
#include <queue>
#include <functional>
#include <thread>
#include <Systemic/DataTypes/DataTypes.h>

using Task = std::function<void()>;

class ThreadPool {
    std::mutex _queueMutex;
    std::condition_variable _condition;
    std::vector<std::thread> workers;
    std::queue<Task> _tasks{};

    bool _isStopping;

    [[nodiscard]] static Int DefaultWorkerCount();

    void WorkerLoop();

public:
    inline static constexpr Int DEFAULT_WORKER_COUNT = -1;

    ThreadPool();
    ~ThreadPool();

    void Start(Int numberOfThreads = DEFAULT_WORKER_COUNT);
    void Stop();

    template<class Function>
    void Enqueue(Function&& task);
};

template <class Function>
void ThreadPool::Enqueue(Function &&task){
  {
    std::unique_lock lock(this->_queueMutex);
    this->_tasks.emplace(std::forward<Function>(task));
  }

  this->_condition.notify_one();
}


