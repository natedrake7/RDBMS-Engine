#include "ThreadPool.h"

ThreadPool::ThreadPool() = default;

ThreadPool::~ThreadPool(){
  condition.notify_all();

  for (std::thread &worker : workers)
    worker.join();
}

void ThreadPool::InitializeWorkers(const std::atomic<bool> &isServerRunning, const int &numberOfThreads){
  for (int i  = 0; i < numberOfThreads; i++) {
    this->workers.emplace_back([this, &isServerRunning] {

      while (true) {
        std::unique_lock<std::mutex> lock(this->queueMutex);

        this->condition.wait(lock, [this, &isServerRunning]() {
          return !this->tasks.empty() || !isServerRunning.load();
        });

        if (!isServerRunning.load())
          return;

        const std::function<void()> task = std::move(this->tasks.front());
        this->tasks.pop();

        lock.unlock();

        task();
      }
    });
  }
}