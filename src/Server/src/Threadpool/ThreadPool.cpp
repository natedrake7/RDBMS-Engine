#include "../../include/ThreadPool.h"
#include "../../../Systemic/include/DataTypes/DataTypes.h"

ThreadPool::ThreadPool() = default;

ThreadPool::~ThreadPool(){
  this->condition.notify_all();

  for (auto &worker : this->workers)
    worker.join();
}

void ThreadPool::InitializeWorkers(const std::atomic<bool> &isServerRunning, const Int numberOfThreads){
  for (int i  = 0; i < numberOfThreads; i++) {
    this->workers.emplace_back([this, &isServerRunning] {

      while (true) {

        std::function<void()> task;

        {
          std::unique_lock<std::mutex> lock(this->queueMutex);

          this->condition.wait(lock, [this, &isServerRunning]() {
            return !this->tasks.empty() || !isServerRunning.load();
          });

          if (!isServerRunning.load())
            return;

          task = std::move(this->tasks.front());
          this->tasks.pop();
        }

        task();
      }
    });
  }
}