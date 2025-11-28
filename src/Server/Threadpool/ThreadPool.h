#pragma once
#include <atomic>
#include <condition_variable>
#include <vector>
#include <queue>
#include <functional>

class ThreadPool {
  std::mutex queueMutex;
  std::condition_variable condition;
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;

  public:
    ThreadPool();
    ~ThreadPool();
    void InitializeWorkers(const std::atomic<bool> &isServerRunning, const int& numberOfThreads);
    template<class F>
    void Enqueue(F&& task);
};

template <class F>
void ThreadPool::Enqueue(F &&task)
{
  {
    std::unique_lock<std::mutex> lock(this->queueMutex);

    this->tasks.emplace(std::forward<F>(task));
  }

  this->condition.notify_one();
}


