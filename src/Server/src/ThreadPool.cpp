#include <iostream>
#include <Server/ThreadPool.h>
#include <Systemic/DataTypes/DataTypes.h>

#include "Systemic/Functions/MathFunctions.h"

Int ThreadPool::DefaultWorkerCount(){
    const auto logicalCores = static_cast<Int>(std::thread::hardware_concurrency());
    return Math::Max(2, logicalCores == 0 ? 4 : logicalCores);
}

void ThreadPool::WorkerLoop(){
    while (true) {
        Task task;

        {
            std::unique_lock lock(this->_queueMutex);

            this->_condition.wait(lock, [this]() {
                return this->_isStopping
                    || !this->_tasks.empty();
            });

            if (this->_isStopping)
                return;

            task = std::move(this->_tasks.front());
            this->_tasks.pop();
        }

        try{
            task();
        }
        catch (const std::exception& ex){
            std::cerr << "ThreadPool: task failed: " << ex.what() << std::endl;
        }
        catch (...){
            std::cerr << "ThreadPool: task failed with an unknown exception" << std::endl;
        }
    }
}

ThreadPool::ThreadPool()
    : _isStopping(false){}

ThreadPool::~ThreadPool(){
    this->Stop();
}

void ThreadPool::Start(Int numberOfThreads){
    numberOfThreads = Math::Max(numberOfThreads, DefaultWorkerCount());
    for (int i  = 0; i < numberOfThreads; i++)
        this->workers.emplace_back(&ThreadPool::WorkerLoop, this);
}

void ThreadPool::Stop(){
    {
        std::unique_lock lock(this->_queueMutex);
        this->_isStopping = true;
    }

    this->_condition.notify_all();
    for (auto &worker : this->workers){
        if (worker.joinable())
            worker.join();
    }

    this->workers.clear();
}
