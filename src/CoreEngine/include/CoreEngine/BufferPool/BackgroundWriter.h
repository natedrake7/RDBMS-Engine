#pragma once
#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <iostream>
#include <ostream>

namespace CoreEngine::Storage{
    class BackgroundWriter{
        std::mutex _mutex;
        std::condition_variable_any _wake;
        std::atomic<bool> _requested;
        std::jthread _thread;

    public:
        BackgroundWriter()
            : _requested(false){}
        BackgroundWriter(const BackgroundWriter&) = delete;
        BackgroundWriter& operator=(const BackgroundWriter&) = delete;

        BackgroundWriter(BackgroundWriter&&) = delete;
        BackgroundWriter& operator=(BackgroundWriter&&) = delete;

        ~BackgroundWriter(){ this->Stop(); }

        void Wake(){
            this->_requested.store(true, std::memory_order_release);
            this->_wake.notify_one();
        }

        void Stop(){
            if (this->_thread.joinable()){
                this->_thread.request_stop();
                this->_thread.join();
            }
        }

        template<typename TClean>
        void Start(TClean&& clean){
            const auto threadFunction = [this, clean](const std::stop_token& cancellationToken){
                while (!cancellationToken.stop_requested()){
                    {
                        std::unique_lock lock(this->_mutex);
                        this->_wake.wait_for(lock, cancellationToken, std::chrono::milliseconds(50),
                            [this]{ return this->_requested.load(std::memory_order_acquire); });

                        this->_requested.store(false, std::memory_order_relaxed);
                    }

                    if (cancellationToken.stop_requested())
                        break;

                    try{
                        clean();
                    }catch(std::exception& e){
                        std::cerr << "BackgroundWriter::Clean exception: " << e.what() << std::endl;
                    }
                }
            };

            this->_thread = std::jthread(threadFunction);
        }
    };
}
