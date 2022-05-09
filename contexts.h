#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>
#include <unordered_map>
#include <iostream>

#define ERR "system error: "
#define MEM_ERR "memory"
#define STD_ERR "standard library"
#define THREAD_ERR "pthread library"

class ThreadContext;


class JobContext{
public:
    const MapReduceClient* client;
    const InputVec inputVec;
    OutputVec* outputVec;
    std::vector<IntermediateVec> shuffled;
    int nThreads;
    std::atomic<stage_t> curStage;
    bool finished = false;
    pthread_t* threads;
    std::vector<ThreadContext*> contexts;
    std::unordered_map<stage_t, std::pair<std::atomic<uint64_t>, std::atomic<uint64_t>>> counters;
    Barrier barrier;
    pthread_mutex_t wait_mutex;
    pthread_mutex_t output_mutex;
    pthread_mutex_t inc_mutex;
    pthread_mutex_t state_mutex;

    unsigned long numOfElementsInShuffle{};

    JobContext(const MapReduceClient* client, InputVec inputVec, OutputVec* outputVec, int nThreads):
            client(client), inputVec(std::move(inputVec)), outputVec(outputVec), nThreads(nThreads), curStage(MAP_STAGE),
            counters{0}, barrier(nThreads), wait_mutex(PTHREAD_MUTEX_INITIALIZER), output_mutex(PTHREAD_MUTEX_INITIALIZER), inc_mutex(PTHREAD_MUTEX_INITIALIZER), state_mutex(PTHREAD_MUTEX_INITIALIZER) {
        threads = new pthread_t[nThreads];
    }

    ~JobContext(){

        delete[] threads;

        if(pthread_mutex_destroy(&wait_mutex)){
            std::cout << ERR << STD_ERR << "\n";
            exit(EXIT_FAILURE);
        }
        if(pthread_mutex_destroy(&inc_mutex)){
            std::cout << ERR << STD_ERR << "\n";
            exit(EXIT_FAILURE);
        }
        if(pthread_mutex_destroy(&state_mutex)){
            std::cout << ERR << STD_ERR << "\n";
            exit(EXIT_FAILURE);
        }
    }
};

class ThreadContext{
public:
    int threadID;
    Barrier* barrier;
    IntermediateVec vec;
    JobContext* job;

    ThreadContext(int id, Barrier* bar, IntermediateVec v, JobContext* j):
    threadID(id), barrier(bar), vec(std::move(v)), job(j){}
};
