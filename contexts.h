#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>
#include <unordered_map>
#include <iostream>

#define ERR "system error: "
#define MEM_ERR "memory"
#define STD_ERR "stdlib"

class ThreadContext;


class JobContext{
public:
    const MapReduceClient* client;
    const InputVec inputVec;
    OutputVec* outputVec;
    std::vector<IntermediateVec> shuffled;
    int nThreads;
    stage_t curStage= MAP_STAGE;
    bool finished = false;
    std::vector<pthread_t> threads;
    std::vector<ThreadContext*> contexts;
    std::unordered_map<stage_t, std::pair<std::atomic<uint64_t>, std::atomic<uint64_t>>> counters;
    Barrier barrier;
    pthread_mutex_t wait_mutex;
    pthread_mutex_t inc_mutex;
    pthread_mutex_t output_mutex;

    unsigned long numOfElementsInShuffle{};

    JobContext(const MapReduceClient* client, InputVec inputVec, OutputVec* outputVec, int nThreads):
    client(client), inputVec(std::move(inputVec)), outputVec(outputVec), wait_mutex(PTHREAD_MUTEX_INITIALIZER),
    inc_mutex(PTHREAD_MUTEX_INITIALIZER), output_mutex(PTHREAD_MUTEX_INITIALIZER), nThreads(nThreads), barrier(nThreads) {}

    ~JobContext();
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
