#include "MapReduceFramework.h"
#include "Barrier.h"
#include <vector>
#include <iostream>
#include <pthread.h>
#include <atomic>
#include <algorithm>

#define ERR "system error: "
#define MEM_ERR "memory"
#define STD_ERR "stdlib"


struct ThreadContext{
    int threadID;
    Barrier* barrier;
    IntermediateVec vec;
};

class JobContext{
public:
    const MapReduceClient* client;
    const InputVec inputVec;
    OutputVec outputVec;
    int nThreads;

    pthread_t* threads;
    ThreadContext* contexts;
    std::atomic<uint64_t> atomic_counter;
    Barrier barrier;
    pthread_mutex_t wait_mutex;
};

JobContext* job;
bool finished = false;

void cleanup(JobHandle job){
    //free memory allocated.
    auto to_del = static_cast<JobContext*>(job);
    delete to_del->threads;
    delete to_del->contexts;
    if(!pthread_mutex_destroy(&to_del->wait_mutex)){
        delete to_del;
        std::cout << ERR << STD_ERR << "\n";
        exit(EXIT_FAILURE);
    }
    delete to_del;
}

void abort(JobHandle job, const std::string& err){
    cleanup(job);
    std::cout << ERR << err << "\n";
    exit(EXIT_FAILURE);
}

/**
 *
 * COME BACK TO BITWISE OPERATIONS
 *
 * */
void init_atomic_counter(){

}
uint64_t inc_atomic_counter(){
    auto old_value = (job->atomic_counter)++;
    return old_value;
}
stage_t getStage(uint64_t atomic_counter){

}
stage_t getPercent(uint64_t atomic_counter){

}

void emit2 (K2* key, V2* value, void* context){
    auto* tc = (ThreadContext*) context;
    tc->vec.emplace_back(std::make_pair(key, value));
}

void emit3 (K3* key, V3* value, void* context){}

void map(ThreadContext* tc){
    auto old_value = inc_atomic_counter();
    while(old_value < job->inputVec.size()){
        auto k = std::get<0>(job->inputVec[old_value]);
        auto v = std::get<1>(job->inputVec[old_value]);
        job->client->map(k, v, tc);
        old_value = inc_atomic_counter();
    }
}

void sort(ThreadContext* tc){
    if(!(tc->vec.empty()))
    {
        std::sort(tc->vec.begin(), tc->vec.end(),
                  [](IntermediatePair const &a, IntermediatePair const &b) {
            return a.first < b.first;
        });
    }
}

void shuffle(ThreadContext* tc){

}

void reduce(ThreadContext* tc){

}

void* run(void* t_cont){
    auto* tc = (ThreadContext*) t_cont;

    //map
    map(tc);

    //sort
    sort(tc);

    //1st barrier
    tc->barrier->barrier();

    //shuffle
    if(!tc->threadID){
        shuffle(tc);
    }

    //2nd barrier
    tc->barrier->barrier();

    //reduce
    reduce(tc);

    return nullptr;
};

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //init job
    job = new JobContext{&client, inputVec, outputVec, multiThreadLevel, {}, {}, {}, multiThreadLevel, PTHREAD_MUTEX_INITIALIZER};
    if(!job) abort(job, MEM_ERR);

    init_atomic_counter();

    //init thread contexts
    job->contexts = new ThreadContext[job->nThreads];
    if(!job->contexts) abort(job, MEM_ERR);
    for (int i = 0; i < job->nThreads; ++i) {
        job->contexts[i] = {i, &job->barrier, {}};
    }

    //init threads
    job->threads = new pthread_t[job->nThreads];
    if(!job->contexts) abort(job, MEM_ERR);
    for (int i = 0; i < job->nThreads; ++i) {
        pthread_create(job->threads + i, nullptr, run, job->contexts + i);
    }

    //return JobHandle
    return static_cast<JobHandle>(job);
}

void waitForJob(JobHandle job){
    auto handle = static_cast<JobContext*>(job);
    if (pthread_mutex_lock(&handle->wait_mutex)){
        abort(job, STD_ERR);
    }
    if(finished){
        return;
    }
    for (int i = 0; i < handle->nThreads; ++i) {
        pthread_join(handle->threads[i], nullptr);
    }
    finished = true;
    if (pthread_mutex_unlock(&handle->wait_mutex)){
        abort(job, STD_ERR);
    }
}

void getJobState(JobHandle job, JobState* state){
    auto handle = static_cast<JobContext*>(job);
    state->stage = getStage(handle->atomic_counter);
    state->percentage=getPercent(handle->atomic_counter);
}

void closeJobHandle(JobHandle job){
    waitForJob(job);
    cleanup(job);
}
