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
    std::vector<IntermediateVec> shuffled;
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

uint64_t getBits(uint64_t n, uint64_t k, uint64_t p){
    return (((1 << k) - 1) & (n >> (p - 1)));
}

uint64_t setMiddleBits(uint64_t n, uint64_t m){
    return (n & 0xC00000007FFFFFFF) | (m << 31);
}

uint64_t setRightBits(uint64_t n, uint64_t m){
    return (n & 0xFFFFFFFF80000000) | m;
}

uint64_t incStartCounter(){
    auto old_value = job->atomic_counter.load();

    //add to middle part
    job->atomic_counter = setMiddleBits(old_value, getBits(old_value, 31, 31) + 1);

    return old_value;
}

uint64_t incFinishCounter(){
    auto old_value = job->atomic_counter.load();

    //add to right part
    job->atomic_counter = setRightBits(old_value, getBits(old_value, 31, 0) + 1);

    return old_value;
}

void resetAtomicCounter(stage_t stage){
    job->atomic_counter = static_cast<uint64_t>(stage) << 62;
}

stage_t getStage(){
    return static_cast<stage_t>((job->atomic_counter.load() & 0xC000000000000000) >> 62);
}

float getPercent(){
    uint64_t num = getBits(job->atomic_counter.load(), 31, 31);
    float percent;
    switch (getStage()) {
        case MAP_STAGE:
            percent = (float)num/(float)job->inputVec.size();
            break;
        case SHUFFLE_STAGE:
            percent = (float)num/(float)job->nThreads;
            break;
        case REDUCE_STAGE:
            percent = (float)num/(float)job->shuffled.size();
            break;
        case UNDEFINED_STAGE:
            percent = 0.f;
            break;
    }
    return percent * 100;
}

void emit2 (K2* key, V2* value, void* context){
    auto* tc = (ThreadContext*) context;
    tc->vec.emplace_back(std::make_pair(key, value));
    incFinishCounter();
}

void emit3 (K3* key, V3* value, void* context){
    job->outputVec.emplace_back(key, value);
    incFinishCounter();
}

void map(ThreadContext* tc){
    auto old_value = incStartCounter();
    while(old_value < job->inputVec.size()){
        auto k = std::get<0>(job->inputVec[old_value]);
        auto v = std::get<1>(job->inputVec[old_value]);
        job->client->map(k, v, tc);
        old_value = incStartCounter();
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

int getMaxIndex(){
    int max_index = -1;
    for(int i=0;i<job->nThreads;++i){
        if(!job->contexts[i].vec.empty()){
            if((max_index == -1) || (job->contexts[max_index].vec[0].first < job->contexts[i].vec[0].first)){
                max_index=i;
            }
        }
    }
    return max_index;
}

bool equals(K2* x, K2* y){
    return (!((*x)<(*y)) && !((*y)<(*x)));
}

void shuffle(){
    int max_ind = getMaxIndex();
    while(max_ind != -1){
        //if k2 is not in vec, add
        if(job->shuffled.empty() || !equals(job->contexts[max_ind].vec[0].first, job->shuffled.back()[0].first)){
            auto v = IntermediateVec();
            v.emplace_back(job->contexts[max_ind].vec[0]);
            job->shuffled.emplace_back(v);
        }

        //otherwise, add to it
        else{
            job->shuffled.back().emplace_back(job->contexts[max_ind].vec[0]);
        }

        //erase elem from job
        job->contexts[max_ind].vec.erase(job->contexts[max_ind].vec.begin());

        //increment counter
        incFinishCounter();

        //get next index
        max_ind = getMaxIndex();
    }

}

void reduce(ThreadContext* tc){
    auto old_value = incStartCounter();
    while(old_value < job->shuffled.size()){
        auto vec = job->shuffled[old_value];
        job->client->reduce(&vec, tc);
        old_value = incStartCounter();
    }
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
        resetAtomicCounter(SHUFFLE_STAGE);
        shuffle();
        resetAtomicCounter(REDUCE_STAGE);
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
    job = new JobContext{&client, inputVec, outputVec, {}, multiThreadLevel, {}, {}, {}, multiThreadLevel, PTHREAD_MUTEX_INITIALIZER};
    if(!job) abort(job, MEM_ERR);

    resetAtomicCounter(MAP_STAGE);

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
    state->stage = getStage();
    state->percentage=getPercent();
}

void closeJobHandle(JobHandle job){
    waitForJob(job);
    cleanup(job);
}
