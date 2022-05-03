#include "MapReduceFramework.h"
#include "Barrier.h"
#include <utility>
#include <vector>
#include <iostream>
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <bitset>
#include <unistd.h>
#include <unordered_map>


#define ERR "system error: "
#define MEM_ERR "memory"
#define STD_ERR "stdlib"

/***********************************   Classes   ********************************************/


class ThreadContext;

class JobContext{
public:
    const MapReduceClient* client;
    const InputVec inputVec;
    OutputVec* outputVec;
    std::vector<IntermediateVec> shuffled;
    int nThreads;
    stage_t curStage= UNDEFINED_STAGE;
    bool finished = false;
    std::vector<pthread_t> threads;
    std::vector<ThreadContext*> contexts;
    std::unordered_map<stage_t, std::pair<std::atomic<uint64_t>, std::atomic<uint64_t>>> counters;
    Barrier barrier;
    pthread_mutex_t wait_mutex;
    pthread_mutex_t inc_mutex;
    unsigned long numOfElementsInShuffle{};

    JobContext(const MapReduceClient* client, InputVec inputVec, OutputVec* outputVec, int nThreads):
                client(client), inputVec(std::move(inputVec)), outputVec(outputVec), wait_mutex(PTHREAD_MUTEX_INITIALIZER),
                inc_mutex(PTHREAD_MUTEX_INITIALIZER), nThreads(nThreads), barrier(nThreads) {}

    ~JobContext(){
        if(pthread_mutex_destroy(&wait_mutex)){
            std::cout << ERR << STD_ERR << "\n";
            exit(EXIT_FAILURE);
        }
        if(pthread_mutex_destroy(&inc_mutex)){
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

/***********************************   Functions   ********************************************/

void calcNumElemInShuffle();

void cleanup(JobHandle job){
    delete static_cast<JobContext*>(job);
}

void abort(JobHandle job, const std::string& err){
    cleanup(job);
    std::cout << ERR << err << "\n";
    exit(EXIT_FAILURE);
}

uint64_t getBits(uint64_t n, uint64_t k, uint64_t p){
    uint64_t a = (((1 << k) - 1) & (n >> (p - 1)));
    return (((1 << k) - 1) & (n >> (p - 1)));
}

uint64_t setMiddleBits(uint64_t n, uint64_t m){
    return (n & 0xC00000007FFFFFFF) | (m << 31);
}

uint64_t setRightBits(uint64_t n, uint64_t m){
    return (n & 0xFFFFFFFF80000000) | m;
}

uint64_t incStartCounter(ThreadContext* tc, stage_t stage){
    uint64_t a = tc->job->counters[stage].first.fetch_add(1);
    return a;
//    return job->counters[stage].first.fetch_add(1);
}

uint64_t incFinishCounter(ThreadContext* tc, stage_t stage){
    return tc->job->counters[stage].second.fetch_add(1);

}

int getStage(JobHandle job){
    return static_cast<int>(static_cast<JobContext*>(job)->curStage);
}

float getPercent(JobContext* job){
    uint64_t num = job->counters[job->curStage].second.load();
    float percent;
    switch (getStage(job)) {
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
    auto tc = static_cast<ThreadContext*>(context);
    tc->vec.emplace_back(std::make_pair(key, value));
    incFinishCounter(tc, MAP_STAGE);
}

void emit3 (K3* key, V3* value, void* context){
    auto tc = static_cast<ThreadContext*>(context);
    tc->job->outputVec->emplace_back(key, value);
    incFinishCounter(tc, REDUCE_STAGE);
}

void map(ThreadContext* tc){
    auto old_value = incStartCounter(tc, MAP_STAGE);
    while(old_value < tc->job->inputVec.size()){
        auto k = std::get<0>(tc->job->inputVec[old_value]);
        auto v = std::get<1>(tc->job->inputVec[old_value]);
        tc->job->client->map(k, v, tc);
        old_value = incStartCounter(tc, MAP_STAGE);
    }
}

void sort(ThreadContext* tc){
    if(!(tc->vec.empty()))
    {
        std::sort(tc->vec.begin(), tc->vec.end(),
                  [](IntermediatePair a, IntermediatePair b) {
            return (*(a.first)) < (*(b.first));
        });
    }
}

int getMaxIndex(ThreadContext* tc){
    int max_index = -1;
    for(int i=0;i<tc->job->nThreads;++i){
        if(!tc->job->contexts[i]->vec.empty()){
            if((max_index == -1) || (tc->job->contexts[max_index]->vec[0].first < tc->job->contexts[i]->vec[0].first)){
                max_index=i;
            }
        }
    }
    return max_index;
}

bool equals(K2* x, K2* y){
    return (!((*x)<(*y)) && !((*y)<(*x)));
}

void shuffle(ThreadContext* tc){
    int max_ind = getMaxIndex(tc);
    while(max_ind != -1){
        //if k2 is not in vec, add
        if(tc->job->shuffled.empty() || !equals(tc->job->contexts[max_ind]->vec[0].first, tc->job->shuffled.back()[0].first)){
            auto v = IntermediateVec();
            v.emplace_back(tc->job->contexts[max_ind]->vec[0]);
            tc->job->shuffled.emplace_back(v);
        }

        //otherwise, add to it
        else{
            tc->job->shuffled.back().emplace_back(tc->job->contexts[max_ind]->vec[0]);
        }

        //erase elem from job
        tc->job->contexts[max_ind]->vec.erase(tc->job->contexts[max_ind]->vec.begin());

        //increment counter
        incFinishCounter(tc, SHUFFLE_STAGE);

        //get next index
        max_ind = getMaxIndex(tc);
//        usleep(1000000);
    }

}

void reduce(ThreadContext* tc){
    auto old_value = incStartCounter(tc, REDUCE_STAGE);
    while(old_value < tc->job->shuffled.size()){
        auto vec = tc->job->shuffled[old_value];
        tc->job->client->reduce(&vec, tc);
        old_value = incStartCounter(tc, REDUCE_STAGE);
    }
}

void calcNumElemInShuffle(ThreadContext* tc) {
    tc->job->numOfElementsInShuffle = 0;
    for (int i = 0; i < tc->job->contexts.size(); i++) {
        tc->job -> numOfElementsInShuffle += tc->job->contexts[i]->vec.size();
    }
}

void* run(void* thread_context){
    auto* tc = (ThreadContext*) thread_context;

    //map
    map(tc);

    //sort
    sort(tc);

    //1st barrier
    tc->barrier->barrier();

    //shuffle
    if(!tc->threadID){
        tc->job->curStage = SHUFFLE_STAGE;
        calcNumElemInShuffle(tc);
        shuffle(tc);
        tc->job->curStage = REDUCE_STAGE;
    }

    //2nd barrier
    tc->barrier->barrier();

    //reduce
    reduce(tc);

    return nullptr;
}

void initCounters(JobContext* job) {
    job->counters[MAP_STAGE] = std::make_pair(0,0);
    job->counters[SHUFFLE_STAGE] = std::make_pair(0,0);
    job->counters[REDUCE_STAGE] = std::make_pair(0,0);
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    //init job
    auto job = new JobContext(&client, inputVec, &outputVec, multiThreadLevel);
    if(!job) abort(job, MEM_ERR);

    initCounters(job);

    //init threads
    job->threads.reserve(job->nThreads * sizeof(pthread_t));
    for (int i = 0; i < job->nThreads; ++i) {
        auto tc = new ThreadContext(i, &job->barrier, {}, job);
        if(!tc) abort(job, MEM_ERR);
        job->contexts.emplace_back(tc);
        pthread_create(job->threads.data() + i, nullptr, run, tc);
    }

    //return JobHandle
    return static_cast<JobHandle>(job);
}

void waitForJob(JobHandle job){
    auto handle = static_cast<JobContext*>(job);
    if (pthread_mutex_lock(&handle->wait_mutex)){
        abort(job, STD_ERR);
    }
    if(handle->finished){
        return;
    }
    for (int i = 0; i < handle->nThreads; ++i) {
        pthread_join(handle->threads[i], nullptr);
    }
    handle->finished = true;
    if (pthread_mutex_unlock(&handle->wait_mutex)){
        abort(job, STD_ERR);
    }
}

void getJobState(JobHandle job, JobState* state){
    state->stage = static_cast<stage_t>(getStage(job));
    state->percentage=getPercent(static_cast<JobContext*>(job));
}

void closeJobHandle(JobHandle job){
    waitForJob(job);
    cleanup(job);
}
