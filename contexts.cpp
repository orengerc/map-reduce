#include "contexts.h"

JobContext::~JobContext() {
    if(pthread_mutex_destroy(&wait_mutex)){
        std::cout << ERR << STD_ERR << "\n";
        exit(EXIT_FAILURE);
    }
    if(pthread_mutex_destroy(&inc_mutex)){
        std::cout << ERR << STD_ERR << "\n";
        exit(EXIT_FAILURE);
    }
}