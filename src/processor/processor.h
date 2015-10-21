#ifndef __PROCESSOR_PROCESSOR_H__
#define __PROCESSOR_PROCESSOR_H__

#include <atomic>
#include <map>
#include <pthread.h>
#include <string>

#include <network/client.h>
#include <processor/processor-worker.h>
#include <processor/xction-worker.h>
#include <tpcc/tpcc-worker.h>
#include <util/index-queue.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/random.h>
#include <util/code-handler.h>
#include <util/const.h>
#include <util/logger.h>
#include <util/stat.h>
#include <util/util.h>
#include "util/config.h"
#include "util/counter.h"

class Client;

class Processor
{
    public:
        Processor(int id, Config * config, char * trace);
        ~Processor();
        int cancel();
        void publishStat();
        void publishProfile();
        void publishCounter();
        void run();
    private:

        Config * config;

        int mId;
        Random mRand;

        Queue<Packet> * mpInQueue;
        Queue<Packet> * mpXctionOutQueue;
        Queue<Packet> ** mpOutQueues;
        Queue<Packet> ** mpXctionOutQueues;

        Client * mpClient;
        ProcessorWorker * mpWorker;
        XctionWorker * mpXctionWorker;
        TpccWorker ** mpTpccWorkers;

        std::map<int, int> * mpSfds;

        pthread_t mWorkerThread;
        pthread_t mXctionThread;
        pthread_t mClientThread;
        pthread_t * mTpccWorkerThreads;
};

#endif // __PROCESSOR_PROCESSOR_H__
