#ifndef __STORAGE_STORAGE_H__
#define __STORAGE_STORAGE_H__

#include <map>
#include <pthread.h>
#include <unistd.h>
#include <pthread.h>

#include <network/server.h>
#include <storage/storage-hash-table.h>
#include <storage/storage-worker.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/code-handler.h>
#include <util/const.h>
#include <util/logger.h>
#include <util/util.h>
#include "util/config.h"

class Storage
{
    public:
        Storage(const char * port, int id, char * trace, Config * config);
        ~Storage();
        int cancel();
        void publish();
        void publishProfile();
        void publishCounter();
        void run();
    private:
        int mId;
        char * mPort;
       
        Config * config;

        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;

        Server * mpServer;
        StorageWorker * mpWorker;

        pthread_t mServerThread;
        pthread_t mWorkerThread;
};

#endif // __STORAGE_STORAGE_H__
