#ifndef __STORAGE_STORAGE_WORKER_H__
#define __STORAGE_STORAGE_WORKER_H__

#include <map>
#include <algorithm>
#include <sys/stat.h> 
#include <fcntl.h>
#include <unistd.h>

#include <pthread.h>

#include <storage/storage-hash-table.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <util/util.h>
#include <tpcc/DB.h>
#include <util/index.h>
#include <util/logger.h>
#include <util/util.h>
#include "util/counter.h"
#include "util/config.h"

class StorageWorker
{
    public:
        StorageWorker(int id, 
                    Config * config,
                    Queue<Packet> * pInQueue, std::map<int, Queue<Packet> *> * pOutQueues);
        ~StorageWorker();
        void resetStat();
        void populate();
        void populateTpcc();
        void populateTpccTrace();
        void publishProfile();
        void setTrace(char * file);
        void run();
        static void * runHelper(void *);
    private:
        int mId;
        Config * config;

        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;
        StorageHashTable mHashTable;
        
        QueueBatcher<Packet> * mpInBatcher;
        QueueBatcher<Packet> ** mpOutBatchers;
       
        int * mSfds;
        Seqnum * mWaterMarks;

        char * mTraceFile;

        uint32_t mNullRead;

        Seqnum getReadWaterMark();
        Counter counter;

        friend class Storage;
};

#endif // __STORAGE_STORAGE_WORKER_H__
