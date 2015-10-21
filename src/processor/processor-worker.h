#ifndef __PROCESSOR_PROCESSOR_WORKER_H__
#define __PROCESSOR_PROCESSOR_WORKER_H__

#include <atomic>
#include <map>
#include <queue>
#include <utility>
#include <algorithm>
#include <atomic>
#include <pthread.h>

#include <processor/water-mark-list.h>
#include <util/packet.h>
#include <util/queue-batcher.h>
#include <util/stat.h>
#include <util/index.h>
#include <util/logger.h>
#include "util/config.h"
#include "util/counter.h"

class ProcessorWorker
{
    public:
        ProcessorWorker(int id,
                Config * config,
                Queue<Packet> * pInQueue, Queue<Packet> * pXctionOutQueue,
                Queue<Packet> ** pXctionOutQueues,
                Queue<Packet> ** pServerOutQueues, std::map<int, int> * pServerSfdMap);
        ~ProcessorWorker();
        void resetStat();
        void publishStat();
        void run();
        static void * runHelper(void * worker);
        Counter counter;
    private:
        int mId;
        Config * config;
        Queue<Packet> * mpInQueue;
        Queue<Packet> * mpXctionOutQueue;
        Queue<Packet> ** mpXctionOutQueues;
        Queue<Packet> ** mpServerOutQueues;
        std::map<int, int> * mpServerSfdMap;

        Seqnum mSeqnum;
        WaterMarkList mWaterMark;
        QueueBatcher<Packet> mXctionOutBatcher;

        Packet * mXctions;
        QueueBatcher<Packet> ** mpStorageOutBatchers;
        QueueBatcher<Packet> ** mpValidatorOutBatchers;

        int * mpServerSfds;

        Stat mReadStat;
        Stat mWriteStat;
        Stat mValidStat;
        Stat mReadFirstStat;
        Stat mWriteFirstStat;
        Stat mValidFirstStat;

        void processReadReq(Packet *);
        int processReadRsp(Packet *);
        void processCommitReq(Packet *);
        int processValidRsp(Packet *);
        int processWriteRsp(Packet *);

        Seqnum getSeqnum();
        Seqnum readSeqnum();
};


#endif // __PROCESSOR_PROCESSOR_WORKER_H__

