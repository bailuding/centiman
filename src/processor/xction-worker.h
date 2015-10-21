#ifndef __PROCESSOR_XCTION_WORKER_H__
#define __PROCESSOR_XCTION_WORKER_H__

#include <assert.h>
#include <pthread.h>
#include <string>

#include "util/counter.h"
#include <util/debug.h>
#include <util/generator.h>
#include <util/index.h>
#include <util/logger.h>
#include <util/queue-batcher.h>
#include <util/index-queue.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/stat.h>
#include <util/util.h>

class XctionWorker
{
    public:
        XctionWorker(Queue<Packet> * pInQueue, Queue<Packet> * pOutQueue);
        ~XctionWorker();
        void publishStat();
        void setTrace(char * );
        void run();
        static void * runHelper(void * worker);

    private:
        Packet ** mpXctions;
        Queue<Packet> * mpInQueue;
        Queue<Packet> * mpOutQueue;
        Stat mStat;
        Stat mTypeStat[6];
        
        char * mTraceFile;
        FILE * mFin;

        void getCommitReq(Packet * xction, Packet * req);
        void processCommitRsp(Packet * rsp, Packet * xction);
        void processReadRsp(Packet * rsp, Packet * xction);
        void evenSplit(Packet * xction);

        void resetStat();
        void initLoad();
        void load(Packet *);
        void finishLoad();

        int mCommitRspCnt;
        int mReadCnt;
        int mWriteCnt;
        
        // counter
        Counter counter;
        friend class Processor;
};


#endif // __PROCESSOR_XCTION_WORKER_H__
