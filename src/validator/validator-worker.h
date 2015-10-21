#ifndef __VALIDATOR_VALIDATOR_WORKER_H__
#define __VALIDATOR_VALIDATOR_WORKER_H__

#include <atomic>
#include <map>
#include <algorithm>
#include <assert.h>
#include <pthread.h>
#include <unistd.h>

#include <util/debug.h>
#include <util/packet.h>
#include <util/index.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <util/stat.h>
#include <validator/hash-table.h>
#include <util/const.h>
#include <util/util.h>
#include "util/counter.h"
#include "util/config.h"

class ValidatorWorker
{
    public:
        ValidatorWorker(int id,
                        Config * config,
                        Queue<Packet> * pInQueue, std::map<int, Queue<Packet> *> * pOutQueues,
                        std::atomic<Packet *> * packetQueue,
                        std::atomic<Seqnum> * pSeqnumExpect);
        ~ValidatorWorker();
        void publishProfile();
        void run();
        static void * runHelper(void *);
        
        int validate(Packet *);
        int validateSi(Packet *);
        void remove(ArrayKey *);
        void update(Packet *);

        void publishStat();

        Counter counter;

    private:
        int mId;
        Config * config;
        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;
        std::atomic<Packet *> * mPacketQueue;
        std::atomic<Seqnum> * mpSeqnumExpect;

        HashTable mHashTable;
        
        Seqnum mLocalExpect;
        Seqnum mWaterMark;
        Seqnum mDelSeqnum;
        
        int mCnt;
        int mOps;
        int mRec;
        uint64_t mCheckCnt;
        uint64_t mPacketCnt;
        uint64_t mUpdateCnt;
        int mSleepCnt;
        int mUniqueCnt;
        int mGetCnt;
        int mPutCnt;
        int mDelCnt;
        
        int mGetCnts[1024];
        int mPutCnts[1024];
        int mDelCnts[1024];

        int mSearchGet;
        int mSearchPut;
        int mSearchDel;

        Stat mStat;
        int mRead;
        int mWrite;

        int validate(Packet *, int);
        int validateSiR(Packet *, int);
        int validateSiW(Packet *, int);
        void remove(ArrayKey *, int);
        void update(Packet *, int);
        void inc(int &idx);

        void resetStat();
};

#endif // __VALIDATOR_VALIDATOR_WORKER_H__

