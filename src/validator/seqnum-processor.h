#ifndef __VALIDATOR_SEQNUM_PROCESSOR_H__
#define __VALIDATOR_SEQNUM_PROCESSOR_H__

#include <atomic>
#include <map>
#include <algorithm>
#include <pthread.h>
#include <set>

#include <util/packet.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <validator/request-buf.h>
#include <util/logger.h>
#include <util/util.h>


class SeqnumProcessor
{
    public:
        SeqnumProcessor(int id,
                        Queue<Packet> * pInQueue, 
                        std::atomic<Packet *> * packetQueue, 
                        std::atomic<Seqnum> * pExpect);
        ~SeqnumProcessor();
        void resetStat();
        void publishProfile();
        void run();
        static void * runHelper(void *);
       
    private:
        int mId;
        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;
        std::atomic<Packet *> * mPacketQueue;
        std::atomic<Seqnum> * mpExpect;
        
        RequestBuf mBuf;
        
        QueueBatcher<Packet> * mpInBatcher;
        Seqnum * mPacketSeqnum;

        int mIdx;
        Seqnum mLocalExpect;
        Seqnum mPrev;
        
        int mPacketCnt;
        int mForwardCnt;
        int mUpdateCnt;

        void forward();
        void inc(int &idx);
    friend class Validator;
};

#endif // __VALIDATOR_SEQNUM_PROCESSOR_H__
