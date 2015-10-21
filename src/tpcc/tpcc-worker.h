#ifndef __TPCC_TPCC_WORKER_H__
#define __TPCC_TPCC_WORKER_H__

#include <tpcc/DB.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/random.h>

class TpccWorker
{
    public:
        TpccWorker(uint32_t rand, int id, int tid, int wid, 
                Queue<Packet> * pInQueue, Queue<Packet> * pOutQueue);
        ~TpccWorker();
        void run();
        void publishStat();
        Stat & getStat();
        Stat * getStatDetail();
        static void * runHelper(void *);
    private:
        int mId;
        TPCC::DB mDb;

        Stat mStats[5];
};

#endif 
