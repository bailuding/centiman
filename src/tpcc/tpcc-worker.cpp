#include "tpcc/tpcc-worker.h"

TpccWorker::TpccWorker(uint32_t rand, int id, int tid, int wid, Queue<Packet> * pInQueue, Queue<Packet> * pOutQueue):
    mId(id), mDb(0, rand, Const::TPCC_WAREHOUSE_NUM, id, pInQueue, pOutQueue)
{
    mDb.setTerminal(tid, wid);
    fprintf(stdout, "TpccWorker[%d] seed %u, w_id %u, t_id %u\n", id, rand, wid, tid); fflush(stdout);
}

TpccWorker::~TpccWorker()
{
}

void TpccWorker::publishStat()
{
    mDb.getConn().mStat.publish();
}

Stat & TpccWorker::getStat()
{
    return mDb.getConn().mStat;
}

Stat * TpccWorker::getStatDetail()
{
    return mStats;
}

void TpccWorker::run()
{
    fprintf(stdout, "TpccWorker[%d]: running ...\n", mId); fflush(stdout);
    int cnt = 0;
    mDb.getConn().mStat.setStart();
    for (int i = 0; i < 5; ++i) {
        mStats[i].setStart();
    }
    int type = 0;
    while (1) {
        Logger::out(2, "TpccWorker[%d]: begin %d\n", mId, cnt);
        mDb.beginTxn();
        double r = mDb.dg.rand();
        if( r <= 0.04 ) {
            type = 0;
            if (Const::MODE_TPCC_SI) {
                mDb.setIsolation(PID_SI);
            }
            mDb.txnStockLevel();
        } else if( r <= 0.08 ) {
            type = 1;
            mDb.txnDelivery();
        } else if( r <= 0.12 ) {
            type = 2;
            mDb.txnOrderStatus();
        } else if( r <= 0.55 ) {
            type = 3;
            mDb.txnPayment();
        } else {
            type = 4;
            mDb.txnNewOrder();
        }
        int dec = mDb.commitTxn();
        mStats[type].update(dec);
        if (++cnt % 2000 == 0) {
            fprintf(stdout, "TpccWorker[%d]: %d\n", mId, cnt); fflush(stdout);
        }
    }
}

void * TpccWorker::runHelper(void * worker)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
 
    ((TpccWorker *) worker)->run();
    pthread_exit(NULL);
}


