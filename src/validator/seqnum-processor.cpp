#include "validator/seqnum-processor.h"

void SeqnumProcessor::inc(int & idx)
{
    if (++idx == Const::VALIDATOR_IN_QUEUE_SIZE) {
        idx = 0;
    }
}

void SeqnumProcessor::forward()
{
    while (mPacketSeqnum[mIdx] == mLocalExpect) {
            ++mLocalExpect;
            inc(mIdx);
            mpExpect->store(mLocalExpect, std::memory_order_release);
    }
}

SeqnumProcessor::SeqnumProcessor(int id,
                                Queue<Packet> * pInQueue, 
                                std::atomic<Packet *> * packetQueue, 
                                std::atomic<Seqnum> * pExpect):
        mId(id),
        mpInQueue(pInQueue),
        mPacketQueue(packetQueue),
        mpExpect(pExpect),

        mBuf(Const::VALIDATOR_REQUEST_BUF_SIZE)
{
    if (Const::MODE_TPCC) {
        mpInBatcher = new QueueBatcher<Packet>(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE * (Const::PROCESSOR_NUM + 1) / 2);
    } else {
        mpInBatcher = new QueueBatcher<Packet>(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE * Const::PROCESSOR_NUM);
    }
    mPacketSeqnum = new Seqnum[Const::VALIDATOR_IN_QUEUE_SIZE];
    std::fill(mPacketSeqnum, mPacketSeqnum + Const::VALIDATOR_IN_QUEUE_SIZE, Const::SEQNUM_NULL);
}

SeqnumProcessor::~SeqnumProcessor()
{
    delete mpInBatcher;
    delete[] mPacketSeqnum;
    Logger::out(0, "SeqnumProcessor: Destroyed\n");
}

void SeqnumProcessor::resetStat()
{
    mPacketCnt = 0;
    mForwardCnt = 0;
    mUpdateCnt = 0;
}

void SeqnumProcessor::publishProfile()
{
//    Logger::out(0, "Validator[SeqnumProcessor]: pkt %d , fwd %d , update %d\n", mPacketCnt, mForwardCnt, mUpdateCnt);
    resetStat();
}

void SeqnumProcessor::run()
{
    Packet * in = NULL;
    
    mIdx = 0;
    mLocalExpect = 0;
    mPrev = 0;
    
    resetStat();
   
    int idx = 0;
    while (1) {
        in = mpInBatcher->getReadSlot();
        /* measure validator processing latency */
//        in->setStart();
      Logger::out(1, "Seqnum[%d]: expect %lu seq %lu r %d w %d\n", mId, mLocalExpect, in->mSeqnum, in->mReadCnt, in->mWriteCnt);
        
        //if (in->mReadCnt > 0 || in->mWriteCnt > 0) {
        in->mReuse = mBuf.getSlot(in->mpDelKeys);
        //}
        idx = in->mSeqnum % Const::VALIDATOR_IN_QUEUE_SIZE;        
        mPacketQueue[idx].store(in, std::memory_order_relaxed);
        mPacketSeqnum[idx] = in->mSeqnum;
        
        /* move forward */
        forward();
    }
}

void * SeqnumProcessor::runHelper(void * processor)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    ((SeqnumProcessor *) processor)->run();
    pthread_exit(NULL);
}


