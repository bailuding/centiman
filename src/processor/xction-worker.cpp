#include "processor/xction-worker.h"

XctionWorker::XctionWorker(Queue<Packet> * pInQueue, Queue<Packet> * pOutQueue):

        mpInQueue(pInQueue), mpOutQueue(pOutQueue),

        mStat(), mReadCnt(0), mWriteCnt(0)

{
    mpXctions = new Packet *[Const::PROCESSOR_XCTION_QUEUE_SIZE];
    for (int i = 0; i < Const::PROCESSOR_XCTION_QUEUE_SIZE; ++i) {
        mpXctions[i] = new Packet();
    }
}

XctionWorker::~XctionWorker()
{
    finishLoad();
    for (int i = 0; i < Const::PROCESSOR_XCTION_QUEUE_SIZE; ++i) {
        delete mpXctions[i];
    }
    delete[] mpXctions;
    Logger::out(1, "XctionWorker: Destroyed!\n");
}

void XctionWorker::setTrace(char * file)
{
    mTraceFile = file;
}

void XctionWorker::publishStat()
{
    mStat.publish();
    if (Const::MODE_TPCC_TRACE) {
        for (int i = 0; i < 6; ++i) {
            std::string msg = "Detail("; msg += i + '0'; msg += ')';
            mTypeStat[i].publish(msg.c_str());
        }
    }
    Logger::out(0, "XctionWorker: %.2f R %.2f W\n", 
            mReadCnt / (mStat.mComplete + 0.1), mWriteCnt / (mStat.mComplete + 0.1));
}

void XctionWorker::resetStat()
{
    mCommitRspCnt = 0;
}

void XctionWorker::processReadRsp(Packet * rsp, Packet * xction)
{
    xction->mReadValues.fill(rsp->mReadCnt);
    xction->mReadSeqnums.fill(rsp->mReadCnt);
    for (size_t i = 0; i < rsp->mReadCnt; ++i) {
        Value * tmp = rsp->mReadValues.get(i);
        xction->mReadValues.set(i, tmp->mVal, tmp->mCnt);
        xction->mReadSeqnums.set(i, *(rsp->mReadSeqnums.get(i)));
    }
}

void XctionWorker::processCommitRsp(Packet * rsp, Packet * xction)
{
    xction->mDec = rsp->mDec;
    mStat.update(xction->mDec);
}

void XctionWorker::evenSplit(Packet * xction)
{
    for (int i = 0; i < xction->mReadCnt; ++i) {
        Key * key = xction->mReadKeys.get(i);
        int tmp = key->mVal[0];
        tmp = (tmp + Const::VALIDATOR_NUM - 1) / Const::VALIDATOR_NUM * Const::VALIDATOR_NUM + i % Const::VALIDATOR_NUM;
        key->mVal[0] = tmp % Const::VALIDATOR_NUM;
    }
    for (int i = 0; i < xction->mWriteCnt; ++i) {
        Key * key = xction->mWriteKeys.get(i);
        int tmp = key->mVal[0];
        tmp = (tmp + Const::VALIDATOR_NUM - 1) / Const::VALIDATOR_NUM * Const::VALIDATOR_NUM + i % Const::VALIDATOR_NUM;
        key->mVal[0] = tmp % Const::VALIDATOR_NUM;
    }
}

void XctionWorker::initLoad()
{
    Logger::out(0, "XctionWorker: load trace file %s\n", mTraceFile);
    mFin = fopen(mTraceFile, "r"); 
    assert(mFin != NULL);
}

void XctionWorker::load(Packet * xction)
{
    int type;
    int readCnt;
    int writeCnt;
    int keyCnt;
    int valueCnt;
    char buf[2048];
    uint8_t val[1024];

    /* type readCnt writeCnt */
    int rv = fscanf(mFin, "%d %d %d ", &type, &readCnt, &writeCnt); 
    assert(rv == 3);
    /* update stat */
    mReadCnt += readCnt;
    mWriteCnt += writeCnt;

    /* type */
    xction->mType = type;

    /* reads */
    xction->mReadCnt = readCnt;
    xction->mReadKeys.fill(readCnt);
    for (int i = 0; i < readCnt; ++i) {
        /* readKeyCnt readKey */
        int rv = fscanf(mFin, "%d ", &keyCnt, buf);
        assert(keyCnt < 1024);
        rv = fscanf(mFin, "%s ", buf);
        Util::hex2bytes(buf, val, keyCnt);
        xction->mReadKeys.get(i)->copyFrom(val, keyCnt);
    }

    /* writes */
    xction->mWriteCnt = writeCnt;
    xction->mWriteKeys.fill(writeCnt);
    xction->mWriteValues.fill(writeCnt);
    for (int i = 0; i < writeCnt; ++i) {
        /* writeKeyCnt writeKey */
        int rv = fscanf(mFin, "%d ", &keyCnt, buf);
        assert(keyCnt < 1024);
        rv = fscanf(mFin, "%s ", buf);
        Util::hex2bytes(buf, val, keyCnt);
        xction->mWriteKeys.get(i)->copyFrom(val, keyCnt);
        /* writeValueCnt */
        rv = fscanf(mFin, "%d ", &valueCnt);
        xction->mWriteValues.get(i)->setSize(valueCnt);
    }
}

void XctionWorker::finishLoad()
{
    fclose(mFin);
}

void XctionWorker::run()
{
    Packet * xction = NULL;
    Packet * in = NULL;
    Packet * out = NULL;
    QueueBatcher<Packet> inBatcher(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE);
    QueueBatcher<Packet> outBatcher(mpOutQueue, Const::UTIL_QUEUE_BATCH_SIZE);
 
    // counter
    int counterQueue = counter.addAvg("xwq");
    int counterLatency = counter.addAvg("xwl");

    if (Const::MODE_TPCC_TRACE) {
        initLoad();
    }
    /* populate read request */

    for (int i = 0; i < Const::PROCESSOR_XCTION_QUEUE_SIZE; ++i) {
        if (Const::MODE_TPCC_TRACE) {
            load(mpXctions[i]);
            mpXctions[i]->mPid = PID_CLIENT_REQ;
        } else {
            Generator::genPacketRand(mpXctions[i], PID_CLIENT_REQ);
        }

        // mark when the transaction starts
        mpXctions[i]->timer.mark(0);

        mpXctions[i]->mXid = i;
        out = outBatcher.getWriteSlot();
        Packet::getReadReq(out, mpXctions[i]);
        
        /*for (int j = 0; j < out->mReadCnt; ++j) {
            Logger::out(0, "ik %d %d ", out->mReadKeys.get(j)->toInt(), mpXctions[i]->mReadKeys.get(j)->toInt());
        }
        Logger::out(0, "\n"); */
        
        // mark when the packet gets into a queue
        out->timer.mark(0);

        outBatcher.putWriteSlot(out);

//        mpXctions[i]->setStart();

    } 
    
    Logger::out(0, "XctionWorker: populated all read requests\n");

    resetStat();
    mStat.setStart();

    /*time_t prev = clock();
    time_t cur = prev;
    
    for (int i = 0; i < 6; ++i) {
        mTypeStat[i].setStart();
    }*/

    /* start to process */
    while (1) {
        in = inBatcher.getReadSlot();
        xction = mpXctions[in->mXid];
       
        // mark the time when the packet gets into the queue
        in->timer.mark(1);
        // update queue wait time
        counter.update(counterQueue, in->timer.lap(1));

        if (in->mPid == PID_READ_RSP) {
            processReadRsp(in, xction);
            inBatcher.putReadSlot(in);
            out = outBatcher.getWriteSlot();
            Packet::getCommitReq(out, xction);

            // mark the time when the packet gets into the queue
            out->timer.mark(0);
            
            outBatcher.putWriteSlot(out);
        } else if (in->mPid == PID_COMMIT_RSP) {
            /* process commit response */
            ++mCommitRspCnt;
            processCommitRsp(in, xction);
            inBatcher.putReadSlot(in);
           // xction->setEnd();
           // mStat.latency(xction->getLatency());

           /* if (mCommitRspCnt % 100000 == 0) {
                cur = clock();
                Logger::out(0, "%lu|", (cur - prev) / 1000);
                prev = cur;
            }*/

            // mark when the transaction finishes
            xction->timer.mark(1);
            // update latency counter
            counter.update(counterLatency, xction->timer.lap(1));

            // mark when the transaction starts
            xction->timer.mark(0);

            /* generate read request */
            if (Const::MODE_TPCC_TRACE) {
                /* update type stat */
                mTypeStat[xction->mType].update(xction->mDec);
                mTypeStat[xction->mType].latency(xction->getLatency());

                load(xction);
                xction->mPid = PID_CLIENT_REQ;
            } else {
                Generator::genPacketRand(xction, PID_CLIENT_REQ);
            }
            out = outBatcher.getWriteSlot();
            Packet::getReadReq(out, xction);

            // mark the time when the packet gets into the queue
            out->timer.mark(0);

            outBatcher.putWriteSlot(out);
//            xction->setStart();
//            Logger::out(0, "commit rsp %d\n", xction->mXid);
        } else {
            assert(0);
        }
    }
}

void * XctionWorker::runHelper(void * worker)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
 
    ((XctionWorker *) worker)->run();
    pthread_exit(NULL);
}



