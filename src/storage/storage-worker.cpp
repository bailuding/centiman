#include "storage/storage-worker.h"

StorageWorker::StorageWorker(int id,
        Config * config,
        Queue<Packet> * pInQueue, std::map<int, Queue<Packet> *> * pOutQueues):

        mId(id),
        mpInQueue(pInQueue), mpOutQueues(pOutQueues),
        
        mHashTable(Const::STORAGE_HASH_TABLE_SIZE),
        mTraceFile(NULL)
{
    this->config = config;
    if (Const::MODE_TPCC) {
        mpInBatcher = new QueueBatcher<Packet>(pInQueue, Const::UTIL_QUEUE_BATCH_SIZE * (Const::PROCESSOR_NUM + 1) / 2);
    } else {
        mpInBatcher = new QueueBatcher<Packet>(pInQueue, Const::UTIL_QUEUE_BATCH_SIZE * Const::PROCESSOR_NUM);
    }
    mpOutBatchers = new QueueBatcher<Packet> * [Const::MAX_NETWORK_FD] ();
    mSfds = new int[Const::MAX_NETWORK_FD] ();
    mWaterMarks = new Seqnum[Const::PROCESSOR_NUM] ();
    resetStat();
}

StorageWorker::~StorageWorker()
{
    delete mpInBatcher;
    for (int i = 0; i < Const::MAX_NETWORK_FD; ++i) {
        delete mpOutBatchers[i];
    }
    delete[] mpOutBatchers;
    delete[] mWaterMarks;
    Logger::out(1, "StorageWorker: Destroyed!\n");
}

void StorageWorker::resetStat()
{
    mHashTable.mGetOps = mHashTable.mPutOps = 0;
    mHashTable.mSearch = 0;
    mNullRead = 0;
}

void StorageWorker::publishProfile()
{
    int ops = mHashTable.mGetOps + mHashTable.mPutOps;
    if (ops == 0 || mNullRead > 0) {
        Logger::out(0, "Queue[StorageWorker]: %u / %u / %u / %u K Ops , %u K recs, "
                       "%u K unique, %u K / %.2f sch\n", 
                        ops / 1000, 
                        mHashTable.mGetOps / 1000, mHashTable.mPutOps / 1000, mNullRead,
                        mHashTable.mRec / 1000,
                        mHashTable.mUnique / 1000,
                        mHashTable.mSearch / 1000, double(mHashTable.mSearch) / (ops + 1));
    }
    resetStat();
}

void StorageWorker::populate()
{
    Logger::out(0, "StorageWorker[%d]: start populating records\n", mId);
    time_t start = time(NULL);

    Bytes key(Const::XCTION_KEY_SIZE);
    key.mCnt = Const::XCTION_KEY_SIZE;
    Bytes value(Const::XCTION_VALUE_SIZE);
    value.mCnt = Const::XCTION_VALUE_SIZE;

    key.mVal[0] = 0;
    for (int i = 0; i < Const::DB_SIZE; ++i) {
        key.setBytes(i);
        size_t idx = Index::getIdx(&key);
        if (Index::getStorageIdx(idx) == mId) {
            int rv = mHashTable.put(key, value, 0);
            assert(rv == 2);
        }
    }
    double diff = time(NULL) - start;
    Logger::out(0, "StorageWorker[%d]: finish populating %u records in %.0f seconds\n", mId, mHashTable.mRec, diff);
}

void StorageWorker::populateTpcc()
{
//    Random rng;
    TPCC::DB db(0, 17, Const::TPCC_WAREHOUSE_NUM);
    Logger::out(0, "StorageWorker[%d]: start populating tpcc\n", mId);
    time_t start = time(NULL);
    db.generate(mHashTable, mId);
    double diff = time(NULL) - start;
    Logger::out(0, "StorageWorker[%d]: finish populating tpcc in %.0f seconds\n", mId, diff);
}

void StorageWorker::populateTpccTrace()
{
    assert(mTraceFile != NULL);
    Logger::out(0, "StorageWorker[%d]: start populating tpcc trace from %s\n", mId, mTraceFile);
    time_t start = time(NULL);

    FILE * fin = fopen(mTraceFile, "r");
    assert(fin != NULL);

    char buf[2048];
    uint8_t val[1024];
    Bytes key;
    Bytes value;
    int keyCnt;
    int valueCnt;
    while (fscanf(fin, "%d %s %d ", &keyCnt, buf, &valueCnt) == 3) {
        /* convert from hex to byte */
        Util::hex2bytes(buf, val, keyCnt);
        key.copyFrom(val, keyCnt);
        /* value */
        value.setSize(valueCnt);
        mHashTable.put(key, value, 0);
    }
    fclose(fin);
    double diff = time(NULL) - start;
    Logger::out(0, "StorageWorker[%d]: finish populating tpcc trace in %.0f seconds\n", mId, diff);
}

void StorageWorker::run()
{
    if (Const::MODE_TPCC_TRACE) {
        populateTpccTrace();
    } else if (Const::MODE_TPCC) {
        populateTpcc();
    } else {
        populate();
    }
    Packet * in = NULL;
    Packet * out = NULL;
    int mSfdCnt = 0;
    
    int readCnt = 0;
    int writeCnt = 0;

    int counterStage = counter.add("stage", true);
    int counterWrite = counter.add("write");
    int counterWriteReq = counter.add("writereq");
    int counterRead = counter.add("read");
    int counterReadReq = counter.add("readreq");
    int counterQueue = counter.addAvg("swq");
    int stage = 0;

    while (1) {
        in = mpInBatcher->getReadSlot();
        if (mpOutBatchers[in->mSfd] == NULL) {
            mpOutBatchers[in->mSfd] = new QueueBatcher<Packet>((*mpOutQueues)[in->mSfd], Const::UTIL_QUEUE_BATCH_SIZE);
            mSfds[in->mSfd] = mSfdCnt++;
        }

        // update stage
        if (in->mPid == PID_WRITE_REQ) {
            // stage is non revertable
            stage = max(stage, config->getSwitchStage(in->mSeqnum));
            counter.set(counterStage, stage);
        }

        // mark the time when the packet gets out of the queue
        in->timer.mark(1);
        counter.update(counterQueue, in->timer.lap(1));

        out = mpOutBatchers[in->mSfd]->getWriteSlot();
        
        Logger::out(2, "StorageWorker[%d]: pid %d, r %d, w %d\n", mId, in->mPid, in->mReadCnt, in->mWriteCnt);

        if (in->mPid == PID_READ_REQ || in->mPid == PID_READ_SI_REQ) {
            out->set(PID_READ_RSP, in->sid, in->mXid);
            assert(in->mReadCnt > 0);
            out->mReadCnt = in->mReadCnt;
            out->mReadValues.fill(out->mReadCnt);
            out->mReadSeqnums.fill(out->mReadCnt);

            if (in->mPid == PID_READ_REQ) {
                /* read latest version */
                for (int i = 0; i < in->mReadCnt; ++i) {
                    int rv = mHashTable.get(*(in->mReadKeys.get(i)), out->mReadValues.get(i), out->mReadSeqnums.get(i));
                    /* may request NULL read in TPCC */
                    if (*(out->mReadSeqnums.get(i)) == Const::SEQNUM_NULL) {
                        ++mNullRead;
                    }
/*                    if (*(out->mReadSeqnums.get(i)) == Const::SEQNUM_NULL) {
                        fprintf(stdout, "rv %d, read %d, write %d\n", rv, readCnt, writeCnt);
                        in->mReadKeys.get(i)->print();
                    }*/
/*                    if (rv != 1) {
                        in->mReadKeys.get(i)->print();
                        fprintf(stdout, "%d %d\n", mId, Index::getStorageIdx(in->mReadKeys.get(i)));
                    }
                    assert(rv == 1);
                    assert(*(out->mReadSeqnums.get(i)) != Const::SEQNUM_NULL);*/
//                    assert(out->mReadValues.get(i)->mCnt > 0);
//                    assert(rv == 1);
                }
            } else {
                /* read a snapshot */
                assert(in->mReadKeys.mVec.size() >= in->mReadCnt);
                for (int i = 0; i < in->mReadCnt; ++i) {
                    int rv = mHashTable.get(*(in->mReadKeys.get(i)), in->mReadSeqnum, out->mReadValues.get(i), out->mReadSeqnums.get(i));
                    if (*(out->mReadSeqnums.get(i)) == Const::SEQNUM_NULL) {
                        ++mNullRead;
                    }

                    //                    assert(rv == 1);
/*                    if (rv != 1) {
                        in->mReadKeys.get(i)->print();
                        fprintf(stdout, "%d %d\n", mId, Index::getStorageIdx(in->mReadKeys.get(i)));
                    }
                    assert(rv == 1);
                    assert(*(out->mReadSeqnums.get(i)) != Const::SEQNUM_NULL);*/
                }
            }

            /* update complete water mark */
            mWaterMarks[mSfds[in->mSfd]] = in->mWaterMark;
            /* get water mark */
            out->mWaterMark = getReadWaterMark();
            assert(out->mReadCnt > 0);
//            Logger::out(2, "Sworker: %u %lu %lu\n", in->mReadKeys.get(0)->mVal[0], in->mWaterMark, out->mWaterMark);
//
            // update counter
            counter.update(counterRead, in->mReadCnt);            
            counter.inc(counterReadReq);
            ++readCnt;
        } else  {
            out->set(PID_WRITE_RSP, in->sid, in->mXid);
            for (size_t i = 0; i < in->mWriteCnt; ++i) {
                assert(in->mWriteKeys.get(i) != NULL);
                assert(in->mWriteKeys.get(i)->mVal != NULL);
                assert(in->mWriteKeys.get(i)->mCnt > 0);
                int rv = mHashTable.put(*(in->mWriteKeys.get(i)), *(in->mWriteValues.get(i)), in->mSeqnum);
                assert(in->mSeqnum != Const::SEQNUM_NULL);
            }
            ++writeCnt;

            // update counter
           counter.update(counterWrite, in->mWriteCnt);
           counter.inc(counterWriteReq);
        }

        // mark timestamp for sender queue wait
        out->timer.mark(0);
        mpOutBatchers[in->mSfd]->putWriteSlot(out);
        mpInBatcher->putReadSlot(in);
    }
}

void * StorageWorker::runHelper(void * worker)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    ((StorageWorker *) worker)->run();
    pthread_exit(NULL);
}
inline
Seqnum StorageWorker::getReadWaterMark()
{
    Seqnum tmp = Const::SEQNUM_NULL;
    for (int i = 0; i < Const::PROCESSOR_NUM; ++i) {
        if (mWaterMarks[i] < tmp) {
            tmp = mWaterMarks[i];
        }
    }
    return tmp;
}

void StorageWorker::setTrace(char * file)
{
    mTraceFile = file;
}

