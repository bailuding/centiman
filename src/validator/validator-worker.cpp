#include "validator/validator.h"

int ValidatorWorker::validateSi(Packet * in)
{
    if (Const::MODE_NO_ABORT || Const::MODE_NO_OP)
        return DEC_COMMIT;
    
    int dec = DEC_COMMIT;
 
    for (int i = 0; i < in->mReadCnt; ++i) {
        dec |= validateSiR(in, i);    
    }
    for (int i = 0; i < in->mWriteCnt; ++i) {
        dec |= validateSiW(in, i);
    }
    return dec;
}

int ValidatorWorker::validateSiR(Packet * in, int idx)
{
    static Key * key;
    static int hid;
    static Seqnum seq;
    /* handle non-existing key */
    if (*(in->mReadSeqnums.get(idx)) == Const::SEQNUM_NULL)
        return DEC_COMMIT;

    key = in->mReadKeys.get(idx);
    hid = Index::getIdx(key);
    seq = mHashTable.get(key, Index::getValidatorHashIdx(hid), in->mReadSeqnum, mSearchGet);
    ++mOps;
    ++mGetCnt;
    
    if (seq == Const::SEQNUM_NULL) {
        if (mDelSeqnum > in->mReadSeqnum) {
/*            fprintf(stdout, "SI NOT FOUND, SEQ %08X RSEQ %08X RVER %08X DEL %08X\n", 
                    in->mSeqnum, in->mReadSeqnum, *(in->mReadSeqnums.get(idx)), mDelSeqnum); fflush(stdout); 
                    return DEC_NOT_FOUND;*/
        }
        return DEC_COMMIT;
    }
    if (seq > *(in->mReadSeqnums.get(idx)) && seq <= in->mReadSeqnum) {
// fprintf(stdout, "in %lu seq %lu read %lu readseq %lu\n", in->mSeqnum, seq, *(in->mReadSeqnums.get(idx)), in->mReadSeqnum);
/*        fprintf(stdout, "INCONSIST SEQ %08X RSEQ %08X RVER %08X VER %08X\n", 
            in->mSeqnum, in->mReadSeqnum, *(in->mReadSeqnums.get(idx)), seq);
        fflush(stdout);*/
        if (Const::MODE_FALSE_ABORT) {
            fprintf(stdout, "VA %08X %08X\n", (unsigned)in->mSeqnum, (unsigned)seq);
        }
        return DEC_INCONSISTENT;
    }
    return DEC_COMMIT;
}

int ValidatorWorker::validateSiW(Packet * in, int idx)
{
    static Key * key;
    static int hid;
    static Seqnum seq;
    key = in->mWriteKeys.get(idx);
    assert(key->mCnt > 0 && key->mVal != NULL);
    hid = Index::getIdx(key);
    seq = mHashTable.get(key, Index::getValidatorHashIdx(hid), mSearchGet);
    ++mOps;
    ++mGetCnt;
    
    if (seq == Const::SEQNUM_NULL) {
        if (mDelSeqnum > in->mWaterMark)
            return DEC_NOT_FOUND;
        return DEC_COMMIT;
    }
    if (seq > in->mSeqnum && seq > in->mWaterMark) {
        if (Const::MODE_FALSE_ABORT) {
            fprintf(stdout, "VA %08X %08X\n", (unsigned int)in->mSeqnum, (unsigned int)seq);
        }
        return DEC_CONFLICT;
    }
    return DEC_COMMIT;
}

int ValidatorWorker::validate(Packet * in)
{
    if (Const::MODE_NO_ABORT || Const::MODE_NO_OP)
        return DEC_COMMIT;
 
    int dec = DEC_COMMIT;

    for (int i = 0; i < in->mReadCnt; ++i) {
        int rv = validate(in, i);
        dec |= rv;
    }

    return dec;
}

int ValidatorWorker::validate(Packet * in, int idx)
{
    static int counterConflictTxn = counter.getId("ctxn");
    static Key * key;
    static int hid;
    static Seqnum seq;
    key = in->mReadKeys.get(idx);
    hid = Index::getIdx(key);
    seq = mHashTable.get(key, Index::getValidatorHashIdx(hid), mSearchGet);
    ++mOps;
    ++mGetCnt;
    
    Seqnum rver = *(in->mReadSeqnums.get(idx));
    
    // the item never exists
    if (rver == Const::SEQNUM_NULL)
        return DEC_NONEXIST;

    Seqnum maxts = max(rver, in->mWaterMark);

    // the item is not found in the cache
    if (seq == Const::SEQNUM_NULL) {
        // no conflict if max(ver, wm) is larger than the truncation
        if (maxts >= mDelSeqnum)
            return DEC_COMMIT;
        // otherwise, not enough info to decide whether there is any conflict
        Logger::out(2, "v %d sid %d seq %d key %d ver %d wm %d vseq %d trunc %d\n",
            mId, in->sid, in->mSeqnum, key, rver, in->mWaterMark, seq, mDelSeqnum);
        // assume it is the latest version if the watermark is off
        if (Const::WATERMARK_FREQUENCY == 0)
            return DEC_COMMIT;
        return DEC_UNKNOWN;
    }

    // the item is in the cache
    // if seq < maxts, it should be reflected in the reads
    if (seq > maxts) {
        // update counter
        counter.appendList(counterConflictTxn, in->mSeqnum);
        counter.appendList(counterConflictTxn, seq);
        counter.appendList(counterConflictTxn, in->mWaterMark);
        return DEC_CONFLICT;
    } else
        return DEC_COMMIT;
}

void ValidatorWorker::remove(ArrayKey * keys)
{
    if (Const::MODE_NO_OP)
        return;
    for (int i = 0; i < (int)keys->mCnt; ++i) {
        remove(keys, i);
    }
}

void ValidatorWorker::remove(ArrayKey * keys, int idx)
{
    Key * key;
    int hid;
    key = keys->get(idx);
    hid = Index::getIdx(key);
    int rv = mHashTable.del(key, Index::getValidatorHashIdx(hid), mSearchDel);
    assert(rv >= 0);
    --mRec;
    ++mOps;
    ++mDelCnt;
}

void ValidatorWorker::update(Packet * in)
{
    if (Const::MODE_NO_OP)
        return;
    for (int i = 0; i < in->mWriteCnt; ++i) {
        update(in, i);
    }
}

void ValidatorWorker::update(Packet * in, int idx)
{
    Key * key;
    int hid;
    key = in->mWriteKeys.get(idx);
    hid = Index::getIdx(key);
    mHashTable.put(key, Index::getValidatorHashIdx(hid), in->mSeqnum, mSearchPut);
    ++mRec;
    ++mOps;
    ++mPutCnt;
}

void ValidatorWorker::inc(int &idx)
{
    ++idx;
    if (idx == Const::VALIDATOR_IN_QUEUE_SIZE)
        idx = 0;
}

ValidatorWorker::ValidatorWorker(int id,
        Config * config,
        Queue<Packet> * pInQueue,
        std::map<int, Queue<Packet> *> * pOutQueues,
        std::atomic<Packet *> * packetQueue,
        std::atomic<Seqnum> * pSeqnumExpect):

        mId(id),
        mpInQueue(pInQueue),
        mpOutQueues(pOutQueues),
        mPacketQueue(packetQueue),
        mpSeqnumExpect(pSeqnumExpect),

        mHashTable(Const::VALIDATOR_HASH_TABLE_SIZE, Const::VALIDATOR_HASH_BUF_SIZE),
        mStat()
{
    this->config = config;
    resetStat();
}

ValidatorWorker::~ValidatorWorker()
{
    Logger::out(0, "ValidatorWorker: Destroyed!\n");
}

void ValidatorWorker::resetStat()
{
    mOps = 0;
    mCheckCnt = 0;
    mPacketCnt = 0;
    mUpdateCnt = 0;
    mSleepCnt = 0;
    mUniqueCnt = 0;
    mGetCnt = 0;
    mPutCnt = 0;
    mDelCnt = 0;

    mSearchGet = 0;
    mSearchPut = 0;
    mSearchDel = 0;
}

void ValidatorWorker::publishStat()
{
/*    fprintf(stdout, "ValidProcLatency: %.2f R %.2f W %.2f\n", mStat.avgLatency(), 
            mRead / (mStat.mComplete + 0.1), mWrite / (mStat.mComplete + 0.1));
    fflush(stdout);*/
}

void ValidatorWorker::publishProfile()
{
    Seqnum seqnum = *mpSeqnumExpect;
    int tmp = 0;
    if (seqnum > mLocalExpect)
        tmp = seqnum - mLocalExpect;

    char buf[2048];
    int idx = 0;
    // Queue
    idx += sprintf(buf + idx, "Queue %d/%d | Rec %d\n", tmp, mpInQueue->mSize, mRec / 1000);
    // Get
//    idx += sprintf(buf + idx, "Get ");
//    for (int i = 1; i <= Const::XCTION_READ_CNT; ++i) {
//        idx += sprintf(buf + idx, "%d/", mGetCnts[i] * i / 1000);
//    }
//    idx += sprintf(buf + idx, " | Search %.2f\n", double(mSearchGet) / (mGetCnt + 1));
    // Put
//    idx += sprintf(buf + idx, "Put ");
//    for (int i = 1; i <= Const::XCTION_WRITE_CNT; ++i) {
//        idx += sprintf(buf + idx, "%d/", mPutCnts[i] * i / 1000);
//    }
//    idx += sprintf(buf + idx, " | Search %.2f\n", double(mSearchPut) / (mPutCnt + 1));
    // Del
//    idx += sprintf(buf + idx, "Del ");
//    for (int i = 1; i <= Const::XCTION_WRITE_CNT; ++i) {
//        idx += sprintf(buf + idx, "%d/", mDelCnts[i] * i / 1000);
//    }
//    idx += sprintf(buf + idx, " | Search %.2f\n", double(mSearchDel) / (mDelCnt + 1));
    // Ops
    idx += sprintf(buf + idx, "OPs %d/%d/%d/%d\n", mOps / 1000, mGetCnt / 1000, mPutCnt / 1000, mDelCnt / 1000);

    // Checks
//    idx += sprintf(buf + idx, "Check %lu/%lu/%lu | Update %lu | Wait %d/%d\n", 
//                   mCheckCnt, mPacketCnt, mPacketCnt / (mCheckCnt + 1), mUpdateCnt, 
//                   mSleepCnt, mUniqueCnt);
    // Log 
    Logger::out(0, "\nProfile[ValidatorWorker %d]\n%s", mId, buf);
    resetStat();
}

void ValidatorWorker::run()
{
    // counter
    int counterStage = counter.add("stage");
    int counterCommit = counter.add("commit");
    int counterConflict = counter.add("conflict");
    int counterUnknown = counter.add("unknown");
    int counterKey = counter.add("key");
    int counterConflictTxn = counter.addList("ctxn");
    int counterQueue = counter.addAvg("vwq");

    Packet * in = NULL;
    Packet * out = NULL;

    int idx = 0;
    mLocalExpect = 0;
    if (config->hashConfig.hasId(mId))
        mDelSeqnum = 0;
    else
        mDelSeqnum = config->getPrepareTime();

    int batsz = Const::UTIL_QUEUE_BATCH_SIZE * Const::PROCESSOR_NUM;
    if (Const::MODE_TPCC) {
        batsz = Const::UTIL_QUEUE_BATCH_SIZE * (Const::PROCESSOR_NUM + 1) / 2;
    }
    QueueBatcher<Packet> inBatcher(mpInQueue, batsz);
    QueueBatcher<Packet> * outBatchers[Const::MAX_NETWORK_FD] = {};

    while (1) {
        Seqnum finish = mpSeqnumExpect->load(std::memory_order_acquire);
        counter.set(counterStage, config->getSwitchStage(finish));

        for (Seqnum i = mLocalExpect; i < finish; ++i) {
            in = mPacketQueue[idx].load(std::memory_order_relaxed);
            assert(in != NULL);

            // mark wait time in the queue
            in->timer.mark(1);
            counter.update(counterQueue, in->timer.lap(1));

            /* remove packets from buffer */
            if (in->mReuse 
                    && in->mpDelKeys->mCnt > 0) {
                remove(in->mpDelKeys);
                in->mpDelKeys->mCnt = 0;
                // prevent GC watermark to revert when an old packet was removed
                mDelSeqnum = max(mDelSeqnum, in->prevSeq);
                Logger::out(3, "v %d seq %d trunc %d\n", mId, in->mSeqnum, mDelSeqnum);
            }

            // remove cached updates
            if (in->mReadCnt > 0 || in->mWriteCnt > 0) {
                int dec = DEC_COMMIT;
                if (Const::MODE_SI || in->mPid == PID_VALID_SI_REQ)
                    dec |= validateSi(in);
                else
                    dec |= validate(in);

                // update counter
                if (Decision::hasConflict(dec)) 
                    counter.inc(counterConflict);
                if (Decision::hasUnknown(dec))
                    counter.inc(counterUnknown);
                if (Decision::isCommit(dec))
                    counter.inc(counterCommit);
                counter.update(counterKey, in->mReadCnt + in->mWriteCnt);

                /* add updates to buffer */
                if (Decision::isCommit(dec)) {
                    update(in);
                }

                /* send packet */
                if (outBatchers[in->mSfd] == NULL) {
                    assert(mpOutQueues->find(in->mSfd) != mpOutQueues->end());
                    outBatchers[in->mSfd] = new QueueBatcher<Packet> ((*mpOutQueues)[in->mSfd], Const::UTIL_QUEUE_BATCH_SIZE);
                }
                
                out = outBatchers[in->mSfd]->getWriteSlot();
                out->set(PID_VALID_RSP, in->sid, in->mXid);
                out->mDec = dec;
                // mark timestamp for send queue wait
                out->timer.mark(0);
                outBatchers[in->mSfd]->putWriteSlot(out);

//                fprintf(stdout, "vworker seq %lu\n", in->mSeqnum);

                /* update request buf */
                if (Decision::isCommit(dec) && !Const::MODE_NO_OP) {
                    in->mpDelKeys->mCnt = in->mWriteCnt;
                    in->mpDelKeys->fill(in->mWriteCnt);
                    for (int i = 0; i < in->mWriteCnt; ++i) {
                        Key * key = in->mWriteKeys.get(i);
                        Key * tmp = in->mpDelKeys->get(i);
                        in->mpDelKeys->set(i, key);
                        in->mWriteKeys.set(i, tmp);
                        in->prevSeq = in->mSeqnum;
                    }
                } else {
                    in->mpDelKeys->mCnt = 0;
                }
            }

            /* measure validator processing latency */
/*            in->setEnd();
            mStat.latency(in->getLatency());
            mStat.mComplete++;*/
            
            inBatcher.putReadSlot(in);
            inc(idx);
        }
        mLocalExpect = finish;
    }
}

void * ValidatorWorker::runHelper(void * worker)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
    ((ValidatorWorker *) worker)->run();
    pthread_exit(NULL);
}


