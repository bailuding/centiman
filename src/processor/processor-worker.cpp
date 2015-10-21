#include "processor/processor-worker.h"

Seqnum ProcessorWorker::getSeqnum()
{
    return (mSeqnum++) * Const::PROCESSOR_NUM + mId;
}

Seqnum ProcessorWorker::readSeqnum()
{
    return mSeqnum * Const::PROCESSOR_NUM + mId;
}

ProcessorWorker::ProcessorWorker(int id, 
        Config * config,
        Queue<Packet> * pInQueue, Queue<Packet> * pXctionOutQueue,
        Queue<Packet> ** pXctionOutQueues,
        Queue<Packet> ** pServerOutQueues, std::map<int, int> * pServerSfdMap):

    mId(id), 
    config(config),
    mpInQueue(pInQueue), mpXctionOutQueue(pXctionOutQueue),
    mpXctionOutQueues(pXctionOutQueues),
    mpServerOutQueues(pServerOutQueues), mpServerSfdMap(pServerSfdMap),

    mSeqnum(0), mWaterMark(Const::PROCESSOR_XCTION_QUEUE_SIZE * Const::TPCC_WORKER_NUM),
    mXctionOutBatcher(pXctionOutQueue, Const::UTIL_QUEUE_BATCH_SIZE),

    mValidStat(), mReadStat(), mWriteStat(), mReadFirstStat(), mWriteFirstStat()
{
    if (Const::MODE_TPCC) {
        mXctions = new Packet[Const::TPCC_WORKER_NUM];
    } else {
        mXctions = new Packet [Const::PROCESSOR_XCTION_QUEUE_SIZE];
    }

    mpServerSfds = new int[Const::MAX_NETWORK_FD];
    std::fill(mpServerSfds, mpServerSfds + Const::MAX_NETWORK_FD, -1);

    mpValidatorOutBatchers = new QueueBatcher<Packet> * [Const::VALIDATOR_NUM];
    for (int i = 0; i < Const::VALIDATOR_NUM; ++i) {
        mpValidatorOutBatchers[i] = new QueueBatcher<Packet>(mpServerOutQueues[i], Const::UTIL_QUEUE_BATCH_SIZE);
    }

    mpStorageOutBatchers = new QueueBatcher<Packet> * [Const::STORAGE_NUM];
    for (int i = 0; i < Const::STORAGE_NUM; ++i) {
        mpStorageOutBatchers[i] = new QueueBatcher<Packet>(mpServerOutQueues[i + Const::VALIDATOR_NUM], Const::UTIL_QUEUE_BATCH_SIZE);
    }
}

ProcessorWorker::~ProcessorWorker()
{
    delete[] mXctions;

    delete[] mpServerSfds;
    for (int i = 0; i < Const::STORAGE_NUM; ++i) {
        delete mpStorageOutBatchers[i];
    }
    delete[] mpStorageOutBatchers;
    for (int i = 0; i < Const::VALIDATOR_NUM; ++i) {
        delete mpValidatorOutBatchers[i];
    }
    delete[] mpValidatorOutBatchers;

    Logger::out(1, "ProcessorWorker: Destroyed\n");
}

void ProcessorWorker::resetStat()
{
}

void ProcessorWorker::publishStat()
{
    Logger::out(0, "LatencyBreakDownRWV: %.2f %.2f %.2f %.2f %.2f %.2f\n", 
            mReadStat.avgLatency(), mWriteStat.avgLatency(), mValidStat.avgLatency(),
            mReadFirstStat.avgLatency(), mWriteFirstStat.avgLatency(), mValidFirstStat.avgLatency());
    Logger::out(0, "DiffDec: %.2f %.2f\n", 
            100.0 * mValidStat.mDiffDec / (mValidStat.mComplete + 1),
            100.0 * mValidStat.mInconsistent / (mValidStat.mComplete + 1));
}

void ProcessorWorker::processReadReq(Packet * in)
{
    static Packet * out[Const::MAX_STORAGE_NUM] = {};
    static int pktCnt = 0;
    static Seqnum prevWm = 0;

    Packet * xction = &mXctions[in->mXid];

    xction->mXid = in->mXid;
    xction->mReadCnt = in->mReadCnt;
    /* fill the vector */
    xction->mReadKeys.fill(in->mReadCnt);
    xction->mReadIdx.fill(in->mReadCnt);
    xction->mReadValues.fill(in->mReadCnt);
    xction->mReadSeqnums.fill(in->mReadCnt); 

    /* water mark */
    // update wm every 10 txn
    Seqnum wm = prevWm;
    if (Const::WATERMARK_FREQUENCY > 0 && pktCnt++ % Const::WATERMARK_FREQUENCY == 0) {
        wm = mWaterMark.getTail();
        prevWm = wm;
    }

    /* read seqnum */
    int pid = PID_READ_REQ;
    if (Const::MODE_SI || in->mPid == PID_READ_SI_REQ) {
        pid = PID_READ_SI_REQ;
        /* support multiple reads */
        if (xction->mReadSeqnum == Const::SEQNUM_NULL) {
            xction->mReadSeqnum = readSeqnum();
            /*            fprintf(stdout, "READSEQ %04X COMPLETE %04X\n", 
                          xction->mReadSeqnum, mWaterMark.getTail());
                          fflush(stdout);*/
        }
        if (xction->mReadSeqnum > (unsigned long)Const::PROCESSOR_XCTION_QUEUE_SIZE * Const::PROCESSOR_NUM / 4)
            xction->mReadSeqnum -= Const::PROCESSOR_XCTION_QUEUE_SIZE * Const::PROCESSOR_NUM / 4;
        else
            xction->mReadSeqnum = 0;
    }

    Logger::out(2, "Pworker: tail %lu, xid %u, %d r\n", wm, in->mXid, in->mReadCnt);

    size_t res = 0;
    for (size_t i = 0; i < in->mReadCnt; ++i) {
        Key * tmp = in->mReadKeys.get(i);
        assert(tmp != NULL);
        size_t idx = Index::getStorageIdx(tmp);
        if (out[idx] == NULL) {
            out[idx] = mpStorageOutBatchers[idx]->getWriteSlot();
            out[idx]->set(pid, idx, in->mXid);
            out[idx]->mWaterMark = wm;
            out[idx]->mReadSeqnum = xction->mReadSeqnum;
            ++res;
        }
        size_t cnt = out[idx]->mReadCnt;
        assert(tmp->mCnt > 0);
        out[idx]->mReadKeys.fillAndSet(cnt, tmp->mVal, tmp->mCnt);
        ++out[idx]->mReadCnt;
        assert(tmp->mCnt > 0);
        xction->mReadKeys.set(i, tmp->mVal, tmp->mCnt);
        xction->mReadIdx.set(i, idx);
    }
    xction->mState = res;

    for (int i = 0; i < Const::STORAGE_NUM; ++i) {
        if (out[i] != NULL) {
            assert(out[i]->mReadCnt > 0);
            // mark when a packet gets into a queue
            out[i]->timer.mark(0);

            mpStorageOutBatchers[i]->putWriteSlot(out[i]);
            out[i] = NULL;
        }
    }

    /* measure read latency */
    xction->setStart();
    xction->mFirst = 1;
}

int ProcessorWorker::processReadRsp(Packet * in)
{
    Packet * xction = &mXctions[in->mXid];

    assert(xction->mState > 0);

    size_t idx = mpServerSfds[in->mSfd] - Const::VALIDATOR_NUM;
    assert(in->mReadCnt > 0);
    for (size_t i = 0, j = 0; i < in->mReadCnt; ++i, ++j) {
        while (j < xction->mReadCnt && *(xction->mReadIdx.get(j)) != idx) {
            ++j;
        }
        assert(j < xction->mReadCnt);

        Value * tmp = in->mReadValues.get(i);
        /* tpcc may request NULL read */
        //        assert(tmp->mVal != NULL);
        //        assert(tmp->mCnt > 0);
        xction->mReadValues.set(j, tmp->mVal, tmp->mCnt);
        xction->mReadSeqnums.set(j, *(in->mReadSeqnums.get(i)));
    }

    /* update read water mark */
    if (in->mWaterMark < xction->mWaterMark) {
        xction->mWaterMark = in->mWaterMark;
    }

    /* measure read latency */
    /*    if (xction->mFirst) {
          xction->setEnd();
          mReadFirstStat.latency(xction->getLatency());
          mReadFirstStat.mComplete++;
          xction->mFirst = 0;
          }*/

    if (--xction->mState == 0) {
        /* measure read latency */
        xction->setEnd();
        mReadStat.latency(xction->getLatency());
        mReadStat.mComplete++;

        if (Const::MODE_TPCC) {
            Packet * out = mpXctionOutQueues[in->mXid]->getWriteSlot();
            Packet::getReadRsp(out, xction);

            // mark when a packet gets into a queue
            out->timer.mark(0);

            mpXctionOutQueues[in->mXid]->putWriteSlot(out);
        } else {
            Packet * out = mXctionOutBatcher.getWriteSlot();
            Packet::getReadRsp(out, xction);

            // mark when a packet gets into a queue
            out->timer.mark(0);

            mXctionOutBatcher.putWriteSlot(out);
        }
        return 1;
    }
    return 0;
}

void ProcessorWorker::processCommitReq(Packet * in)
{
    static Packet * out[Const::MAX_VALIDATOR_NUM] = {};

    static int counterWatermarkLag = counter.getId("wmlag");
    static int counterWatermarkLagAvg = counter.getId("wmlagavg");

    Packet * xction = &mXctions[in->mXid];
    /* fill the vector */
    xction->mWriteKeys.fill(xction->mWriteCnt);
    xction->mWriteValues.fill(xction->mWriteCnt);

    Packet::getCommitReq(xction, in);

    xction->mDec = DEC_COMMIT;
    xction->mState = 0;
    /* assign seqnum */
    xction->mSeqnum = getSeqnum();
    /* insert water mark */
    mWaterMark.insert(xction->mSeqnum);

    // stats on max watermark lag
    counter.updateMax(counterWatermarkLag, xction->mSeqnum - xction->mWaterMark);
    counter.update(counterWatermarkLagAvg, xction->mSeqnum - xction->mWaterMark);

    if (Const::MODE_NO_VALID) {
        xction->mState = 1;
        in->mDec = DEC_COMMIT;
        processValidRsp(in);
        return;
    }
    /* prepare validation request */

    int pid = PID_VALID_REQ;
    if (Const::MODE_SI || in->mPid == PID_COMMIT_SI_REQ) {
        pid = PID_VALID_SI_REQ;
        assert(xction->mReadSeqnum != Const::SEQNUM_NULL);
    }

    //    Logger::out(2, "Pworker: %u %lu | %u | %lu %lu\n", 
    //            in->mReadKeys.get(0)->mVal[0], *(in->mReadSeqnums.get(0)),
    //            in->mWriteKeys.get(0)->mVal[0], xction->mSeqnum, xction->mWaterMark);
    for (int i = 0; i < Const::VALIDATOR_NUM; ++i) {
        out[i] = mpValidatorOutBatchers[i]->getWriteSlot();
        out[i]->set(pid, i, in->mXid);
        out[i]->mSeqnum = xction->mSeqnum;
        out[i]->mWaterMark = xction->mWaterMark;
        out[i]->mReadSeqnum = xction->mReadSeqnum;
    }

    // get timestamp for switch mode
    int switchStage = config->getSwitchStage(xction->mSeqnum);
    static int counterKey = counter.getId("key");
    static int counterStage = counter.getId("stage");

    counter.set(counterStage, switchStage);

    for (int i = 0; i < xction->mReadCnt; ++i) {
        Key * tmp = xction->mReadKeys.get(i);
        assert(tmp->mCnt > 0);
        Seqnum seqnum = *(xction->mReadSeqnums.get(i));
        if (switchStage == 0 || switchStage == 1) {
            // normal: old hash
            int idx = Index::getValidatorId(&config->hashConfig, tmp);
            out[idx]->addValidReqRead(tmp, seqnum);
            counter.inc(counterKey);
            // prepare: old and new hash
            if (switchStage == 1) {
                int newIdx = Index::getValidatorId(&config->newHashConfig, tmp);
                if (newIdx != idx) {
                    out[newIdx]->addValidReqRead(tmp, seqnum);
                    counter.inc(counterKey);
                }
            }
        } else if (switchStage == 2) {
            // switch: new hash
            int idx = Index::getValidatorId(&config->newHashConfig, tmp);
            out[idx]->addValidReqRead(tmp, seqnum);
            counter.inc(counterKey);
        }
    }

    for (int i = 0; i < xction->mWriteCnt; ++i) {
        Key * tmp = xction->mWriteKeys.get(i);
        assert(tmp->mCnt > 0);
        if (switchStage == 0 || switchStage == 1) {
            // normal: old hash
            int idx = Index::getValidatorId(&config->hashConfig, tmp);
            out[idx]->addValidReqWrite(tmp);
            counter.inc(counterKey);
            // prepare: old and new hash
            if (switchStage == 1) {
                int newIdx = Index::getValidatorId(&config->newHashConfig, tmp);
                if (newIdx != idx) {
                    out[newIdx]->addValidReqWrite(tmp);
                    counter.inc(counterKey);
                }
            }
        } else if (switchStage == 2) {
            // switch: new hash
            int idx = Index::getValidatorId(&config->newHashConfig, tmp);
            out[idx]->addValidReqWrite(tmp);
            counter.inc(counterKey);
        }
    }


    Logger::out(1, "seq %d stage %d id0 %d v0 %d id1 %d v1 %d\n", xction->mSeqnum, switchStage, 
          out[0]->sid, out[0]->mReadCnt + out[0]->mWriteCnt, out[1]->sid, out[1]->mReadCnt + out[1]->mWriteCnt);

    xction->mState = Const::VALIDATOR_NUM;

    for (int i = 0; i < Const::VALIDATOR_NUM; ++i) {
        // mark when a packet gets into a queue
        out[i]->timer.mark(0);

        mpValidatorOutBatchers[i]->putWriteSlot(out[i]);
    }

    /* measure validation latency */
/*    xction->setStart();
    xction->mFirst = 1;*/

    //    fprintf(stdout, "water %lu seq %lu tail %lu\n", xction->mWaterMark, xction->mSeqnum, mWaterMark.getTail()); 
}

int ProcessorWorker::processValidRsp(Packet * in)
{
    static int counterCommit = counter.getId("commit");
    static int counterAbort = counter.getId("abort");
    static int counterAbortTxn = counter.getId("atxn");
    static int counterConflict = counter.getId("conflict");
    static int counterConflictTxn = counter.getId("actxn");

    static Packet * out[Const::MAX_STORAGE_NUM] = {};

    Packet * xction = &mXctions[in->mXid];

    // check switch mode
    int stage = config->getSwitchStage(xction->mSeqnum);
    // not in prepare stage or old hash in prepare stage
    if (stage != 1
            || (stage == 1 && config->hashConfig.hasId(in->sid))) {
        xction->mDec |= in->mDec;
        /* different decision */
        if (xction->mFirstDec == DEC_FULL) {
            xction->mFirstDec = xction->mDec;
        }
        if (xction->mFirstDec != xction->mDec) {
            xction->mDiffDec = 1;
            //fprintf(stdout, "DiffDec %u\n", xction->mSeqnum);
        }


    }
   /* if (Decision::hasConflict(in->mDec)) {
        Logger::out(0, "stage %d seq %d dec %d\n", 
                stage, xction->mSeqnum, xction->mDec);
    }*/
 
    assert(xction->mState > 0);

    if (--xction->mState == 0) {
        // update counter
        if (Decision::isCommit(xction->mDec))
            counter.inc(counterCommit);
        else {
            if (Decision::isConflict(xction->mDec)) {
                counter.inc(counterConflict);
                counter.appendList(counterConflictTxn, xction->mSeqnum);
            }
            counter.inc(counterAbort);
            counter.appendList(counterAbortTxn, xction->mSeqnum);
        }

        /* measure validation latency */
/*        xction->setEnd();
        mValidStat.latency(xction->getLatency());*/

        mValidStat.update(xction->mDec);
        if (xction->mDiffDec == 1) {
            mValidStat.diffDec();
        }
        if ((xction->mDec == DEC_COMMIT) && xction->mWriteCnt != 0) {
            size_t res = 0;
            for (size_t i = 0; i < xction->mWriteCnt; ++i) {
                Key * tmp = xction->mWriteKeys.get(i);
                size_t idx = Index::getStorageIdx(tmp);
                if (out[idx] == NULL) {
                    out[idx] = mpStorageOutBatchers[idx]->getWriteSlot();
                    out[idx]->set(PID_WRITE_REQ, idx, xction->mXid);
                    out[idx]->mSeqnum = xction->mSeqnum;
                    ++res;
                }
                size_t cnt = out[idx]->mWriteCnt;
                assert(tmp->mCnt > 0);
                out[idx]->mWriteKeys.fillAndSet(cnt, tmp->mVal, tmp->mCnt);
                Value * ptr = xction->mWriteValues.get(i);
                assert(ptr->mCnt > 0);
                out[idx]->mWriteValues.fillAndSet(cnt, ptr->mVal, ptr->mCnt);
                ++out[idx]->mWriteCnt;
            }

            xction->mState = res;

            for (int i = 0; i < Const::STORAGE_NUM; ++i) {
                if (out[i] != NULL) {
                    // mark when a packet gets into a queue
                    out[i]->timer.mark(0);

                    mpStorageOutBatchers[i]->putWriteSlot(out[i]);
                    out[i] = NULL;
                }
            }

            /* measure write latency */
          /*  xction->setStart();
            xction->mFirst = 1;*/
            return 1;
        } else {
            // no writes
            if (Const::MODE_TPCC) {
                Packet * outx = mpXctionOutQueues[in->mXid]->getWriteSlot();
                outx->set(PID_COMMIT_RSP, 0, in->mXid);
                outx->mDec = xction->mDec;
                // mark when a packet gets into a queue
                outx->timer.mark(0);

                mpXctionOutQueues[in->mXid]->putWriteSlot(outx);
            } else {
                Packet * outx = mXctionOutBatcher.getWriteSlot();
                outx->set(PID_COMMIT_RSP, 0, in->mXid);
                outx->mDec = xction->mDec;

                // mark when a packet gets into a queue
                outx->timer.mark(0);

                mXctionOutBatcher.putWriteSlot(outx);
            }
            /* remove water mark */
            mWaterMark.remove(xction->mSeqnum);
            //            fprintf(stdout, "water remove %lu tail %lu\n", xction->mSeqnum, mWaterMark.getTail());
            
            xction->reset();
            return 2;
        }
    }
    return 0;
}

int ProcessorWorker::processWriteRsp(Packet * in)
{
    Packet * xction = &mXctions[in->mXid];
    /* measure write latency */
    /*    if (xction->mFirst) {
          xction->setEnd();
          mWriteFirstStat.latency(xction->getLatency());
          mWriteFirstStat.mComplete++;
          xction->mFirst = 0;
          }*/

    if (--xction->mState == 0) {
        /* measure write latency */
        xction->setEnd();
        mWriteStat.latency(xction->getLatency());
        mWriteStat.mComplete++;

        if (Const::MODE_TPCC) {
            Packet * out = mpXctionOutQueues[in->mXid]->getWriteSlot();
            out->set(PID_COMMIT_RSP, 0, in->mXid);
            out->mDec = DEC_COMMIT;

            // mark when a packet gets into a queue
            out->timer.mark(0);

            mpXctionOutQueues[in->mXid]->putWriteSlot(out);
        } else {
            Packet * out = mXctionOutBatcher.getWriteSlot();
            out->set(PID_COMMIT_RSP, 0, in->mXid);
            out->mDec = DEC_COMMIT;

            // mark when a packet gets into a queue
            out->timer.mark(0);

            mXctionOutBatcher.putWriteSlot(out);
        }
        /* remove water mark */
        mWaterMark.remove(xction->mSeqnum);
        xction->reset();
        //        fprintf(stdout, "water remove %lu tail %lu\n", xction->mSeqnum, mWaterMark.getTail());
        return 1;
    }
    return 0;
}

void ProcessorWorker::run()
{
    int counterStage = counter.add("stage");
    int counterCommit = counter.add("commit");
    int counterAbort = counter.add("abort");
    int counterKey = counter.add("key");
    int counterAbortTxn = counter.addList("atxn");
    int counterConflict = counter.add("conflict");
    int counterConflictTxn = counter.addList("actxn");
    int counterWatermarkLag = counter.add("wmlag");
    int counterWatermarkLagAvg = counter.addAvg("wmlagavg");

    int counterQueue = counter.addAvg("pwq");

    Packet * in = NULL;
    QueueBatcher<Packet> batcher(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE);
    resetStat();

    queue<Packet *> slowPkt;
    int writeRspCnt = 0;
    int stage = 0;

    while (1) {
        in = batcher.getReadSlot();
        Logger::out(2, "Pworker[%d]: pid %d, xid %d, seq %lu\n", mId, in->mPid, in->mXid, mXctions[in->mXid].mSeqnum);

        // mark the time when a packet is taken out of the queue
        in->timer.mark(1);
        // update counter
        counter.update(counterQueue, in->timer.lap(1));

        // set the server socket file descriptor when getting the response for
        // the first time
        if ((in->mPid == PID_READ_RSP || in->mPid == PID_WRITE_RSP || in->mPid == PID_VALID_RSP) && mpServerSfds[in->mSfd] == -1) {
            assert(mpServerSfdMap->find(in->mSfd) != mpServerSfdMap->end());
            mpServerSfds[in->mSfd] = (*mpServerSfdMap)[in->mSfd];
        }

        if (in->mPid == PID_READ_REQ || in->mPid == PID_READ_SI_REQ) {
            processReadReq(in);
        } else if (in->mPid == PID_READ_RSP) {
            processReadRsp(in);
        } else if (in->mPid == PID_COMMIT_REQ || in->mPid == PID_COMMIT_SI_REQ) {
            processCommitReq(in);
            // update stage
            stage = max(stage, config->getSwitchStage(mXctions[in->mXid].mSeqnum));
        } else if (in->mPid == PID_VALID_RSP) {
            processValidRsp(in);
        /*    if (processValidRsp(in) != 0) {
                Logger::out(0, "vrsp seq %d dec %d\n", 
                        mXctions[in->mXid].mSeqnum, mXctions[in->mXid].mDec);
            }*/
        } else if (in->mPid == PID_WRITE_RSP) {
             ++writeRspCnt;
            // process slow write response
            while (!slowPkt.empty() 
                    && writeRspCnt - (int)slowPkt.front()->prevSeq >= Const::SLOW_TXN_LAG) {
                Packet * p = slowPkt.front();
                processWriteRsp(p);
                slowPkt.pop();
                Logger::out(0, "process slow write rsp %d cnt %d\n", p->prevSeq, writeRspCnt);
                // put back the packet
                batcher.putReadSlot(p);
            }
            // introduce slow write response in stage 1
            // only for the first processor to avoid async
            if (stage == 1
                   && mId == 0 
                   && Const::SLOW_TXN_FREQUENCY != 0
                   && writeRspCnt % Const::SLOW_TXN_FREQUENCY < 10) {
                in->prevSeq = writeRspCnt;
                Logger::out(0, "add slow write rsp %d cnt %d\n", in->prevSeq, writeRspCnt);
                slowPkt.push(in);
                continue;
            }
            processWriteRsp(in);
        } else {
            assert(0);
        }
        batcher.putReadSlot(in);
    }
}

void * ProcessorWorker::runHelper(void * worker)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    ((ProcessorWorker *) worker)->run();
    pthread_exit(NULL);
}


