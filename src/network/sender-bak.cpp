#include "network/sender.h"


Sender::Sender(std::map<int, Queue<Packet> *> * pOutQueues)
{
    mpOutQueues = pOutQueues;
 
    mpBufs = new SocketBuf *[Const::MAX_NETWORK_FD] ();
    mpBatchers = new QueueBatcher<Packet> *[Const::MAX_NETWORK_FD] ();
    for (std::map<int, Queue<Packet> *>::iterator it = mpOutQueues->begin();
            it != mpOutQueues->end(); ++it) {
        mpBufs[it->first] = new SocketBuf();
        mpBatchers[it->first] = new QueueBatcher<Packet>(it->second, Const::UTIL_QUEUE_BATCH_SIZE);
    }

    mId = 0;
    mpInQueue = NULL;

    resetStat();
}

Sender::~Sender()
{
    for (int i = 0; i < Const::MAX_NETWORK_FD; ++i) {
        delete mpBufs[i];
        delete mpBatchers[i];
    }
    delete[] mpBufs;
    delete[] mpBatchers;
}

void Sender::resetStat()
{
    mPacketCnt = 0;
    mSendCnt = 0;
    mFullSend = 0;
    mFullPacketSend = 0;
    mByteCnt = 0;

}

void Sender::setId(int id)
{
    mId = id;
}

void Sender::setInQueue(Queue<Packet> * inQueue)
{
    mpInQueue = inQueue;
}

int Sender::getByteCnt()
{
    return mByteCnt;
}

int Sender::getPacketCnt()
{
    return mPacketCnt;
}

int Sender::getSendCnt()
{
    return mSendCnt;
}

int Sender::getFullSend()
{
    return mFullSend;
}

int Sender::getFullPacketSend()
{
    return mFullPacketSend;
}

int Sender::getCheckCnt()
{
    return mCheckCnt;
}

// send out everything in the buffer
int Sender::sendBuf(int sfd, SocketBuf * sbuf)
{
    int cnt = 0;
    int flags = MSG_NOSIGNAL;
    int rv = 0;
    while (sbuf->mExp < sbuf->mCnt) {
        int tmp = send(sfd, sbuf->mBuf + sbuf->mExp, sbuf->mCnt - sbuf->mExp, flags);
//        fprintf(stdout, "send %d bytes to %d, buf cnt %d, idx cnt %d\n", tmp, sfd, sbuf->mCnt, sbuf->mExp);
        if (tmp > 0) {
            cnt += tmp;
            sbuf->mExp += tmp;
            ++mSendCnt;
        } else {
//            fprintf(stdout, "send %d bytes to %d, buf cnt %d, idx cnt %d, errno %d\n", cnt, sfd, sbuf->mCnt, sbuf->mExp, errno);
            rv = -1;
            break;
        }
    }

    mByteCnt += cnt;
    /* reset or copy residual bytes */
    if (sbuf->mExp == sbuf->mCnt) {
        sbuf->mCnt = sbuf->mExp = 0;
    } else if (sbuf->mExp > 0) {
        memmove(sbuf->mBuf, sbuf->mBuf + sbuf->mExp, sbuf->mCnt - sbuf->mExp);
        sbuf->mCnt -= sbuf->mExp;
        sbuf->mExp = 0;
//        fprintf(stdout, "move %d bytes\n", sbuf->mCnt);
    }

//    fprintf(stdout, "finish, send %d bytes to %d, buf cnt %d, idx cnt %d, rv %d\n", cnt, sfd, sbuf->mCnt, sbuf->mExp, rv);
    return rv;
}

void Sender::run()
{
    Packet * ptr;
    SocketBuf * sbuf;
    Queue<Packet> * queue;
    QueueBatcher<Packet> * batcher;
    QueueBatcher<Packet> inBatcher(mpInQueue, 1);

    // counter
    int counterQueue = counter.add("queue");

    int size = 0;
    mByteCnt = 0;
    mPacketCnt = 0;
    mStart = time(NULL);
    assert(mStart != -1);
    int processed = 0;
   
    mCheckCnt = 0;
    
    int flag = 0;

    while (1) {
        /* Check new available writes */
        for (std::map<int, Queue<Packet> * >::iterator it = mpOutQueues->begin();
                it != mpOutQueues->end(); ++it) {
            queue = it->second;
            processed = 0;
            int rv = 0;
            int rr = 0;
            while (rv == 0 && rr == 0 && !queue->isEmpty()) {
                batcher = mpBatchers[it->first];
                sbuf = mpBufs[it->first];
                
                ptr = batcher->probReadSlot();
                int psize = ptr->marshalSize();
                Logger::out(2, "Sender: pid %d, pkt size %d\n", ptr->mPid, psize);
                assert(psize < Const::NETWORK_BUF_SIZE); 
                if (psize + sbuf->mCnt <= Const::NETWORK_BUF_SIZE) {
                    if ((ptr->mPid == PID_VALID_REQ || ptr->mPid == PID_VALID_SI_REQ)
                            && ptr->mReadCnt == 0 && ptr->mWriteCnt == 0) {
                        Packet * in = inBatcher.getWriteSlot();

                        // mark when a packet is taken out of a queue
                        in->timer.mark();
                        // update counter
                        counter.update(counterQueue, in->timer.lap(0));

                        in->set(PID_VALID_RSP, ptr->sid, ptr->mXid);
                        in->mSfd = it->first;
                        in->mDec = DEC_COMMIT;
                        inBatcher.putWriteSlot(in);
//                        fprintf(stdout, "sender seq %lu\n", ptr->mSeqnum);
                    }

                    ptr->marshal(sbuf->mBuf + sbuf->mCnt, &size);
                    assert(psize == size);
                    sbuf->mCnt += psize;
                    /* pop and put back the read slot */
                    batcher->getReadSlot();
                    batcher->putReadSlot(ptr);

                    if (sbuf->mCnt - sbuf->mExp > Const::NETWORK_SEND_BATCH_SIZE) {
                        rv = sendBuf(it->first, sbuf);
                        ++mFullSend;
                        ++flag;
                    }
                    ++mPacketCnt;     
                    ++processed;
                } else {
                    rv = -1;
                }
                if (processed != 0)
                    ++mCheckCnt;
                /* round robin */
                if (Const::NETWORK_PROCESS_BATCH_NUM != 0 
                    && processed == Const::NETWORK_PROCESS_BATCH_NUM) {
                    rr = -1;
                }
            }
        }
        /* Check remaining writes */
        for (std::map<int, Queue<Packet> *>::iterator it = mpOutQueues->begin();
                it != mpOutQueues->end(); ++it) {
            sbuf = mpBufs[it->first];
            if (sbuf->mExp < sbuf->mCnt) {
                sendBuf(it->first, sbuf);
            }
        }

        if (flag == 0) {
            // No send buf called
            //            fprintf(stdout, "sleep\n");
            usleep(Const::NETWORK_SLEEP_TIME_MICRO);
        }
    }
}

void * Sender::runHelper(void * writer)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
 
    ((Sender *) writer)->run();
    pthread_exit(NULL);
}



