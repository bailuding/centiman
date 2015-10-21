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

// send out bytes from a buffer as much as possible
// return the number of bytes sent
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
    }

    return cnt;
}


void Sender::swipe()
{
    // counter
    static int counterSwipe = counter.add("swipe");

    // check available writes
    for (auto it = mpOutQueues->begin();
            it != mpOutQueues->end(); ++it) {
        sendBuf(it->first, mpBufs[it->first]);
    }

    counter.inc(counterSwipe);
}

void Sender::serialize()
{ 
    Packet * ptr;
    SocketBuf * sbuf;
    Queue<Packet> * queue;
    QueueBatcher<Packet> * batcher;
    QueueBatcher<Packet> inBatcher(mpInQueue, 1);

    // counter
    static int counterQueue = counter.addAvg("nsq");

    int size = 0;
    mByteCnt = 0;
    mPacketCnt = 0;
    mStart = time(NULL);
    assert(mStart != -1);
    int processed = 0;

    mCheckCnt = 0;


    // check available writes
    for (std::map<int, Queue<Packet> * >::iterator it = mpOutQueues->begin();
            it != mpOutQueues->end(); ++it) {
        queue = it->second;
        processed = 0;
        batcher = mpBatchers[it->first];
        sbuf = mpBufs[it->first];

        int rr = 0;
        int rv = 0;
        while (rv == 0 && rr == 0 && !queue->isEmpty()) {

            ptr = batcher->probReadSlot();
            int psize = ptr->marshalSize();
            // check for extremely large message
            assert(psize < Const::NETWORK_BUF_SIZE); 

            // if the message can fit into the buffer
            if (psize + sbuf->mCnt <= Const::NETWORK_BUF_SIZE) {
                // if it is an empty validation request
                // send the response here since validator will not send out
                // response
                if ((ptr->mPid == PID_VALID_REQ || ptr->mPid == PID_VALID_SI_REQ)
                        && ptr->mReadCnt == 0 && ptr->mWriteCnt == 0) {
                    Packet * in = inBatcher.getWriteSlot();
                    
                    in->set(PID_VALID_RSP, ptr->sid, ptr->mXid);
                    in->mSfd = it->first;
                    in->mDec = DEC_COMMIT;

                    // mark the packet when it is put into a queue
                    in->timer.mark(0);

                    inBatcher.putWriteSlot(in);
                }

                // mark the packet when it is taken out of the queue
                ptr->timer.mark(1);
                // update counter
                counter.update(counterQueue, ptr->timer.lap(1));

                ptr->marshal(sbuf->mBuf + sbuf->mCnt, &size);
                assert(psize == size);
                sbuf->mCnt += psize;
                /* pop and put back the read slot */
                batcher->getReadSlot();
                batcher->putReadSlot(ptr);
            } else {
                rv = -1;
                assert(-1);
            }
            if (processed != 0)
                ++mCheckCnt;

            // check other queues if a threshold is set for batch processing
            // and it has reached the threshold
            if (Const::NETWORK_PROCESS_BATCH_NUM != 0 
                    && processed == Const::NETWORK_PROCESS_BATCH_NUM) {
                rr = -1;
            }
        }
    }
}

void Sender::run()
{
    Timer swipeTimer;

    swipeTimer.reset();
    swipeTimer.resize(2);
    swipeTimer.mark(0, true);

    while (1) {
        // serialize packets to bytes
        serialize();
        // mark time
        swipeTimer.mark(1, true);
        // send buffer if enough time has passed
        if (swipeTimer.lapMs(1) >= Const::NETWORK_SWIPE_TIME_MS) {
            // send out all the bytes
            swipe();
            // mark time
            swipeTimer.mark(0, true);
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



