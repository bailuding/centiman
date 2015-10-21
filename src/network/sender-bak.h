#ifndef __NETWORK_SENDER_H__
#define __NETWORK_SENDER_H__

#include <map>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>


#include "util/counter.h"
#include <network/socket-buf.h>
#include <processor/processor.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <util/logger.h>
#include <util/util.h>

class Processor;

class Sender
{
    public:
        Sender(std::map<int, Queue<Packet> *> * pQueues);
        ~Sender();
        void resetStat();
        void setId(int id);
        void setInQueue(Queue<Packet> *);
        
        int getByteCnt();
        int getPacketCnt();
        int getSendCnt();
        int getFullSend();
        int getFullPacketSend();
        int getCheckCnt();

        void run();
        static void * runHelper(void *);

        Counter counter;
    private:
        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;
        SocketBuf ** mpBufs;
        QueueBatcher<Packet> ** mpBatchers;

        int mId;
        Processor * mpProcessor;

        int mByteCnt;
        int mPacketCnt;

        int mSendCnt;
        int mFullSend;
        int mFullPacketSend;

        int mCheckCnt;

        time_t mStart;

        int sendBuf(int, SocketBuf *);
};

#endif // __NETWORK_SENDER_H__

