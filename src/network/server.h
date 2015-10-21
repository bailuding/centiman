#ifndef __NETWORK_SERVER_H__
#define __NETWORK_SERVER_H__

#include <map>
#include <pthread.h>
#include <time.h>
#include <sys/epoll.h>
#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>


#include <network/sender.h>
#include <network/socket-buf.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include <network/socket.h>
#include <util/code-handler.h>
#include <util/logger.h>

class Server
{
    public:
        Server(const char * port, Queue<Packet> * pInQueue, std::map<int, Queue<Packet> *> * const pOutQueues, int outQueueSize);
        ~Server();
        void publishStat(const char * msg);
        int cancel();
        void run();
        static void * runHelper(void *);
    private:
        const char * mPort;
        Queue<Packet> * mpInQueue;
        std::map<int, Queue<Packet> *> * mpOutQueues;
        int mOutQueueSize;
        int mSfd;
       
        SocketBuf ** mpBufs;
        Sender * mpSender;

        struct epoll_event * mEvents; 
        int mByteCnt;
        time_t mStart;
        int mPacketCnt;
        int mRecvCnt;
        int mFullRecv;

        QueueBatcher<Packet> * mpBatcher;

        pthread_t mSenderThread;

        void resetStat();

        friend class Validator;
        friend class Storage;
};


#endif // __NETWORK_SERVER_H__
