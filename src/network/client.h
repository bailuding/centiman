#ifndef __NETWORK_CLIENT_H__
#define __NETWORK_CLIENT_H__

#include <errno.h>
#include <map>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <network/socket.h>

#include "util/counter.h"
#include <util/code-handler.h>
#include <util/logger.h>
#include <network/socket-buf.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/queue-batcher.h>
#include "util/config.h"

class Processor;
class Sender;

class Client
{
    public:
        Client(int id, Config * config, Queue<Packet> * pInQueue, Queue<Packet> ** pOutQueues, 
              //  char ** hostnames, char ** ports, 
                int serverCnt, std::map<int, int> * pSfds);
        ~Client();
        void publishStat(const char * msg);
        int cancel();
        void run();
        static void * runHelper(void *);
   private:
        int mId;
        Config * config;
        Queue<Packet> * mpInQueue;
        Queue<Packet> ** mpOutQueues;
       // char ** mHostnames;
       // char ** mPorts;
        int mServerCnt;
        std::map<int, int> * mpSfds;

        std::map<int, Queue<Packet> *> * mpOutQueueMap;

        std::map<int, SocketBuf *> mBufs;
        Sender * mpSender;

        struct epoll_event * mEvents;

        int mByteCnt;
        int mPacketCnt;
        int mRecvCnt;
        int mFullRecv;
        time_t mStart;

        QueueBatcher<Packet> mBatcher;

        pthread_t mSenderThread;

        void resetStat();

        Counter counter;

        friend class Processor;
};

#endif // __NETWORK_CLIENT_H__
