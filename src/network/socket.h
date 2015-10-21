#ifndef __NETWORK_SOCKET_H__
#define __NETWORK_SOCKET_H__

#include <map>
#include <pthread.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <network/sender.h>
#include <util/packet.h>
#include <util/queue.h>
#include <util/const.h>
#include <util/code-handler.h>
#include <util/logger.h>
#include <util/queue-batcher.h>

class Sender;

class Socket
{
    public:
        static int accept(int servfd);
        static int bind(const char * port);
        static int connect(const char * hostname, const char * port);
        static int set(int sfd);
        static void startSender(Sender * sender, pthread_t * pThread);
        static int processBulk(const char * id, int sfd, QueueBatcher<Packet> * pBatcher, SocketBuf * socketBuf, int * byteCnt, int * packetCnt, int * recvCnt, int * fullRecv);
    private:
        static void * get_in_addr(struct sockaddr *sa);
};

#endif // __NETWORK_SOCKET_H__
