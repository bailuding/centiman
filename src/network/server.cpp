#include "network/server.h"

Server::Server(const char * port, Queue<Packet> * pInQueue, std::map<int, Queue<Packet> *> * const pOutQueues, int outQueueSize)
{
    mpInQueue = pInQueue;
    mpOutQueues = pOutQueues;
    mOutQueueSize = outQueueSize;
    mPort = port;

    mpSender = NULL;
    mpBufs = NULL;

    mEvents = (epoll_event *) calloc(Const::NETWORK_MAX_EVENTS, sizeof(epoll_event));
    memset(mEvents, 0, sizeof mEvents);
    
    if (Const::MODE_TPCC) {
        mpBatcher = new QueueBatcher<Packet>(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE * (Const::PROCESSOR_NUM + 1) / 2);
    } else {
        mpBatcher = new QueueBatcher<Packet>(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE * Const::PROCESSOR_NUM);
    }
    mpBufs = new SocketBuf *[Const::MAX_NETWORK_FD] ();
    resetStat();
}

Server::~Server()
{
    for (int i = 0; i < Const::MAX_NETWORK_FD; ++i) {
        if (mpBufs[i] != NULL)
            close(i);
        delete mpBufs[i];
    }
    delete[] mpBufs;

    delete mpSender;

    free(mEvents);
    close(mSfd);
    
    delete mpBatcher;

    Logger::out(1, "Server: Destroyed\n");
}

void Server::resetStat()
{
    mByteCnt = 0;
    mPacketCnt = 0;

    mRecvCnt = 0;
    mFullRecv = 0;
}

void Server::publishStat(const char * msg)
{
    double diff = time(NULL) - mStart;
    double mb = 1024 * 1024;
    
    if (mpSender == NULL)
        return;
    Logger::out(0, "Server[%s]: run %.0f | "
                    "Packet %d %d | "
                    "Recv %.0f %.0f | "
                    "Send %.0f %.0f | "
                    "Recv() %.0f %.0f %d | "
                    "Send() %.0f %.0f %.0f | "
                    "Check %.0f %d\n", 
                    msg, diff,
                    mByteCnt / (mPacketCnt + 1), mpSender->getByteCnt() / (mpSender->getPacketCnt() + 1), 
                    mPacketCnt / diff, mByteCnt / diff / mb, 
                    mpSender->getPacketCnt() / diff, mpSender->getByteCnt() / diff / mb, 
                    mRecvCnt / diff, mFullRecv / diff, mPacketCnt / (mRecvCnt + 1),
                    mpSender->getSendCnt() / diff, mpSender->getFullSend() / diff, mpSender->getFullPacketSend() / diff,
                    mpSender->getCheckCnt() / diff, mpSender->getPacketCnt() / (mpSender->getCheckCnt() + 1));
}

void Server::run()
{
    /* Initialize listening socket */
    if ((mSfd = Socket::bind(mPort)) == -1)
        return;
    if (Socket::set(mSfd) == -1)
        return;
    if (listen(mSfd, SOMAXCONN) == -1) {
        perror("Server: listen");
        return;
    }

    /* Initialize epoll */
    int epfd = epoll_create(Const::NETWORK_EPOLL_SIZE);
    if (epfd == -1) {
        perror("Server: epoll_create");
        return;
    }

    struct epoll_event ev;
    ev.data.fd = mSfd;
    ev.events = EPOLLIN; // Level triger
    int s = epoll_ctl(epfd, EPOLL_CTL_ADD, mSfd, &ev);

    if (s == -1) {
        perror("Server: epoll_ctl");
        return;
    }

    mStart = time(NULL);
    assert(mStart != -1);

    int clientCnt = 0;

    Logger::out(1, "Server[%s]: established server.\n", mPort);
    while (1) {
        Logger::out(1, "Server[%s]: I am waiting for events!\n", mPort);
        int n = epoll_wait(epfd, mEvents, Const::NETWORK_MAX_EVENTS, -1);
        for (int i = 0; i < n; ++i) {
            Logger::out(1, "Server[%s]: I got an event\n", mPort);
            if ((mEvents[i].events & EPOLLERR) ||
                (mEvents[i].events & EPOLLHUP) ||
                (!(mEvents[i].events & EPOLLIN))) {
                Logger::out(1, "Server[%s]: epoll error!\n", mPort);
                close(mEvents[i].data.fd);
            } else if (mSfd == mEvents[i].data.fd) {
                Logger::out(1, "Server[%s]: I got a connection!\n", mPort);
                int sfd;
                while ((sfd = Socket::accept(mSfd)) != -1) {
                    /* Add to epoll */
                    ev.data.fd = sfd;
                    ev.events = EPOLLIN; // Level trigger
                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &ev) == -1) {
                        perror("Server: epoll_ctl");
                        break;
                    }
                    mpBufs[sfd] = new SocketBuf();
                    Queue<Packet> * tmp = new Queue<Packet> (mOutQueueSize);
                    (*mpOutQueues)[sfd] = tmp; 
                    ++clientCnt;
                    /* Spawn Sender */
                    if (clientCnt == Const::PROCESSOR_NUM) {
                        mpSender = new Sender(mpOutQueues);
                        Socket::startSender(mpSender, &mSenderThread);
                        Logger::out(0, "Server[%s]: all clients connected.\n", mPort);
                    }
                    Logger::out(1, "Server[%s]: I added epoll for descriptor %d!\n", mPort, sfd);
                }
            } else {
                Logger::out(2, "Server[%s]: I got a packet!\n", mPort);
                int fd = mEvents[i].data.fd;
                if (Socket::processBulk(mPort, fd, mpBatcher, mpBufs[fd], &mByteCnt, &mPacketCnt, &mRecvCnt, &mFullRecv) == -1 &&
                    errno != EAGAIN && errno != EWOULDBLOCK) {
                    Logger::out(0, "Server: I closed connection on descriptor %d!\n", fd);
                    close(fd);
                }
            }
        }
    }
}

int Server::cancel()
{
    if (CodeHandler::print("pthreadCancel", pthread_cancel(mSenderThread)))
        return -1;
    return 0;
}

void * Server::runHelper(void * server)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    ((Server *) server)->run();
    pthread_exit(NULL);
}


