#include "network/client.h"

Client::Client(int id, 
        Config * config,
        Queue<Packet> * pInQueue, Queue<Packet> ** pOutQueues, 
        //char ** hostnames, char ** ports, 
        int serverCnt, std::map<int, int> * pSfds):
        
        mId(id),
        mpInQueue(pInQueue), mpOutQueues(pOutQueues),
        //mHostnames(hostnames), mPorts(ports),
        mServerCnt(serverCnt), mpSfds(pSfds),

        mBatcher(mpInQueue, Const::UTIL_QUEUE_BATCH_SIZE)

{
    this->config = config;

    mpOutQueueMap = new std::map<int, Queue<Packet> *>();

    mEvents = (epoll_event *) calloc(Const::NETWORK_MAX_EVENTS, sizeof(epoll_event));
    memset(mEvents, 0, sizeof mEvents);

    resetStat();
}

Client::~Client()
{
    for (std::map<int, SocketBuf *>::iterator it = mBufs.begin(); it != mBufs.end(); ++it) {
        close(it->first);
        delete it->second;
    }
   
    delete mpSender;
    delete mpOutQueueMap;

    free(mEvents);
    Logger::out(1, "Client: Destroyed\n");
}

void Client::resetStat()
{
    mByteCnt = 0;
    mPacketCnt = 0;
    mRecvCnt = 0;
    mFullRecv = 0;
}

void Client::publishStat(const char * msg)
{
    time_t finish = time(NULL);
    assert(finish != -1);
    double diff = finish - mStart;
    double mb = 1024 * 1024;
    Logger::out(0, "Client[%s]: run %.0f | "
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

void Client::run() 
{
    int epfd = epoll_create(Const::NETWORK_EPOLL_SIZE);
    if (epfd == -1) {
        perror("Client: epoll_create");
        return;
    }

    struct epoll_event ev;
    int sfd;
    int vnum = config->vIps.size();

    std::cout << "Client:\t" << mServerCnt << " " << vnum << endl;

    for (int i = 0; i < mServerCnt; ++i) {
        pair<string, string> ip = i < vnum ? 
            config->vIps.getIp(i): config->sIps.getIp(i - vnum);
        Logger::out(1, "Client: connecting %s:%s\n", ip.first.c_str(), ip.second.c_str());
        if ((sfd = Socket::connect(ip.first.c_str(), ip.second.c_str())) == -1)
            return;
        if (Socket::set(sfd))
            return;
        /* Add to epoll */
        ev.data.fd = sfd;
        ev.events = EPOLLIN;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &ev) == -1) {
            perror("Client: epoll_ctl");
            break;
        }
        (*mpSfds)[sfd] = i;
        mBufs[sfd] = new SocketBuf();
        (*mpOutQueueMap)[sfd] = mpOutQueues[i];
    }
    
    /* Spawn Sender */
    mpSender = new Sender(mpOutQueueMap);
    mpSender->setId(mId);
    mpSender->setInQueue(mpInQueue);
    Socket::startSender(mpSender, &mSenderThread);
    Logger::out(0, "Client: all server connected.\n");

    mStart = time(NULL);

    while (1) {
        Logger::out(2, "Client: I am waiting for events!\n");
        int n = epoll_wait(epfd, mEvents, Const::NETWORK_MAX_EVENTS, -1);
        for (int i = 0; i < n; ++i) {
            Logger::out(2, "Client: I got an event!\n");
            if ((mEvents[i].events & EPOLLERR) ||
                (mEvents[i].events & EPOLLHUP) ||
                (!(mEvents[i].events & EPOLLIN))) {
                assert(mEvents[i].events & ENOMEM);
                Logger::err(0, "Client: epoll error %d!\n", mEvents[i].events);
                close(mEvents[i].data.fd);
            } else {
                Logger::out(2, "Client: I got a packet!\n");
                int fd = mEvents[i].data.fd;
                if (Socket::processBulk("Client", fd, &mBatcher, mBufs[fd], &mByteCnt, &mPacketCnt, &mRecvCnt, &mFullRecv) == -1 &&
                        errno != EAGAIN && errno != EWOULDBLOCK) {
                    Logger::out(1, "Client: close connection on descriptor %d\n", fd);
                    close(fd);
                }
            }
        }
    }
}

int Client::cancel()
{
    if (CodeHandler::print("pthreadCancel", pthread_cancel(mSenderThread)))
        return -1;
    return 0;
}

 
void * Client::runHelper(void * client)
{
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
 
    ((Client *) client)->run();
    pthread_exit(NULL);
}

