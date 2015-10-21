#include "network/socket.h"

int Socket::accept(int servfd)
{
    struct sockaddr in_addr;
    socklen_t in_len;
    int sfd;
    char hbuf[NI_MAXHOST];
    char sbuf[NI_MAXSERV];

    in_len = sizeof in_addr;
    if ((sfd = ::accept(servfd, &in_addr, &in_len)) == -1) {
//        perror("SocketAccept: accept");
        return -1;
    }

    int s = getnameinfo(&in_addr, in_len, hbuf, sizeof hbuf, sbuf, sizeof sbuf, NI_NUMERICHOST | NI_NUMERICSERV);
    if (s == 0) {
        Logger::out(1, "SocketAccept: accept connection on descriptor %d (host = %s, port = %s)\n", sfd, hbuf, sbuf);
    }

    if (Socket::set(sfd) == -1)
        return -1;
    
    return sfd;
}

int Socket::bind(const char * port)
{
    int sfd;
    struct addrinfo hints, *servinfo, *p;
    int rv;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if ((rv = getaddrinfo(NULL, port, &hints, &servinfo)) != 0) {
        Logger::err(1, "SocketBind: getaddrinfor: %s\n", gai_strerror(rv));
        return -1;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;
        if (::bind(sfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sfd);
            continue;
        }
        break;
    }
    
    if (p == NULL) {
        perror("SocketBind: could not bind\n");
        return -1;
    }
   
    char hbuf[NI_MAXHOST];
    char sbuf[NI_MAXSERV];
    int s = getnameinfo(p->ai_addr, sizeof (sockaddr), hbuf, sizeof hbuf, sbuf, sizeof sbuf, NI_NUMERICHOST | NI_NUMERICSERV);
    if (s == 0) {
        Logger::out(1,"SocketBind: binding to %s:%s on descriptor %d\n", hbuf, sbuf, sfd);
    }

    freeaddrinfo(servinfo);

    return sfd;
}

int Socket::connect(const char * hostname, const char * port)
{
    int sfd;
    struct addrinfo hints, * servinfo, *p;
    int rv;
    char s[INET6_ADDRSTRLEN];
    
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(hostname, port, &hints, &servinfo)) != 0) {
        Logger::err(1, "SocketConnect: getaddrinfo: %s\n", gai_strerror(rv));
        return -1;
    }

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;
        if (::connect(sfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sfd);
            continue;
        }
        break;
    }
    if (p == NULL) {
        Logger::err(1, "SocketConnect: failed to connect\n");
        return -1;
    }
    
    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof s);
    Logger::out(1, "SocketConnect: connecting to %s on descriptor %d\n", s, sfd);

    freeaddrinfo(servinfo);

    return sfd;
}

int Socket::set(int sfd)
{
    /* Set non blocking */
    int flags = fcntl(sfd, F_GETFL, 0);
    if (flags == -1) {
        perror("SocketSet: get fcntl");
        return -1;
    }
    flags |= O_NONBLOCK;
    if (fcntl(sfd, F_SETFL, flags)) {
        perror("SocketSet: set fcntl");
        return -1;
    }

    /* Set TCP no delay */
    if (Const::NETWORK_TCP_NODELAY == 1) {
        if (setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (char *)&flags, sizeof(flags)) == -1) {
            perror("SocketSet: TPC_NODELAY");
            return -1;
        }
    }
    
    /* Set address reusable */
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof(flags)) == -1) {
        perror("SocketSet: SO_REUSEADDR");
        return -1;
    }

    /* Set linger time */
    struct linger linger;
    linger.l_onoff = 1;
    linger.l_linger = 0;

    if (setsockopt(sfd, SOL_SOCKET, SO_LINGER, &linger, sizeof(linger)) == -1) {
        perror("SocketSet: SO_LINGER");
        return -1;
    }

    return 0;
}

void Socket::startSender(Sender * sender, pthread_t * pThread)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    int rc;
    
    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    if (CodeHandler::print("pthreadAttr", rc))
        exit(-1);

    rc = pthread_create(pThread, &attr, Sender::runHelper, (void *) sender);
    if (CodeHandler::print("pthreadCreate", rc)) {
        exit(-1);
    }

    pthread_attr_destroy(&attr);
}

int Socket::processBulk(const char * id, int sfd, QueueBatcher<Packet> * pBatcher, SocketBuf * socketBuf, int * byteCnt, int * packetCnt, int * recvCnt, int * fullRecv)
{
    int cnt = 0;
    int size = sizeof(uint16_t);
    int flags = 0;
    int processed = 0;
    cnt = recv(sfd, socketBuf->mBuf + socketBuf->mCnt, Const::NETWORK_BUF_SIZE - socketBuf->mCnt, flags);
    * recvCnt += 1;
    if (cnt == -1)
        return -1;
    if (cnt == 0)
        return processed;

    if (cnt == Const::NETWORK_BUF_SIZE - socketBuf->mCnt) {
        *fullRecv += 1;
    }

    *byteCnt += cnt;
    socketBuf->mCnt += cnt;
    int idx = 0;
    int msg = 0;

    /* process bytes */
    while (1) {
        if (socketBuf->mCnt - idx >= size) {
            msg = *((uint16_t *)(socketBuf->mBuf + idx));
        } else {
            break;
        }
        if (socketBuf->mCnt - idx >= msg) {
            Logger::out(2, "Socket: msgsz %d\n", msg);
            Packet * ptr = pBatcher->getWriteSlot();
            ptr->unmarshal(socketBuf->mBuf + idx, msg);
            Logger::out(2, "Socket: get, pid %u, xid %u, seq %lu\n", ptr->mPid, ptr->mXid, ptr->mSeqnum);
            ptr->mSfd = sfd;

            // mark the packet when it is put into a queue
            ptr->timer.mark(0);

            pBatcher->putWriteSlot(ptr);
            Logger::out(2, "Socket: put, pid %u, xid %u, seq %lu\n", ptr->mPid, ptr->mXid, ptr->mSeqnum);
            ++processed;
            *packetCnt += 1; 
            idx += msg;
        } else {
            break;
        }
    }

    memmove(socketBuf->mBuf, socketBuf->mBuf + idx, socketBuf->mCnt - idx);
    socketBuf->mCnt -= idx;
    return processed;
}


void * Socket::get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


