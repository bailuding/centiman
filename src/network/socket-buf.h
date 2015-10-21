#ifndef __NETWORK_SOCKET_BUF_H__
#define __NETWORK_SOCKET_BUF_H__

#include <stdint.h>
#include <stdio.h>

#include <util/const.h>

class SocketBuf
{
    public:
        SocketBuf()
        {
            mBuf = new uint8_t[Const::NETWORK_BUF_SIZE];
            mExp = 0;
            mCnt = 0;
        }

        ~SocketBuf()
        {
            delete[] mBuf;
        }
        
        uint8_t * mBuf;
        int mExp;
        int mCnt;
};


#endif // __NETWORK_SOCKET_BUF_H__

