#ifndef __VALDIATOR_REQUEST_BUF_H__
#define __VALIDATOR_REQUEST_BUF_H__

#include <util/type.h>
#include <util/simple-queue.h>
#include "util/logger.h"

class RequestBuf
{
    public:
        RequestBuf(size_t size);
        ~RequestBuf();
        bool getSlot(ArrayKey *& key);
    private:
        SimpleQueue<ArrayKey> * mBuf;
};

inline
RequestBuf::RequestBuf(size_t size)
{
    mBuf = new SimpleQueue<ArrayKey> (size);
    Logger::out(0, "VALIDATOR_REQUEST_BUF_SIZE: %d\n", size);
}

inline
RequestBuf::~RequestBuf()
{
    delete mBuf;
}

inline
bool RequestBuf::getSlot(ArrayKey *& key)
{
    key = mBuf->getSlot();
    return mBuf->isReuse();
}


#endif // __VALIDATOR_REQUEST_BUF_H__
