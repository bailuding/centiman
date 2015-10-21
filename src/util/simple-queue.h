#ifndef __UTIL_SIMPLE_QUEUE_H__
#define __UTIL_SIMPLE_QUEUE_H__

#include <util/logger.h>

/**
 * A non-thread-safe, fix-size resuable queue
 */
template <class Type>
class SimpleQueue
{
    public:
        SimpleQueue(size_t size);
        ~SimpleQueue();
        Type * getSlot(); 
        void putSlot(Type *);
        bool isReuse();
        
        Type ** mQueue;
        size_t mSize;
        size_t mCnt;
        size_t mSlotIdx;
        void inc(size_t &);
};

template <class Type>
SimpleQueue<Type>::SimpleQueue(size_t size)
{
    mSize = size;
    mCnt = 0;
    mSlotIdx = 0;
    mQueue = new Type *[mSize];
    for (size_t i = 0; i < mSize; ++i)
        mQueue[i] = new Type();
}

template <class Type>
SimpleQueue<Type>::~SimpleQueue()
{
    for (size_t i = 0; i < mSize; ++i)
        delete mQueue[i];
    delete[] mQueue;
}

template <class Type>
Type * SimpleQueue<Type>::getSlot()
{
    size_t tmp = mSlotIdx;
    inc(mSlotIdx);
    ++mCnt;
    return mQueue[tmp];
}

template <class Type>
bool SimpleQueue<Type>::isReuse()
{
    return mCnt > mSize;
}

template <class Type>
void SimpleQueue<Type>::inc(size_t & val)
{
    ++val;
    if (val == mSize)
        val = 0;
}

#endif // __UTIL_SIMPLE_QUEUE_H__

