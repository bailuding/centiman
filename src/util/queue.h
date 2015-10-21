#ifndef __UTIL_QUEUE_H__
#define __UTIL_QUEUE_H__

#include <assert.h>
#include <pthread.h>
#include <stdlib.h>

#include <util/logger.h>

/**
 * A thread-safe queue used when the processing is non-trivial.
 * Each element of the queue is a pointer to an object.
 * The Type class needs to provide Type() and reset().
 */
template <class Type>
class Queue
{
    public:
        Queue(int size);
        ~Queue();

        void publish(const char * msg);
        
        Type * getReadSlot();
        int getReadSlots(Type **, int size);
        void putReadSlot(Type *);
        void putReadSlots(Type **, int size);
        Type * getWriteSlot();
        int getWriteSlots(Type **, int size);
        void putWriteSlot(Type *);
        void putWriteSlots(Type **, int size);
        int getReadWaitCnt();
        int getWriteWaitCnt();
        bool isEmpty();
        int getSize();
        int getElmtCnt();

        int mSize;
        
    private:
        Queue() {}
        Queue(const Queue& q) {}

        Type ** mPtrs;
        int mElmtCnt;
        int mSlotCnt;
        int mReadGetSlot;
        int mReadPutSlot;
        int mWriteGetSlot;
        int mWritePutSlot;

        int mWaitCnt;

        pthread_mutex_t mLock;
        pthread_cond_t mReadCond;
        pthread_cond_t mWriteCond;

        void inc(int &val);
};

template <class Type> 
Queue<Type>::Queue(int size)
{
    mSize = size;
    mPtrs = new Type *[mSize];
    for (int i = 0; i < mSize; ++i) {
        mPtrs[i] = new Type();
    }

    mElmtCnt = 0;
    mSlotCnt = size;
    mReadGetSlot = 0;
    mReadPutSlot = 0;
    mWriteGetSlot = 0;
    mWritePutSlot = 0;
    
    mWaitCnt = 0;

    pthread_mutex_init(&mLock, NULL);
    pthread_cond_init(&mReadCond, NULL);
    pthread_cond_init(&mWriteCond, NULL);
}

template <class Type>
Queue<Type>::~Queue()
{
    for (int i = 0; i < mSize; ++i) {
        delete mPtrs[i];
    }
        delete[] mPtrs;

    pthread_mutex_destroy(&mLock);
    pthread_cond_destroy(&mReadCond);
    pthread_cond_destroy(&mWriteCond);
}

template <class Type>
void Queue<Type>::publish(const char * msg)
{
    if (mElmtCnt > 0) {
        Logger::out(0, "Queue[%s]: %d / %d , read wait %d times\n", msg, mElmtCnt, mSize, mWaitCnt);
    }
    mWaitCnt = 0;
}

template <class Type> 
Type * Queue<Type>::getReadSlot()
{
    pthread_mutex_lock(&mLock);
    while (mElmtCnt == 0) {
        ++mWaitCnt;
        pthread_cond_wait(&mReadCond, &mLock);
    }
    Type * tmp = mPtrs[mReadGetSlot];
    --mElmtCnt;
    mPtrs[mReadGetSlot] = NULL;
    inc(mReadGetSlot);
    pthread_mutex_unlock(&mLock);
    return tmp;
}

/*
 * Get a batch of read slots from the queue.
 * If there are not enough slots available, get as much as possible.
 * It will be blocked if no slot is available.
 */
template <class Type>
inline
int Queue<Type>::getReadSlots(Type ** elmts, int size)
{
    pthread_mutex_lock(&mLock);
    while (mElmtCnt == 0) {
        pthread_cond_wait(&mReadCond, &mLock);
    }
    int cnt = mElmtCnt < size ? mElmtCnt: size;
    for (int i = 0; i < cnt; ++i) {
        elmts[i] = mPtrs[mReadGetSlot];
        mPtrs[mReadGetSlot] = NULL;
        inc(mReadGetSlot);
    }
    mElmtCnt -= cnt;
    pthread_mutex_unlock(&mLock);
    return cnt;
}

template <class Type>
inline
void Queue<Type>::putReadSlot(Type * ptr)
{
    pthread_mutex_lock(&mLock);
    mPtrs[mReadPutSlot] = ptr;
    inc(mReadPutSlot);
    ++mSlotCnt;
    pthread_cond_signal(&mWriteCond);
    pthread_mutex_unlock(&mLock);
}

/*
 * Put a batch of read slots to the queue.
 * Assume the queue has enough available slots.
 */
template <class Type>
inline
void Queue<Type>::putReadSlots(Type ** elmts, int size)
{
    pthread_mutex_lock(&mLock);
    assert(size + mSlotCnt <= mSize);
    for (int i = 0; i < size; ++i) {
        mPtrs[mReadPutSlot] = elmts[i];
        inc(mReadPutSlot);
    }
    mSlotCnt += size;
    pthread_cond_signal(&mWriteCond);
    pthread_mutex_unlock(&mLock);
}

template <class Type>
inline
Type * Queue<Type>::getWriteSlot()
{
    pthread_mutex_lock(&mLock);
    while (mSlotCnt == 0) {
        pthread_cond_wait(&mWriteCond, &mLock);
    }
    Type * tmp = mPtrs[mWriteGetSlot];
    tmp->reset();
    --mSlotCnt;
    mPtrs[mWriteGetSlot] = NULL;
    inc(mWriteGetSlot);
    pthread_mutex_unlock(&mLock);
    return tmp;
}

template <class Type>
inline
int Queue<Type>::getWriteSlots(Type ** elmts, int size)
{
    pthread_mutex_lock(&mLock);
    while (mSlotCnt == 0) {
        pthread_cond_wait(&mWriteCond, &mLock);
    }
    int cnt = mSlotCnt < size ? mSlotCnt: size;
    for (int i = 0; i < cnt; ++i) {
        elmts[i] = mPtrs[mWriteGetSlot];
        elmts[i]->reset();
        mPtrs[mWriteGetSlot] = NULL;
        inc(mWriteGetSlot);
    }
    mSlotCnt -= cnt;
    pthread_mutex_unlock(&mLock);
    return cnt;
}

template <class Type>
inline
void Queue<Type>::putWriteSlot(Type * ptr)
{
    pthread_mutex_lock(&mLock);
    mPtrs[mWritePutSlot] = ptr;
    inc(mWritePutSlot);
    ++mElmtCnt;
    pthread_cond_signal(&mReadCond);
    pthread_mutex_unlock(&mLock);
}

template <class Type>
inline
void Queue<Type>::putWriteSlots(Type ** elmts, int size)
{
    pthread_mutex_lock(&mLock);
    assert(size + mElmtCnt <= mSize);
    for (int i = 0; i < size; ++i) {
        mPtrs[mWritePutSlot] = elmts[i];
        inc(mWritePutSlot);
    }
    mElmtCnt += size;
    pthread_cond_signal(&mReadCond);
    pthread_mutex_unlock(&mLock);
}

template <class Type>
inline
void Queue<Type>::inc(int &val)
{
    if (++val == mSize)
        val = 0;
}

template <class Type>
int Queue<Type>::getReadWaitCnt()
{
    return 0;
//    return mReadWaitCnt;
}

template <class Type>
int Queue<Type>::getWriteWaitCnt()
{
    return 0;
//    return mWriteWaitCnt;
}

template <class Type>
inline
bool Queue<Type>::isEmpty()
{
    return mElmtCnt == 0;
}

template <class Type>
inline
int Queue<Type>::getSize()
{
    return mSize;
}

template <class Type>
inline
int Queue<Type>::getElmtCnt()
{
    return mElmtCnt;
}

#endif // __UTIL_QUEUE_H__

