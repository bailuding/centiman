#ifndef __UTIL_INDEX_QUEUE_H__
#define __UTIL_INDEX_QUEUE_H__

#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <time.h>

#include <util/link.h>
#include <util/logger.h>

/**
 * A thread-safe queue where its element can be accessed by index.
 * Each element of the queue is a pointer to the type.
 */
template <class Type>
class IndexQueue
{
    public:
        IndexQueue(size_t size);
        ~IndexQueue();
        Link<Type> * getSlot();
        size_t getSlots(Link<Type> ** elmts, size_t size);
        Type * get(size_t idx);
        void del(size_t idx);
        void dels(size_t * elmts, size_t size);
        size_t getWaitCnt();

    private:
        Link<Type> ** mQueue;
        size_t mSize;
        Link<Type> * mSlotList;

//        size_t mWaitCnt;

        pthread_mutex_t mLock;
        pthread_cond_t mCond;

        size_t mCnt;
};

template <class Type>
IndexQueue<Type>::IndexQueue(size_t size)
{
    mSize = size;
//    mWaitCnt = 0;

    mQueue = new Link<Type> *[mSize];
    for (size_t i = 0; i < mSize; ++i) {
        mQueue[i] = new Link<Type>();
        mQueue[i]->mId = i;
        if (i > 0)
            mQueue[i - 1]->mpNext = mQueue[i];
    }
    mSlotList = mQueue[0];

    pthread_mutex_init(&mLock, NULL);
    pthread_cond_init(&mCond, NULL);

    mCnt = 0;
}

template <class Type>
IndexQueue<Type>::~IndexQueue()
{
    for (size_t i = 0; i < mSize; ++i)
        delete mQueue[i];
    delete[] mQueue;

    Logger::out(2, "IndexQueue: Destroyed at time %d.\n", time(NULL));

    pthread_mutex_destroy(&mLock);
    pthread_cond_destroy(&mCond);
}

template <class Type>
Link<Type> * IndexQueue<Type>::getSlot()
{
    pthread_mutex_lock(&mLock);
    while (mSlotList == NULL) {
        pthread_cond_wait(&mCond, &mLock);
    }
    Link<Type> * tmp = mSlotList;
    mSlotList = mSlotList->mpNext;
    ++mCnt;
    assert(mCnt <= mSize);
    pthread_mutex_unlock(&mLock);
    return tmp;
}

template <class Type>
size_t IndexQueue<Type>::getSlots(Link<Type> ** elmts, size_t size)
{
    pthread_mutex_lock(&mLock);
    while (mSlotList == NULL) {
        pthread_cond_wait(&mCond, &mLock);
    }
    size_t cnt = 0;
    while (cnt < size && mSlotList != NULL) {
        elmts[cnt++] = mSlotList;
        mSlotList = mSlotList->mpNext;
    }
    mCnt += cnt;
    assert(mCnt <= mSize);
    pthread_mutex_unlock(&mLock);
    return cnt;
}

template <class Type>
Type * IndexQueue<Type>::get(size_t idx)
{
    return mQueue[idx]->mpVal;
}

template <class Type>
void IndexQueue<Type>::del(size_t idx)
{
    pthread_mutex_lock(&mLock);
    mQueue[idx]->mpNext = mSlotList;
    mSlotList = mQueue[idx];
    assert(mCnt > 0);
    --mCnt;
    pthread_cond_signal(&mCond);
    pthread_mutex_unlock(&mLock);
}

template <class Type>
void IndexQueue<Type>::dels(size_t * elmts, size_t size)
{
    pthread_mutex_lock(&mLock);
    for (size_t i = 0; i < size; ++i) {
        mQueue[elmts[i]]->mpNext = mSlotList;
        mSlotList = mQueue[elmts[i]];
    }
    assert(mSlotList != NULL);
    assert(mCnt >= size);
    mCnt -= size;
    pthread_cond_signal(&mCond);
    pthread_mutex_unlock(&mLock);
}

template <class Type>
size_t IndexQueue<Type>::getWaitCnt()
{
    return 0;
//    return mWaitCnt;
}

#endif // __UTIL_INDEX_QUEUE_H__
