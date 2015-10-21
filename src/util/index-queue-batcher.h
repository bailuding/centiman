#ifndef __UTIL_INDEX_QUEUE_BATCHER_H__
#define __UTIL_INDEX_QUEUE_BATCHER_H__

#include <util/index-queue.h>
#include <util/link.h>

template <class Type>
class IndexQueueBatcher
{
    public:
        IndexQueueBatcher(IndexQueue<Type> *, size_t batchSize);
        ~IndexQueueBatcher();
        Link<Type> * getSlot();
        void del(size_t idx);
    private:
        IndexQueue<Type> * mpQueue;
        size_t mBatchSize;
        Link<Type> ** mGetElmts;
        size_t * mDelElmts;

        size_t mGetCnt;
        size_t mGetIdx;
        size_t mPutIdx;
};

template <class Type>
IndexQueueBatcher<Type>::IndexQueueBatcher(IndexQueue<Type> * pQueue, size_t batchSize)
{
    mpQueue = pQueue;
    mBatchSize = batchSize;

    mGetElmts = new Link<Type> *[mBatchSize];
    mDelElmts = new size_t[mBatchSize];

    mGetCnt = 0;
    mGetIdx = 0;
    mPutIdx = 0;
}

template <class Type>
IndexQueueBatcher<Type>::~IndexQueueBatcher()
{
    delete[] mGetElmts;
    delete[] mDelElmts;
}

template <class Type>
Link<Type> * IndexQueueBatcher<Type>::getSlot()
{
    if (mGetIdx < mGetCnt) {
        return mGetElmts[mGetIdx++];
    } else {
        mGetCnt = mpQueue->getSlots(mGetElmts, mBatchSize);
        mGetIdx = 1;
        return mGetElmts[0];
    }
}

template <class Type>
void IndexQueueBatcher<Type>::del(size_t idx)
{
    mDelElmts[mPutIdx] = idx;
    if (++mPutIdx == mBatchSize) {
        mpQueue->dels(mDelElmts, mBatchSize);
        mPutIdx = 0;
    }
}

#endif // __UTIL_INDEX_QUEUE_BATCHER_H__

