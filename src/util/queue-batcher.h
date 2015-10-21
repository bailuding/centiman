#ifndef __UTIL_QUEUE_BATCHER_H__
#define __UTIL_QUEUE_BATCHER_H__

#include <util/queue.h>

/*
 * Batched queue abstraction for a single thread. 
 * A QueueBatcher is used for either get/put read slots, or get/put write slots,
 * but not both. 
 */
template <class Type>
class QueueBatcher
{
    public:
        QueueBatcher(Queue<Type> * pQueue, size_t batchSize);
        QueueBatcher(Queue<Type> * pQueue, size_t getBatchSize, size_t putBatchSize);
        ~QueueBatcher();
        Type * getReadSlot();
        Type * probReadSlot();
        void putReadSlot(Type *);
        Type * getWriteSlot();
        void putWriteSlot(Type *);
    private:
        Queue<Type> * mpQueue;
        
        size_t mGetBatchSize;
        size_t mPutBatchSize;
       
        size_t mGetCnt;
        
        size_t mGetIdx;
        size_t mPutIdx;

        Type ** mGetElmts;
        Type ** mPutElmts;

        void init();

        size_t mGetSlotCnt;
        size_t mGetSlotTime;
};

template <class Type>
QueueBatcher<Type>::QueueBatcher(Queue<Type> * pQueue, size_t batchSize)
{
    mpQueue = pQueue;
    mGetBatchSize = batchSize;
    mPutBatchSize = batchSize;
    init();
}

template <class Type>
QueueBatcher<Type>::QueueBatcher(Queue<Type> * pQueue, size_t getBatchSize, size_t putBatchSize)
{
    assert(getBatchSize >= putBatchSize);
    mpQueue = pQueue;
    mGetBatchSize = getBatchSize;
    mPutBatchSize = putBatchSize;
    init();
}

template <class Type>
void QueueBatcher<Type>::init()
{
    mGetCnt = 0;
    mGetIdx = 0;
    mPutIdx = 0;
    mGetElmts = new Type *[mGetBatchSize];
    mPutElmts = new Type *[mPutBatchSize];
    mGetSlotCnt = 0;
    mGetSlotTime = 0;
}

template <class Type>
QueueBatcher<Type>::~QueueBatcher()
{
    delete[] mGetElmts;
    delete[] mPutElmts;
}

template <class Type>
inline
Type * QueueBatcher<Type>::getReadSlot()
{
    if (mGetIdx < mGetCnt) {
        return mGetElmts[mGetIdx++];
    } else {
        mGetCnt = mpQueue->getReadSlots(mGetElmts, mGetBatchSize);
        mGetIdx = 1;
        mGetSlotCnt += mGetCnt;
        mGetSlotTime++;
        return mGetElmts[0];
    }
}

template <class Type>
inline
Type * QueueBatcher<Type>::probReadSlot()
{
    if (mGetIdx < mGetCnt) {
        return mGetElmts[mGetIdx];
    } else {
        mGetCnt = mpQueue->getReadSlots(mGetElmts, mGetBatchSize);
        mGetIdx = 0;
        mGetSlotCnt += mGetCnt;
        mGetSlotTime++;
        return mGetElmts[0];
    }
}

template <class Type>
inline
void QueueBatcher<Type>::putReadSlot(Type * elmt)
{
    mPutElmts[mPutIdx] = elmt;
    if (++mPutIdx == mPutBatchSize) {
        mpQueue->putReadSlots(mPutElmts, mPutBatchSize);
        mPutIdx = 0;
    }
}

template <class Type>
inline
Type * QueueBatcher<Type>::getWriteSlot()
{
    if (mGetIdx < mGetCnt) {
        return mGetElmts[mGetIdx++];
    } else {
        mGetCnt = mpQueue->getWriteSlots(mGetElmts, mGetBatchSize);
        mGetIdx = 1;
        mGetSlotCnt += mGetCnt;
        mGetSlotTime++;
        return mGetElmts[0];
    }
}

template <class Type>
inline
void QueueBatcher<Type>::putWriteSlot(Type * elmt)
{
    mPutElmts[mPutIdx] = elmt;
    if (++mPutIdx == mPutBatchSize) {
        mpQueue->putWriteSlots(mPutElmts, mPutBatchSize);
        mPutIdx = 0;
    }
}

#endif // __UTIL_QUEUE_BATCHER_H__
