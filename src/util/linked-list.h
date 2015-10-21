#ifndef __UTIL_LINKED_LIST_H__
#define __UTIL_LINKED_LIST_H__

#include <assert.h>
#include <pthread.h>
#include <string.h>
#include <time.h>

#include <util/link.h>
#include <util/logger.h>

/**
 * A queue where its element can be accessed by index.
 * Each element of the queue is a pointer to the type.
 * Extend space as it needs.
 * Not thread-safe.
 */
template <class Type>
class LinkedList
{
    public:
        LinkedList(size_t size);
        ~LinkedList();
        Link<Type> * get();
        void put(size_t idx);
        size_t getElmtCnt();

    private:
        std::vector<Link<Type> *> mQueue;
        size_t mSize;
        Link<Type> * mSlotList;
        size_t mElmtCnt;
};

template <class Type>
LinkedList<Type>::LinkedList(size_t size):
    mQueue(size)
{
    mSize = size;
    for (size_t i = 0; i < mSize; ++i) {
        mQueue[i] = new Link<Type>();
        mQueue[i]->mId = i;
        if (i > 0)
            mQueue[i - 1]->mpNext = mQueue[i];
    }
    mSlotList = mQueue[0];
    mElmtCnt = 0;
}

template <class Type>
LinkedList<Type>::~LinkedList()
{
    for (size_t i = 0; i < mSize; ++i)
        delete mQueue[i];
}

template <class Type>
Link<Type> * LinkedList<Type>::get()
{
    if (mSlotList == NULL) {
        /* out of space, allocate 256 slots */
        fprintf(stdout, "LinkedList[%p]: allocate 1024 slots\n", this); fflush(stdout);
        mQueue.resize(mSize + 1024);
        for (int i = mSize; i < mSize + 1024; ++i) {
            mQueue[i] = new Link<Type>();
            mQueue[i]->mId = i;
            if (i > mSize) {
                mQueue[i - 1]->mpNext = mQueue[i];
            }
        }
        mSlotList = mQueue[mSize];
        mSize += 1024;
    }
    Link<Type> * tmp = mSlotList;
    mSlotList = mSlotList->mpNext;
    ++mElmtCnt;
    return tmp;
}

template <class Type>
void LinkedList<Type>::put(size_t idx)
{
    mQueue[idx]->mpNext = mSlotList;
    mSlotList = mQueue[idx];
    --mElmtCnt;
}

template <class Type>
size_t LinkedList<Type>::getElmtCnt()
{
    return mElmtCnt;
}

#endif // __UTIL_LINKED_LIST_H__
