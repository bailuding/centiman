#ifndef __PROCESSOR_WATER_MARK_LIST_H__
#define __PROCESSOR_WATER_MARK_LIST_H__

#include <util/const.h>
#include <util/type.h>

struct WaterMarkNode
{
    Seqnum mVal;
    WaterMarkNode * mNext;
    WaterMarkNode(): mVal(Const::SEQNUM_NULL), mNext(NULL) {}
};

struct WaterMarkList
{
    WaterMarkNode * mElmts;
    WaterMarkNode * mLatest;
    WaterMarkNode * mElmtList;
    WaterMarkNode * mSlotList;

    int mSize;
    Seqnum mLast;
    int mSlotCnt;
    int mElmtCnt;

    WaterMarkList(int size): mLatest(NULL), mElmtList(NULL)
    {
        mSize = size;
        mElmts = new WaterMarkNode[mSize] ();
        for (int i = 0; i < mSize - 1; ++i) {
            mElmts[i].mNext = &(mElmts[i + 1]);
        }
        mSlotList = mElmts;
        mLast = Const::SEQNUM_NULL;
        mSlotCnt = mSize;
        mElmtCnt = 0;
    }

    ~WaterMarkList()
    {
        delete[] mElmts;
    }

    /* insert a val that is larger than the latest one in the list */
    int insert(Seqnum val)
    {
//        fprintf(stdout, "WMINS %04X\n", val);
        if (mSlotList != NULL) {
            /* insert */
            WaterMarkNode * tmp = mSlotList;
            mSlotList = mSlotList->mNext;
            tmp->mVal = val;
            tmp->mNext = NULL;
            if (mLatest != NULL) {
                mLatest->mNext = tmp;
            } else {
                mElmtList = tmp;
            }
            mLatest = tmp;
//            print();
//            ++mElmtCnt;
//            --mSlotCnt;
//            assert(slotCnt() == mSlotCnt);
//            assert(elmtCnt() == mElmtCnt);
            return 0;
        }
        /* no space */
        assert(0);
        return -1;
    }

    /* remove a val that is close to the tail of the list */
    int remove(Seqnum val)
    {
//        fprintf(stdout, "WMRM %04X\n", val);
        WaterMarkNode * prev = NULL;
        WaterMarkNode * cur = mElmtList;
        while (cur != NULL) {
            if (cur->mVal == val) {
                if (cur == mElmtList) {
                    mLast = cur->mVal;
                    mElmtList = mElmtList->mNext;
                    /* update latest */
                    if (mElmtList == NULL) 
                        mLatest = NULL;
                } else {
                    /* prev != NULL */
                    prev->mNext = cur->mNext;
                    /* update latest */
                    if (cur == mLatest) {
                        mLatest = prev;
                    }
                }
                cur->mNext = mSlotList;
                mSlotList = cur;
//                --mElmtCnt;
//                ++mSlotCnt;
//                assert(elmtCnt() == mElmtCnt);
//                assert(slotCnt() == mSlotCnt);
//                print();
                return 0;
            } else {
                prev = cur;
                cur = cur->mNext;
            }
        }
        assert(0);
        return -1;
    }

    /* get the smallest val in the list */
    Seqnum getTail()
    {
        return mLast == Const::SEQNUM_NULL ? 0: mLast;
    }

    void print()
    {
        fprintf(stdout, "WaterMark:");
        WaterMarkNode * tmp = mElmtList;
        while (tmp != NULL) {
            fprintf(stdout, " %04X", tmp->mVal);
            tmp = tmp->mNext;
        }
        fprintf(stdout, "\n");
    }

    int slotCnt()
    {
        int cnt = 0;
        WaterMarkNode * tmp = mSlotList;
        while (tmp != NULL) {
            ++cnt;
            tmp = tmp->mNext;
        }
        return cnt;
    }

    int elmtCnt()
    {
        int cnt = 0;
        WaterMarkNode * tmp = mElmtList;
        while (tmp != NULL) {
            ++cnt;
            tmp = tmp->mNext;
        }
        return cnt;
    }
};

#endif 
