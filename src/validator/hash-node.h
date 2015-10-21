#ifndef __VALIDATOR_HASH_NODE_H__
#define __VALIDATOR_HASH_NODE_H__

#include <util/type.h>

#include <validator/seqnum-node.h>
#include "util/const.h"

class HashNode
{
    public:
        Key * mpKey;
        SeqnumNode * mpHead;
        SeqnumNode * mpRear;
        HashNode * mpNext;
        HashNode * mpPrev;
        uint32_t mId;

        HashNode(): mpHead(NULL), mpRear(NULL), mpNext(NULL), mpPrev(NULL), mId(0)
        {
            mpKey = new Key(Const::AVG_WRITE_CNT);
        }

        ~HashNode()
        {
            delete mpKey;
        }

        void reset()
        {
            mId = 0;
            mpNext = NULL;
            mpPrev = NULL;
            mpHead = NULL;
            mpRear = NULL;
        }

        void put(Seqnum seqnum, LinkedList<SeqnumNode> * pSeqBuf)
        {
            Link<SeqnumNode> * slot = pSeqBuf->get();
            SeqnumNode * tmp = slot->mpVal;
            tmp->reset();
            tmp->mId = slot->mId;
            tmp->mSeqnum = seqnum;
            tmp->mpNext = mpHead;
            if (mpHead != NULL)
                mpHead->mpPrev = tmp;
            if (mpRear == NULL)
                mpRear = tmp;
            mpHead = tmp;
        }

        void getItvl(Seqnum seqnum, Seqnum * pLow, Seqnum * pHigh)
        {
            SeqnumNode * tmp = mpHead;
            while (tmp != NULL) {
                if (tmp->mSeqnum <= seqnum) {
                    break;
                }
                tmp = tmp->mpNext;
            }
            if (tmp == NULL) {
                *pLow = seqnum;
                *pHigh = seqnum + 1;
            } else {
                *pLow = tmp->mSeqnum;
                if (tmp->mpPrev != NULL) {
                    *pHigh = tmp->mpPrev->mSeqnum - 1;
                } else {
                    *pHigh = -1;
                }
            }
        }

        /**
         * Return 0 if delete the oldest seqnum
         * Return 1 if no seqnum is left after the deletion
         */
        int delRear(LinkedList<SeqnumNode> * pSeqBuf)
        {
            if (mpRear == mpHead) {
                pSeqBuf->put(mpRear->mId);
                mpRear = mpHead = NULL;
                return 1;
            } else {
                SeqnumNode * tmp = mpRear->mpPrev;
                pSeqBuf->put(mpRear->mId);
                tmp->mpNext = NULL;
                mpRear = tmp;
                return 0;
            }
        }
};

#endif // __VALIDATOR_HASH_NODE_H__
