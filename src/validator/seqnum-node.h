#ifndef __VALIDATOR_SEQNUM_NODE_H__
#define __VALIDATOR_SEQNUM_NODE_H__

class SeqnumNode
{
    public:
        size_t mId;
        Seqnum mSeqnum;
        SeqnumNode * mpNext;
        SeqnumNode * mpPrev;

        SeqnumNode(Seqnum seqnum): mId(0), mSeqnum(seqnum), mpNext(NULL), mpPrev(NULL) {}
        SeqnumNode(): mId(0), mSeqnum(0), mpNext(NULL), mpPrev(NULL) {}

        void reset()
        {
            mId = 0;
            mSeqnum = 0;
            mpNext = NULL;
            mpPrev = NULL;
        }

};

#endif // __VALIDATOR_SEQNUM_NODE_H__

