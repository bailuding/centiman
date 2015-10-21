#ifndef __VALIDATOR_HASH_TABLE_H__
#define __VALIDATOR_HASH_TABLE_H__

#include <util/linked-list.h>
#include <validator/hash-node.h>

#include <util/index.h>
#include <util/const.h>

class HashTable
{
    public:
        HashTable(size_t size, size_t bufSize);
        ~HashTable();
        HashNode * getHashNode(const Key * pKey, size_t idx, int & searchOp);
        Seqnum get(const Key * pKey, size_t idx, int & searchOp);
        Seqnum get(const Key * pKey, size_t idx, Seqnum readSeqnum, int & searchOp);
        void put(const Key * pKey, size_t idx, Seqnum seqnum, int & searchOp);
        int del(const Key * pKey, size_t idx, int & searchOp);
//    private:
        size_t mSize;
        size_t mBufSize;
        HashNode ** mpTable;
        LinkedList<HashNode> * mpBuf;
        LinkedList<SeqnumNode> * mpSeqBuf;
    friend class ValidatorWorker;
};

inline
HashTable::HashTable(size_t size, size_t bufSize)
{
    mSize = size;
    mBufSize = bufSize;
    mpTable = new HashNode *[mSize];
    for (size_t i = 0; i < mSize; ++i)
        mpTable[i] = NULL;
    mpBuf = new LinkedList<HashNode>(bufSize);
    mpSeqBuf = new LinkedList<SeqnumNode>(bufSize);
}

inline
HashTable::~HashTable()
{
    delete[] mpTable;
    delete mpBuf;
    delete mpSeqBuf;
}

inline
HashNode * HashTable::getHashNode(const Key * pKey, size_t idx, int & searchOp)
{
    HashNode * tmp = mpTable[idx];
    while (tmp != NULL) {
        ++searchOp;
        if (tmp->mpKey->isEqual(*pKey)) {
            break;
        }
        tmp = tmp->mpNext;
    }
    return tmp;
}

inline
Seqnum HashTable::get(const Key * pKey, size_t idx, int & searchOp)
{
    idx %= mSize;
    HashNode * tmp = getHashNode(pKey, idx, searchOp);
    if (tmp != NULL) {
        return tmp->mpHead->mSeqnum;
    } else
        return Const::SEQNUM_NULL;
}

inline
Seqnum HashTable::get(const Key * pKey, size_t idx, Seqnum readSeqnum, int & searchOp)
{
    idx %= mSize;
    HashNode * tmp = getHashNode(pKey, idx, searchOp);
    if (tmp != NULL) {
        SeqnumNode * cur = tmp->mpRear;
        Seqnum seq = Const::SEQNUM_NULL;
        /* find the latest version that is smaller than readSeqnum */
        while (cur != NULL && cur->mSeqnum < readSeqnum) {
            seq = cur->mSeqnum;
            cur = cur->mpPrev;
        }
        return seq;
    } else {
        return Const::SEQNUM_NULL;
    }
}

inline
void HashTable::put(const Key * pKey, size_t idx, Seqnum seqnum, int & searchOp)
{
    idx %= mSize;
    HashNode * tmp = getHashNode(pKey, idx, searchOp);
    if (tmp != NULL) {
        tmp->put(seqnum, mpSeqBuf);
    } else {
        Link<HashNode> * tmp = mpBuf->get();
        HashNode * cur = tmp->mpVal;
        cur->reset();
        cur->mId = tmp->mId;
        cur->mpKey->copyFrom(*pKey);
        cur->put(seqnum, mpSeqBuf);
        if (mpTable[idx] != NULL) {
            mpTable[idx]->mpPrev = cur;
        }
        cur->mpNext = mpTable[idx];
        mpTable[idx] = cur;
    }
//    assert(mpBuf->getElmtCnt() <= mpSeqBuf->getElmtCnt());
}


/**
 * Return 0 if delete the oldest seqnum without deleting the key
 * Return 1 if delete the oldest seqnum and deleting the key
 * Return -1 if no such key is found
 */
inline
int HashTable::del(const Key * pKey, size_t idx, int & searchOp)
{
    idx %= mSize;
    int rv = 0;
    HashNode * tmp = getHashNode(pKey, idx, searchOp);
    if (tmp != NULL) {
        if (tmp->delRear(mpSeqBuf) == 0) {
            rv = 0;
        } else {
            /* Delete the key */
            HashNode * prev = tmp->mpPrev;
            HashNode * next = tmp->mpNext;
            if (prev != NULL) {
                prev->mpNext = next; 
            } else {
                mpTable[idx] = next;
            }
            if (next != NULL) {
                next->mpPrev = prev;
            }
            mpBuf->put(tmp->mId);
            rv = 1;
        } 
    } else {
        rv = -1;
    }
//    assert(mpBuf->getElmtCnt() <= mpSeqBuf->getElmtCnt());
    return rv;
}

#endif // __VALIDATOR_HASH_TABLE_H__
