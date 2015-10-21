#ifndef __STORAGE_STORAGE_HASH_TABLE_H__
#define __STORAGE_STORAGE_HASH_TABLE_H__

#include <stdio.h>

#include <util/const.h>
#include <util/index.h>
#include <storage/storage-hash-node.h>
#include <util/linked-list.h>
#include <util/type.h>

class StorageHashTable 
{
    public:
        StorageHashTable(uint32_t size);
        ~StorageHashTable();
        int get(const Key &, Value *, Seqnum *);
        int get(const Key &, const Seqnum &, Value *, Seqnum *);
        int put(const Key &, const Value &, const Seqnum &);
        uint32_t getHashIdx(const Key *) const;
        uint32_t mRec;
        uint32_t mGetOps;
        uint32_t mPutOps;
        uint32_t mSearch;
        uint32_t mUnique;
    private:
        StorageHashNode ** mTable; 
        uint32_t mSize;
        LinkedList<StorageHashNode> * mpBuf;
};

inline
StorageHashTable::StorageHashTable(uint32_t size)
{
    mRec = 0;
    mGetOps = 0;
    mPutOps = 0;
    mSearch = 0;
    mUnique = 0;
    mSize = size;
    mTable = new StorageHashNode *[mSize];
    for (size_t i = 0; i < mSize; ++i) {
        mTable[i] = NULL;
    }
//    mpBuf = new LinkedList<StorageHashNode> (Const::STORAGE_HASH_BUF_SIZE);
}

inline
StorageHashTable::~StorageHashTable()
{
    for (size_t i = 0; i < mSize; ++i) {
        StorageHashNode * tmp = mTable[i];
        while (tmp != NULL) {
            StorageHashNode * next = tmp->mpNext;
            delete tmp;
            tmp = next;
        }
    }
    delete[] mTable;
}

/**
 * Copy the latest value and seqnum from the hash node
 * Return 0 if the key is not found
 */
inline
int StorageHashTable::get(const Key & key, Value * pValue, Seqnum * pSeqnum) 
{
    ++mGetOps;
    uint32_t idx = Index::getStorageHashIdx(&key) % mSize;
    StorageHashNode * tmp = mTable[idx];
    while (tmp != NULL && !key.isEqual(tmp->mKey)) {
        ++mSearch;
        tmp = tmp->mpNext;
    }
    if (tmp == NULL) {
        pValue->mCnt = 0;
        *pSeqnum = Const::SEQNUM_NULL;
        return 0;
    } else {
        int rv = tmp->latest(pValue, pSeqnum);
        assert(rv != -1);
        assert(pValue->mCnt > 0);
        return 1;
    }
}

/**
 * Copy the latest value and seqnum from the hash node for a given readSeqnum
 * Return 0 if the key is not found
 * Return 1 if get the key successfully
 * Return -1 if no record that has a smaller timestamp than readSeqnum is found
 */
inline
int StorageHashTable::get(const Key & key, const Seqnum & readSeqnum, Value * pValue, Seqnum * pSeqnum) 
{
    ++mGetOps;
    uint32_t idx = Index::getStorageHashIdx(&key) % mSize;
    StorageHashNode * tmp = mTable[idx];
    while (tmp != NULL && !key.isEqual(tmp->mKey)) {
        ++mSearch;
        tmp = tmp->mpNext;
    }
    if (tmp == NULL) {
        pValue->mCnt = 0;
        *pSeqnum = Const::SEQNUM_NULL;
        return 0;
    } else {
        int rv = tmp->latest(readSeqnum, pValue, pSeqnum);
        return rv == -1 ? -1: 1;
    }
}

/**
 * Copy the value and seqnum from the input 
 * Return 1 if the input version is newer than existing one
 * Return 0 if the input version is not newer than existing one
 * Return 2 if the key does not exist
 */
inline
int StorageHashTable::put(const Key & key, const Value & value, const Seqnum & seqnum)
{
    ++mPutOps;
    uint32_t idx = Index::getStorageHashIdx(&key) % mSize;
    StorageHashNode * tmp = mTable[idx];
    if (tmp == NULL) {
        ++mUnique;
    }
    while (tmp != NULL && !key.isEqual(tmp->mKey)) {
        ++mSearch;
        tmp = tmp->mpNext;
    }
    if (tmp == NULL) {
        StorageHashNode * node = new StorageHashNode(key, value, seqnum, mTable[idx]);
        mTable[idx] = node;
        ++mRec;
        return 2;
    }
    return tmp->insert(value, seqnum);
}
#endif // __STORAGE_HASH_TABLE_H__
