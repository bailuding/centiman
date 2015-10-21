#ifndef __UTIL_ARRAY_BYTES_H__
#define __UTIL_ARRAY_BYTES_H__

#include <vector>

#include <util/bytes.h>

/**
 * Array of Bytes without worrying about boundaries
 */
class ArrayBytes
{
    public:
        ArrayBytes();
        ~ArrayBytes();
        void set(size_t idx, Bytes * ptr);
        void fillAndSet(size_t idx, Bytes * ptr);
        void set(size_t idx, uint8_t * val, uint16_t cnt);
        void fillAndSet(size_t idx, uint8_t * val, uint16_t cnt);
        Bytes * get(size_t idx);
        Bytes * fillAndGet(size_t idx);
        void fill(size_t idx);
        size_t mCnt;
        std::vector<Bytes *> mVec;
};

inline
ArrayBytes::ArrayBytes()
{
    mCnt = 0;
    mVec.clear();
}

inline
ArrayBytes::~ArrayBytes()
{
    for (size_t i = 0; i < mVec.size(); ++i) {
        delete mVec[i];
    }
    mVec.clear();
}

/**
 * Set an element at idx. Fill holes with NULL pointers if there are
 * any.
 * Expected usage expands the array incrementally.
 */
inline
void ArrayBytes::set(size_t idx, Bytes * ptr)
{
    assert(mVec.size() > idx);
    mVec[idx] = ptr;
}

inline
void ArrayBytes::fillAndSet(size_t idx, Bytes * ptr)
{
    fill(idx);
    set(idx, ptr);
}

/**
 * Set an element at idx. Fill holes with pointers to new objects if there are
 * any.
 * Expected usage expands the array incrementally.
 */
inline
void ArrayBytes::set(size_t idx, uint8_t * val, uint16_t cnt)
{    
    assert(mVec.size() > idx);
    if (mVec[idx] == NULL) {
        mVec[idx] = new Bytes();
    }
//    assert(val != NULL);
//    assert(cnt > 0);
    mVec[idx]->copyFrom(val, cnt);
}

inline
void ArrayBytes::fillAndSet(size_t idx, uint8_t * val, uint16_t cnt)
{
    assert(cnt > 0);
    fill(idx);
    set(idx, val, cnt);
}


/**
 * Get an element at idx. Fill holes with pointers to new objects if there are
 * any.
 * Expected usage expands the array incrementally.
 */
inline
Bytes * ArrayBytes::get(size_t idx)
{   
    assert(mVec.size() > idx);
    if (mVec[idx] == NULL) {
        mVec[idx] = new Bytes();
    }
    return mVec[idx];
}

inline
Bytes * ArrayBytes::fillAndGet(size_t idx)
{
    fill(idx);
    if (mVec[idx] == NULL) {
        mVec[idx] = new Bytes();
    }
    return mVec[idx];
}

inline
void ArrayBytes::fill(size_t idx)
{
    for (size_t i = mVec.size(); i <= idx; ++i) {
        mVec.push_back(NULL);
    }
}

#endif // __UTIL_ARRAY_BYTES_H__
