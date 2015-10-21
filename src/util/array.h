#ifndef __UTIL_ARRAY_H__
#define __UTIL_ARRAY_H__

#include <assert.h>
#include <vector>

#include <string.h>

/**
 * Array without worring about boundaries
 * Expected to be used for any type with the assignment operator. 
 */
template <class Type>
class Array
{
    public:
        Array();
        ~Array();
        void set(size_t idx, Type * ptr);
        void fillAndSet(size_t idx, Type * ptr);
        void set(size_t idx, const Type & tmp);
        void fillAndSet(size_t idx, const Type & tmp);
        Type * get(size_t idx);
        Type * fillAndGet(size_t idx);
        void fill(size_t idx);

        std::vector<Type *> mVec;

};

template <class Type>
Array<Type>::Array()
{
    mVec.clear();
}

template <class Type>
Array<Type>::~Array()
{
    for (size_t i = 0; i < mVec.size(); ++i) {
        delete mVec[i];
    }
    mVec.clear();
}

/**
 * Get an element at idx. Fill holes with pointers to new objects if there are
 * any.
 * Expected usage expands the array incrementally.
 */
template <class Type>
inline
Type * Array<Type>::get(size_t idx)
{
    assert(mVec.size() > idx);
    if (mVec[idx] == NULL) {
        mVec[idx] = new Type();
    }
    return mVec[idx];
}

template <class Type>
inline
Type * Array<Type>::fillAndGet(size_t idx)
{
    fill(idx);
    return get(idx);
}

/**
 * Set an element at idx. Fill holes with pointers to new objects if there are
 * any.
 * Expected usage expands the array incrementally.
 */
template <class Type>
inline
void Array<Type>::set(size_t idx, const Type & tmp)
{
    assert(mVec.size() > idx);
    if (mVec[idx] == NULL) {
        mVec[idx] = new Type();
    }
    *mVec[idx] = tmp;
}

template <class Type>
inline
void Array<Type>::set(size_t idx, Type * tmp)
{
    assert(mVec.size() > idx);
    mVec[idx] = tmp;
}


template <class Type>
inline
void Array<Type>::fillAndSet(size_t idx, const Type & tmp)
{
    fill(idx);
    set(idx, tmp);
}

template <class Type>
inline
void Array<Type>::fillAndSet(size_t idx, Type * tmp)
{
    fill(idx);
    set(idx, tmp);
}

template<class Type>
inline
void Array<Type>::fill(size_t idx)
{
    for (size_t i = mVec.size(); i <= idx; ++i) {
        mVec.push_back(NULL);
    }
}

#endif // __UTIL_ARRAY_H__
