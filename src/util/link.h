#ifndef __UTIL_LINK_H__
#define __UTIL_LINK_H__

#include <stdint.h>

/**
 * Used for linked list
 */
template <class Type>
class Link
{
    public:
        Link();
        ~Link();
        uint32_t mId;
        Type * mpVal;
        Link * mpNext;
};

template <class Type>
Link<Type>::Link()
{
    mId = 0;
    mpVal = new Type();
    mpNext = NULL;
}

template <class Type>
Link<Type>::~Link()
{
    delete mpVal;
}

#endif // __UTIL_LINK_H__
