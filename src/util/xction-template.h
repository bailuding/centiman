#ifndef __UTIL_XCTION_TEMPLATE_H__
#define __UTIL_XCTION_TEMPLATE_H__

#include <cstdio>
#include <util/xction-id.h>

struct XctionTemplate
{
    int mType;
    int mRate;

    int mKeySize;
    int mValueSize;

    /* uniform */
    int mReadCnt;
    int mWriteCnt;
    
    /* synthetic */
    int mXctionSize;
    int mItemUpdate;
    int mUpdate;

    /* zipfs */
    double mAlpha;
    int mNum;

    void print();
};

inline
void XctionTemplate::print()
{
    if (mType == XID_UNIFORM) {
        fprintf(stdout, "UNIFORM: RKVRW %d %d %d %d %d\n", mRate, mKeySize, mValueSize, mReadCnt, mWriteCnt);
    } else if (mType == XID_SYN) {
        fprintf(stdout, "SYN: RKVSIU %d %d %d %d %d %d\n", mRate, mKeySize, mValueSize, mXctionSize, mItemUpdate, mUpdate);
    } else if (mType == XID_ZIPFS) {
        fprintf(stdout, "ZIPFS: RKVSIUA %d %d %d %d %d %d %f\n", mRate, mKeySize, mValueSize, mXctionSize, mItemUpdate, mUpdate, mAlpha);
    }
}

#endif 
