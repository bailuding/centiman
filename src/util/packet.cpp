#include "util/packet.h"
#include <assert.h>

void Packet::addValidReqRead(Key * key, Seqnum seqnum)
{
    assert(key != NULL);
    assert(key->mVal != NULL);
    mReadKeys.fillAndSet(mReadCnt, key->mVal, key->mCnt);
    mReadSeqnums.fillAndSet(mReadCnt, seqnum);
    ++mReadCnt;
}

void Packet::addValidReqWrite(Key * key)
{
    mWriteKeys.fillAndSet(mWriteCnt, key->mVal, key->mCnt);
    ++mWriteCnt;
}

void Packet::set(int pid, int sid, int xid)
{
    mPid = pid;
    this->sid = sid;
    mXid = xid;
}

int Packet::getLatency()
{
    int ltc = 0;
    if (mEnd.tv_usec >= mStart.tv_usec)
        ltc = mEnd.tv_usec - mStart.tv_usec;
    else {
        ltc = mEnd.tv_usec + 1000000 - mStart.tv_usec;
        --mEnd.tv_sec;
    }
    ltc += (mEnd.tv_sec - mStart.tv_sec) * 1000000;
    return ltc / 1000;
}

void Packet::getReadReq(Packet * req, Packet * xction)
{
    req->set(PID_READ_REQ, 0, xction->mXid);
    req->mReadCnt = xction->mReadCnt;
    req->mReadKeys.fill(req->mReadCnt);
    for (int i = 0; i < xction->mReadCnt; ++i) {
        Key * tmp = xction->mReadKeys.get(i);
        assert(tmp->mCnt > 0);
        req->mReadKeys.set(i, tmp->mVal, tmp->mCnt);
    }
}

void Packet::getWriteReq(Packet * req, Packet * xction)
{
    req->set(PID_WRITE_REQ, 0, xction->mXid);
    req->mWriteCnt = xction->mWriteCnt;
    req->mWriteKeys.fill(req->mWriteCnt);
    req->mWriteValues.fill(req->mWriteCnt);
    for (int i = 0; i < xction->mWriteCnt; ++i) {
        Key * tmp = xction->mWriteKeys.get(i);
        assert(tmp->mCnt > 0);
        req->mWriteKeys.set(i, tmp->mVal, tmp->mCnt);
        Value * ptr = xction->mWriteValues.get(i);
        assert(ptr->mCnt > 0);
        req->mWriteValues.set(i, ptr->mVal, ptr->mCnt);
    }
}

void Packet::getCommitReq(Packet * req, Packet * xction)
{
    req->set(PID_COMMIT_REQ, 0, xction->mXid);
    req->mReadCnt = xction->mReadCnt;
    req->mReadKeys.fill(req->mReadCnt);
    req->mReadSeqnums.fill(req->mReadCnt);
    for (int i = 0; i < xction->mReadCnt; ++i) {
        Key * tmp = xction->mReadKeys.get(i);
        assert(tmp->mCnt > 0);
        req->mReadKeys.set(i, tmp->mVal, tmp->mCnt);
        req->mReadSeqnums.set(i, *(xction->mReadSeqnums.get(i))); 
    }
    req->mWriteCnt = xction->mWriteCnt;
    req->mWriteKeys.fill(req->mWriteCnt);
    req->mWriteValues.fill(req->mWriteCnt);
    for (int i = 0; i < xction->mWriteCnt; ++i) {
        Key * tmp = xction->mWriteKeys.get(i);
        assert(tmp->mCnt > 0);
        req->mWriteKeys.set(i, tmp->mVal, tmp->mCnt);
        Value * ptr = xction->mWriteValues.get(i);
        assert(ptr->mCnt > 0);
        req->mWriteValues.set(i, ptr->mVal, ptr->mCnt);
    }
}

void Packet::getReadRsp(Packet * req, Packet * xction)
{
    req->set(PID_READ_RSP, 0, xction->mXid);
    req->mReadCnt = xction->mReadCnt;
    req->mReadValues.fill(req->mReadCnt);
    req->mReadSeqnums.fill(req->mReadCnt);
    for (int i = 0; i < xction->mReadCnt; ++i) {
        Value * tmp = xction->mReadValues.get(i);
//        assert(tmp->mVal != NULL);
//      assert(tmp->mCnt > 0);
        req->mReadValues.set(i, tmp->mVal, tmp->mCnt);
        req->mReadSeqnums.set(i, *(xction->mReadSeqnums.get(i)));
    }

}

void Packet::unmarshal(uint8_t * buf, int cnt) 
{
    /* Message format:
     * msg size: 2 bytes
     * xid: 4 bytes
     * pid: 1 byte
     * sid: 1 bytes
     * dec: 1 byte
     * seqnum: 8 bytes
     * water mark: 8 bytes
     * read seqnum: 8 bytes
     * read cnt: 2 bytes
     * read key size: 1 byte
     * read value size: 2 byte
     * read seqnums: 8 bytes
     * write cnt: 2 bytes
     * write key size: 1 byte
     * write value size: 2 bytes
     */
    
    reset();
    
    uint16_t idx = 0;
    /* msg size */
    idx += sizeof(idx);
    /* xid */
    padding(idx);
    mXid = *((XctionId *)(buf + idx));
    idx += sizeof(XctionId);
    /* pid */
    mPid = buf[idx];
    ++idx;
    // sid
    sid = buf[idx];
    ++idx;
    /* dec or padding */
    if (mPid == PID_VALID_RSP) {
        mDec = buf[idx];
        ++idx;
    }
    /* seqnum */
    if (mPid == PID_SEQNUM_RSP || mPid == PID_VALID_REQ 
        || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        padding(idx);
        mSeqnum = *((Seqnum *)(buf + idx));
        idx += sizeof(Seqnum);
    }
    /* water mark */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ 
        || mPid == PID_READ_RSP || mPid == PID_VALID_REQ
        || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        mWaterMark = *((Seqnum *)(buf + idx));
        idx += sizeof(mWaterMark);
    }
    /* read seqnum */
    if (mPid == PID_READ_SI_REQ || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        mReadSeqnum = *((Seqnum *)(buf + idx));
        idx += sizeof(mReadSeqnum);
    }
    /* read cnt */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ
        || mPid == PID_READ_RSP || mPid == PID_VALID_REQ
        || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        mReadCnt = *((XctionSize *)(buf + idx));
        idx += sizeof(mReadCnt);
    }
    /* read keys */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ 
        || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ) {
        mReadKeys.fill(mReadCnt);
        for (int i = 0; i < mReadCnt; ++i) {
            /* read key size */
            KeySize tmp = buf[idx];
            ++idx;
            /* read key */
            assert(tmp > 0);
            mReadKeys.set(i, buf + idx, tmp);
            idx += tmp;
        }
    }

    /* read values */
    if (mPid == PID_READ_RSP) {
        assert(mReadCnt > 0);
        mReadValues.fill(mReadCnt);
        for (int i = 0; i < mReadCnt; ++i) {
            /* read value size */
            padding(idx);
            ValueSize tmp = *((ValueSize *)(buf + idx));
            idx += sizeof(tmp);
            /* read value */
            /* tpcc may request NULL read */
//            assert(tmp > 0);
            mReadValues.set(i, buf + idx, tmp);
            idx += tmp;
        }
    }

    /* read seqnums */
    if (mPid == PID_READ_RSP || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ) {
        mReadSeqnums.fill(mReadCnt);
        /* read seqnum */
        for (int i = 0; i < mReadCnt; ++i) {
            padding(idx);
            mReadSeqnums.set(i, *((Seqnum *)(buf + idx))); 
            idx += sizeof(Seqnum);
        }
    }
    
    /* write cnt */
    if (mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        padding(idx);
        mWriteCnt = *((XctionSize *)(buf + idx));
        idx += sizeof(mWriteCnt);
    }
   
    /* write keys */
    if (mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        mWriteKeys.fill(mWriteCnt);
        for (int i = 0; i < mWriteCnt; ++i) {
            /* write key size */
            KeySize tmp = buf[idx];
            ++idx;
            /* write key */
            assert(tmp > 0);
            mWriteKeys.set(i, buf + idx, tmp);
            idx += tmp;
        }
    }

    /* write values */
    if (mPid == PID_WRITE_REQ) {
        mWriteValues.fill(mWriteCnt);
        for (int i = 0; i < mWriteCnt; ++i) {
            /* write value size */
            padding(idx);
            ValueSize tmp = *((ValueSize *)(buf + idx));
            idx += sizeof(tmp);
            /* write value */
            assert(tmp > 0);
            mWriteValues.set(i, buf + idx, tmp);
            idx += tmp;
        }
    }
}

void Packet::marshal(uint8_t * buf, int * pCnt)
{
    /* Message format:
     * msg size: 2 bytes
     * xid
     * pid
     * sid: server id
     * dec
     * seqnum
     * water mark
     * read seqnum
     * read cnt
     * read key size
     * read value size: 2 bytes
     * read seqnums
     * write cnt
     * write key size
     * write value size
     */

    assert(mPid != PID_NULL);

    uint16_t idx = 0;
    /* msg size */
    idx += sizeof(idx);
    /* xid */
    padding(idx);
    memcpy(buf + idx, &mXid, sizeof(XctionId));
    idx += sizeof(XctionId);
    /* pid */
    buf[idx] = uint8_t(mPid);
    assert(buf[idx] != 0);
    ++idx;
    // sid
    buf[idx] = uint8_t(sid);
    ++idx;
    /* dec  or padding */
    if (mPid == PID_VALID_RSP) {
        buf[idx] = int8_t(mDec);
        ++idx;
    }
    /* seqnum */
    if (mPid == PID_SEQNUM_RSP || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ 
        || mPid == PID_WRITE_REQ) {
        padding(idx);
        memcpy(buf + idx, &mSeqnum, sizeof(mSeqnum));
        idx += sizeof(mSeqnum);
    }
    /* water mark */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ 
        || mPid == PID_READ_RSP || mPid == PID_VALID_REQ
        || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        memcpy(buf + idx, &mWaterMark, sizeof(mWaterMark));
        idx += sizeof(mWaterMark);
    }
    /* read seqnum */
    if (mPid == PID_READ_SI_REQ || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        memcpy(buf + idx, &mReadSeqnum, sizeof(mReadSeqnum));
        idx += sizeof(mReadSeqnum);
    }
    /* read cnt */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ 
        || mPid == PID_READ_RSP || mPid == PID_VALID_REQ
        || mPid == PID_VALID_SI_REQ) {
        assert(mReadCnt < 65536);
        padding(idx);
        memcpy(buf + idx, &mReadCnt, sizeof(mReadCnt));
        idx += sizeof(mReadCnt);
    }
    /* read keys */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ
        || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ) {
        for (int i = 0; i < mReadCnt; ++i) {
            Key * key = mReadKeys.get(i);
            assert(key->mCnt < 256);
            buf[idx] = key->mCnt;
            assert(key->mCnt > 0);
            ++idx;
            memcpy(buf + idx, key->mVal, key->mCnt);
            idx += key->mCnt;
        }
    }

    /* read values */
    if (mPid == PID_READ_RSP) {
        assert(mReadCnt > 0);
        for (int i = 0; i < mReadCnt; ++i) {
            Value * value = mReadValues.get(i);
            assert(value->mCnt < 65536);
            /* tpcc may request NULL read */
       //     assert(value->mCnt > 0);
            padding(idx);
            memcpy(buf + idx, &(value->mCnt), sizeof(value->mCnt));
            idx += sizeof(value->mCnt);
            memcpy(buf + idx, value->mVal, value->mCnt);
            idx += value->mCnt;
        }
    }

    /* read seqnums */
    if (mPid == PID_READ_RSP || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ) {
        /* read seqnums */
        for (int i = 0; i < mReadCnt; ++i) {
            padding(idx);
            memcpy(buf + idx, mReadSeqnums.get(i), sizeof(Seqnum));
            idx += sizeof(Seqnum);
        }
    }
    /* write cnt */
    if (mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        padding(idx);
        memcpy(buf + idx, &mWriteCnt, sizeof(mWriteCnt));
        idx += sizeof(mWriteCnt);
    }
    /* write keys */
    if (mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        for (int i = 0; i < mWriteCnt; ++i) {
            Key * key = mWriteKeys.get(i);
            assert(key->mCnt < 256);
            assert(key->mCnt > 0);
            buf[idx] = key->mCnt;
            ++idx;
            memcpy(buf + idx, key->mVal, key->mCnt);
            idx += key->mCnt;
        }
    }

    /* write values */
    if (mPid == PID_WRITE_REQ) {
        for (int i = 0; i < mWriteCnt; ++i) {
            Value * value = mWriteValues.get(i);
            assert(value->mCnt < 65536);
            assert(value->mCnt > 0);
            padding(idx);
            memcpy(buf + idx, &(value->mCnt), sizeof(value->mCnt));
            idx += sizeof(value->mCnt);
            memcpy(buf + idx, value->mVal, value->mCnt);
            idx += value->mCnt;
        }
    }
    /* message size */
    memcpy(buf, &idx, sizeof(uint16_t));
    *pCnt = idx;
}

int Packet::marshalSize()
{
    /* Message format:
     * msg size: 2 bytes
     * xid
     * pid
     * sid
     * dec
     * seqnum
     * water mark
     * read seqnum
     * read cnt
     * read key size
     * read value size
     * read seqnums
     * write cnt
     * write key size
     * write value size
     */

    assert(mPid != PID_NULL);

    uint16_t idx = 0;
    /* msg size */
    idx += sizeof(idx);
    /* xid */
    padding(idx);
    idx += sizeof(XctionId);
    /* pid */
    ++idx;
    // sid
    ++idx;
    /* dec  or padding */
    if (mPid == PID_VALID_RSP) {
        ++idx;
    }
    /* seqnum */
    if (mPid == PID_SEQNUM_RSP || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ 
        || mPid == PID_WRITE_REQ) {
        padding(idx);
        idx += sizeof(mSeqnum);
    }
    /* water mark */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ
        || mPid == PID_READ_RSP || mPid == PID_VALID_REQ
        || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        idx += sizeof(mWaterMark);
    }
    /* read seqnum */
    if (mPid == PID_READ_SI_REQ || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        idx += sizeof(mReadSeqnum);
    }
    /* read cnt */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ
        || mPid == PID_READ_RSP || mPid == PID_VALID_REQ
        || mPid == PID_VALID_SI_REQ) {
        padding(idx);
        idx += sizeof(mReadCnt);
    }
    /* read keys */
    if (mPid == PID_READ_REQ || mPid == PID_READ_SI_REQ
        || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ) {
        for (int i = 0; i < mReadCnt; ++i) {
            ++idx;
            idx += mReadKeys.get(i)->mCnt;
        }
    }

    /* read values */
    if (mPid == PID_READ_RSP) {
        assert(mReadCnt > 0);
        for (int i = 0; i < mReadCnt; ++i) {
            padding(idx);
            idx += sizeof(ValueSize);
            idx += mReadValues.get(i)->mCnt;
        }
    }

    /* read seqnums */
    if (mPid == PID_READ_RSP || mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ) {
        /* read seqnums */
        for (int i = 0; i < mReadCnt; ++i) {
            padding(idx);
            idx += sizeof(Seqnum);
        }
    }
    /* write cnt */
    if (mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        padding(idx);
        idx += sizeof(mWriteCnt);
    }
    /* write keys */
    if (mPid == PID_VALID_REQ || mPid == PID_VALID_SI_REQ || mPid == PID_WRITE_REQ) {
        for (int i = 0; i < mWriteCnt; ++i) {
            ++idx;
            idx += mWriteKeys.get(i)->mCnt;
        }
    }

    /* write values */
    if (mPid == PID_WRITE_REQ) {
        for (int i = 0; i < mWriteCnt; ++i) {
            padding(idx);
            idx += sizeof(ValueSize);
            idx += mWriteValues.get(i)->mCnt;
        }
    }
    /* message size */
    return idx;
}

void Packet::print()
{
    fprintf(stdout, "RW %d %d\n", mReadCnt, mWriteCnt);
    for (int i = 0; i < mReadCnt; ++i) {
        mReadKeys.get(i)->print();
    }
    for (int i = 0; i < mWriteCnt; ++i) {
        mWriteKeys.get(i)->print();
    }
    fprintf(stdout, "\n");
}


