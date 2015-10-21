#ifndef __UTIL_STAT_H__
#define __UTIL_STAT_H__

#include <map>
#include <stdio.h>
#include <string.h>
#include <time.h>

class Stat
{
    public:
        Stat();
        void reset();
        void setStart();
        void setEnd();
        
        void update(int dec);
        void diffDec();
        void latency(int val);
        
        void merge(Stat & stat);
        void publish(const char * hdr = "");
        
        int mComplete;
        int mCommit;
        int mNotFound;
        int mInconsistent;
        int mConflict;
        int mDiffDec;
        int mLatency;
        double mThroughput;
        double mNetThroughput;

        double mAbortRate;
        double mDiffDecRate;
        double mConflictRate;
        double mNotFoundRate;
        double mInconsistentRate;

        time_t mStart;
        time_t mEnd;

        int mMergeCnt;

        double avgLatency();
        void setAbortRate();
};

inline
Stat::Stat()
{
    reset();
}

inline
void Stat::reset()
{
    mComplete = 0;
    mCommit = 0;
    mNotFound = 0;
    mInconsistent = 0;
    mConflict = 0;
    mDiffDec = 0;
    mLatency = 0;
    mThroughput = 0;
    mNetThroughput = 0;
    mStart = 0;
    mEnd = 0;
    mMergeCnt = 0;
}

inline
void Stat::merge(Stat & stat)
{
    mComplete += stat.mComplete;
    mCommit += stat.mCommit;
    mNotFound += stat.mNotFound;
    mInconsistent += stat.mInconsistent;
    mConflict += stat.mConflict;
    mDiffDec += stat.mDiffDec;
    mLatency += stat.mLatency;
    mStart += stat.mStart;
    ++mMergeCnt;
}

inline
void Stat::setStart()
{
    mStart = time(NULL);
}

inline
void Stat::setEnd()
{
    mEnd = time(NULL);
    double diff = difftime(mEnd, mStart);
    mThroughput = double(mCommit) / diff;
    mNetThroughput = double(mComplete) / diff;
    setAbortRate();
}

inline
void Stat::update(int dec)
{
    ++mComplete;
    if (dec == DEC_COMMIT)
        ++mCommit;
    if (dec & DEC_NOT_FOUND)
        ++mNotFound;
    if (dec & DEC_INCONSISTENT)
        ++mInconsistent;
    if (dec & DEC_CONFLICT)
        ++mConflict;
}

inline
void Stat::diffDec()
{
    ++mDiffDec;
}

inline
void Stat::latency(int val)
{
    mLatency += val;
}

inline
double Stat::avgLatency()
{
    return double(mLatency) / (mComplete + 1);
}

inline
void Stat::setAbortRate()
{
    mDiffDecRate = 100.0 * mDiffDec / (mComplete + 1);
    mAbortRate = 100.0 * (mComplete - mCommit) / (mComplete + 1);
    mNotFoundRate = 100.0 * mNotFound / (mComplete + 1);
    mConflictRate = 100.0 * mConflict / (mComplete + 1);
    mInconsistentRate = 100.0 * mInconsistent / (mComplete + 1);
}

inline
void Stat::publish(const char * hdr)
{
    if (mMergeCnt > 0) {
        mStart /= mMergeCnt;
    }
    setEnd();
    char msg[256];
    strcpy(msg, "Count");
    if (strlen(hdr) > 0) {
        strcpy(msg, hdr); 
    }
    Logger::out(0, "\n%s: %d %d %d %d %d %d"
                    " | Throughput: %.4f %.4f"
                    " | Abort %.4f %.4f %.4f %.4f %.4f"
                    " | Latency %.4f\n",
                    msg,
                    mComplete, mCommit, mConflict, mInconsistent, mNotFound, mDiffDec,
                    mThroughput / 1000, mNetThroughput / 1000, 
                    mAbortRate, mConflictRate, mInconsistentRate, mNotFoundRate, mDiffDecRate, 
                    avgLatency());
}

#endif // __UTIL_STAT_H__
