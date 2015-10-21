#ifndef __TIMER_H__
#define __TIMER_H__

#include <assert.h>
#include <ctime>
#include <vector>

#include "util/const.h"

using namespace std;

class Timer
{
    public:
        Timer();
        void mark(int idx, bool force = false);
        void reset();
        long lap(int idx);
        long lapMs(int idx);
        void resize(int size);
    private:
        long lap(timespec * first, timespec * second);
        int cnt;
        int size;
        vector<timespec> times;
};

inline
Timer::Timer()
{
    cnt = 0;
    size = 0;
}

inline
void Timer::reset()
{
    cnt = 0;
}

inline
void Timer::mark(int idx, bool force)
{
    if (Const::NO_TIMER && !force)
        return;
    assert(idx < size);
    int rv = clock_gettime(CLOCK_REALTIME, &times[idx]);
    assert(rv == 0);
}

inline
long Timer::lap(int idx)
{
    assert(idx < size && idx > 0);
    return lap(&times[idx - 1], &times[idx]);
}

inline
long Timer::lap(timespec * first, timespec * second)
{
    long sec = second->tv_sec - first->tv_sec;
    long nano = second->tv_nsec - first->tv_nsec;
    return nano + sec * (long) 1000000000;
}

inline
long Timer::lapMs(int idx)
{
    return lap(idx) / 1000000;
}

inline
void Timer::resize(int size)
{
    while (this->size < size) {
        times.push_back(timespec());
        ++this->size;
    }
}
#endif

