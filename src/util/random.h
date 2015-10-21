#ifndef __UTIL_RANDOM_H__
#define __UTIL_RANDOM_H__

#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>

#include <stdio.h>

class Random
{
    private:
        int mSeed;
    public:
        Random()
        {
            mSeed = getpid() ^ (pthread_self() + 1) ^ clock();
            fprintf(stdout, "seed %d\n", mSeed);
        }

        Random(int seed)
        {
            mSeed = seed * (pthread_self() + 1);
        }
        
        /* Copy from Wiki */
        int rand()
        {
            mSeed = 18000 * (mSeed & 65535) + (mSeed >> 16);
            return mSeed;
        }
};

#endif // __UTIL_RANDOM_H__

