#ifndef __UTIL_GENERATOR_H__
#define __UTIL_GENERATOR_H__

#include <pthread.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include <util/const.h>
#include <util/packet.h>
#include <util/packet-id.h>
#include <util/random.h>
#include <util/xction-template.h>
#include <util/logger.h>
#include <util/xction-id.h>
#include <util/zipfs.h>


class Generator
{
    public:
        static void load(const char * filename);
        static int genPacketRand(Packet *, int pid);

        static XctionTemplate mXctions[32];
        static int mSize;

    private:
        static Random mRand;

        static int get(FILE * file, char * attr, char * val);
        static int fill(char * attr, char * val);
        static int test(const char *, const char *, const char *, int *);
        static int test(const char *, const char *, const char *, double *);

        static int getXctionIdx(int val);
};

#endif // __UTIL_GENERATOR_H__
