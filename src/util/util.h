#ifndef __UTIL_UTIL_H__
#define __UTIL_UTIL_H__

#include <string>
#include <ctime>

#include <util/queue-batcher.h>

using namespace std;

class Util
{
    public:
        static void setAffinity(pthread_t thread, int cpuno) {
            int s;
            cpu_set_t cpuset;

            /* Set affinity mask to cpuno */

            CPU_ZERO(&cpuset);
            CPU_SET(cpuno, &cpuset);

            s = pthread_setaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
            if (s != 0)
                Logger::out(0, "pthread_setaffinity_np");

            s = pthread_getaffinity_np(thread, sizeof(cpu_set_t), &cpuset);
            if (s != 0)
                Logger::out(0, "pthread_getaffinity_np");
        }

        static int hex2int(char chr)
        {
            /* 0 .. 9 */
            if ('0' <= chr && chr <= '9')
                return chr - '0';
            /* A .. F */
            if ('A' <= chr && chr <= 'F')
                return chr - 'A' + 10;
            fprintf(stdout, "%u\n", uint8_t(chr));
            assert(0);
        }

        static void hex2bytes(char * hex, uint8_t * buf, int cnt)
        {
            for (int i = 0; i < cnt; ++i) {
                buf[i] = hex2int(hex[i * 2]) * 16 + hex2int(hex[i * 2 + 1]);
            }
        }

        static string now()
        {
            time_t rawtime;
            struct tm * timeinfo;
            char buf[80];

            time (&rawtime);
            timeinfo = localtime (&rawtime);

            strftime (buf,80,"%x-%X",timeinfo);
            return string(buf);
        }

};

#endif // __UTIL_UTIL_H__
