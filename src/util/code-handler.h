#ifndef __UTIL_CODE_HANDLER_H__
#define __UTIL_CODE_HANDLER_H__

#include <errno.h>
#include <stdio.h>

#include <util/logger.h>

class CodeHandler
{
    public:
        static int print(const char * msg, int rc)
        {
            if (rc) {
                Logger::err(0, "Error: return code from %s is %d\n", msg, rc);
            }
            return rc;
        }

        static int socketRecv(int rc)
        {
            if (rc == -1) {
                if (errno != EAGAIN) {
                    perror("recv");
                }
                return 1;
            }
            if (rc == 0)
                return 1;
            return 0;
        }

};

#endif // __UTIL_CODE_HANDLER_H__

