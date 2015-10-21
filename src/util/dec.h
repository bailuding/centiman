#ifndef __UTIL_DEC_H__
#define __UTIL_DEC_H__

#define DEC_NULL            0
#define DEC_FULL            255
#define DEC_COMMIT          1 
#define DEC_NOT_FOUND       2
#define DEC_CONFLICT        4
#define DEC_INCONSISTENT    8
#define DEC_UNKNOWN         16
#define DEC_NONEXIST        32

class Decision
{
    public:
        static bool hasUnknown(int dec)
        {
            return dec & DEC_UNKNOWN;
        }

        static bool hasConflict(int dec)
        {
            return dec & DEC_CONFLICT;
        }

        static bool isCommit(int dec)
        {
            return dec == DEC_COMMIT;
        }

        // abort due to conflict only
        static bool isConflict(int dec)
        {
            return (dec & DEC_CONFLICT)
                && !(dec & DEC_NOT_FOUND)
                && !(dec & DEC_INCONSISTENT)
                && !(dec & DEC_NONEXIST)
                && !(dec & DEC_UNKNOWN);
        }
};

#endif // __UTIL_DEC_H__
