#include <assert.h>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <util/bytes.h>
#include <util/index.h>
#include <util/util.h>

std::string int2str(int x)
{
    if (x == 0)
        return "0";
    std::string msg = "";
    while (x > 0) {
        char chr = x % 10 + '0';
        msg = chr + msg;
        x /= 10;
    }
    return msg;
}

int main(int argc, char ** argv)
{
    /* filename splitCnt scale */
    char * filename = argv[1];
    int splitCnt = atoi(argv[2]);
    int scale = 1;
    if (argc == 4) {
        scale = atoi(argv[3]);
    }
    fprintf(stdout, "file %s, splitCnt %d, scale %d\n", filename, splitCnt, scale);
    FILE * fileIn = fopen(filename, "r");
    FILE ** fileOuts = new FILE * [splitCnt];
    for (int i = 0; i < splitCnt; ++i) {
        std::string tmp = filename;
        tmp += "-" + int2str(splitCnt);
        tmp += "-" + int2str(i);
        /* clean file */
        fileOuts[i] = fopen(tmp.c_str(), "w");
        fclose(fileOuts[i]);
        /* open file */
        fileOuts[i] = fopen(tmp.c_str(), "a");
        fprintf(stdout, "out file %d: %s\n", i, tmp.c_str());
    }
    char buf[2048];
    uint8_t val[1024];
    int type;
    int readCnt;
    int writeCnt;
    int keyCnt;
    int valueCnt;
    Bytes key;
    Const::STORAGE_NUM = splitCnt;
    time_t start = time(NULL);
    int cnt = 0;
    while (fscanf(fileIn, "%d %d ", &readCnt, &writeCnt) == 2) {
        if (++cnt % 100000 == 0) {
            fprintf(stdout, "%s %d\n", filename, cnt); fflush(stdout);
        }
        /* omit reads */
        for (int i = 0; i < readCnt; ++i) {
            /* key */
            int rv = fscanf(fileIn, "%d ", &keyCnt);
            assert(rv == 1);
            assert(keyCnt < 1024);
            rv = fscanf(fileIn, "%s ", buf);
            assert(rv == 1);
        }
        for (int i = 0; i < writeCnt; ++i) {
            /* key */
            int rv = fscanf(fileIn, "%d ", &keyCnt);
            assert(rv == 1);
            assert(keyCnt < 1024);
            rv = fscanf(fileIn, "%s ", buf);
            assert(rv == 1);
            rv = fscanf(fileIn, "%d ", &valueCnt);
            assert(rv == 1);
            assert(scale < 255);
            /* convert from hex to byte */
            /* use the first byte as the scale id */
            Util::hex2bytes(buf, val + 1, keyCnt);

            for (uint8_t s_id = 0; s_id < scale; ++s_id) {
                /* use the first byte as the scale id */
                val[0] = s_id;
                key.copyFrom(val, keyCnt + 1);
                int idx = Index::getStorageIdx(&key);
                assert(idx >= 0 && idx < splitCnt);
                fprintf(fileOuts[idx], "%d %02X%s %d\n", keyCnt + 1, s_id, buf, valueCnt);
            }
        }
    }
    for (int i = 0; i < splitCnt; ++i) {
        fclose(fileOuts[i]);
    }
    delete[] fileOuts;
    fclose(fileIn);
    fprintf(stdout, "finish splitting %s in %.f seconds\n", filename, 1.f * time(NULL) - start);
    return 0;
}

