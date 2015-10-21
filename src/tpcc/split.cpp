#include <assert.h>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

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
    int idx = 0;
    char buf[1024][1024];
    int type;
    int readCnt;
    int writeCnt;
    int keyCnt[1024];
    int valueCnt[1024];
    int rv;
    time_t start = time(NULL);
    int cnt = 0;
    while (fscanf(fileIn, "%d %d %d ", &type, &readCnt, &writeCnt) == 3) {
        if (++cnt % 10000 == 0) {
            fprintf(stdout, "%s %d\n", filename, cnt); fflush(stdout);
        }
        /* split a transaction if it has more than 50 reads */
        int cnt = 0;
        while (readCnt > 50) {
            /* check if it is StockLevel */
            assert(type == 0);
            ++cnt;
            /* type 5 for splitted stock level xction */
            /* buffer xction */
            fprintf(fileOuts[idx], "5 %d %d\n", 50, 0);
            for (int i = 0; i < 50; ++i) {
                assert(fscanf(fileIn, "%d ", &keyCnt[i]) == 1);
                assert(keyCnt[i] < 512);
                rv = fscanf(fileIn, "%s ", buf[i]);
                assert(rv == 1);
            }
            /* scale xction */
            for (uint8_t s_id = 0; s_id < scale; ++s_id) {
                fprintf(fileOuts[idx], "5 %d %d\n", 50, 0);
                for (int i = 0; i < 50; ++i) {
                    fprintf(fileOuts[idx], "%d %02X%s\n", keyCnt[i] + 1, s_id, buf[i]);
                }
                idx = (idx + 1) % splitCnt;
            }
            readCnt -= 50;
        }
        if (cnt > 0) {
            //            fprintf(stdout, "split %d read-only txns from StockLevel\n", cnt);
        }
        assert(readCnt + writeCnt < 1024);
        /* buffer xction */
        for (int i = 0; i < readCnt; ++i) {
            assert(fscanf(fileIn, "%d ", &keyCnt[i]) == 1);
            assert(keyCnt[i] < 512);
            rv = fscanf(fileIn, "%s ", buf[i]);
            assert(rv == 1);
        }
        for (int i = 0; i < writeCnt; ++i) {
            assert(fscanf(fileIn, "%d ", &keyCnt[i + readCnt]) == 1);
            assert(keyCnt[i + readCnt] < 512);
            rv = fscanf(fileIn, "%s ", buf[i + readCnt]);
            assert(rv == 1);
            rv = fscanf(fileIn, "%d ", &valueCnt[i + readCnt]);
            assert(rv == 1);
        }

        /* scale xction */
        for (uint8_t s_id = 0; s_id < scale; ++s_id) {
            fprintf(fileOuts[idx], "%d %d %d\n", type, readCnt, writeCnt);
            for (int i = 0; i < readCnt; ++i) {
                fprintf(fileOuts[idx], "%d %02X%s\n", keyCnt[i] + 1, s_id, buf[i]);
            }
            for (int i = 0; i < writeCnt; ++i) {
                fprintf(fileOuts[idx], "%d %02X%s\n%d\n", 
                        keyCnt[i + readCnt] + 1, s_id, buf[i + readCnt], valueCnt[i + readCnt]);
            }
            idx = (idx + 1) % splitCnt;
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

