#ifndef __UTIL_ZIPFS_H__
#define __UTIL_ZIPFS_H__

#include <math.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

class Zipfs
{
    public:
        Zipfs();
        int next(int myMaxRandom, double myTheta);
    private:
        double theta;
        double zetaN;
        double zetaTwo;
        double alpha;
        double eta;
        int maxRandom;

        double zeta(int n, double theta);
};

inline
Zipfs::Zipfs()
{
    /* initialize random number gen */
    int seed = getpid() ^ (pthread_self() + 1) ^ clock();
    fprintf(stdout, "zipfs seed %d\n", seed);
    srand(seed);

    maxRandom = -1;
    theta = -1.0;
}

inline
double Zipfs::zeta(int n, double theta) {
    double ans = 0.0;
    for(int i = 1; i <= n; i++) {
        ans += pow(1.0 / i, theta);
    }
    return ans;
}

inline
int Zipfs::next(int myMaxRandom, double myTheta) {

    //Cache values.
    if(maxRandom != myMaxRandom || myTheta != theta) {

        maxRandom = myMaxRandom;
        theta = myTheta;
        zetaN = zeta(maxRandom, theta);
        zetaTwo = zeta(2, theta);
        alpha = 1.0 / (1.0 - theta);
        eta = (1.0 - pow(2.0 / maxRandom, 1.0 - theta)) / (1.0 - zetaTwo / zetaN);
    }

    double u = rand() / (double(RAND_MAX) + 1);
    double uz = u * zetaN;

    if(uz < 1.0) {
        return 0;
    }

    if(uz < 1.0 + pow(0.5, theta)) {
        return 1;
    }
    return (int) (maxRandom * pow(eta * u - eta + 1.0, alpha));
}

#endif 
