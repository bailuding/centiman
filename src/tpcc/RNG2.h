/*
 *  RNG.h
 *
 *  Created by Alan Demers on 12/06/11.
 *
 *	A RNG package based on rng2 from "Numerical Recipes in C++" Second Edition.
 *
 */

#ifndef __UTIL__RNG2__
#define __UTIL__RNG2__

#include <stdint.h>


class RNG2 {

    static const int32_t IM1 = 2147483563;
    static const int32_t IM2 = 2147483399;
    static const int32_t IA1 = 40014;
    static const int32_t IA2 = 40692;
    static const int32_t IQ1 = 53668;
    static const int32_t IQ2 = 52774;
    static const int32_t IR1 = 12211;
    static const int32_t IR2 = 3791;
    static const int32_t NTAB = 32;
    static const int32_t IMM1 = IM1 - 1;
    static const int32_t NDIV = 1 + IMM1/NTAB;
    int32_t idum;
    int32_t idum2;
    int32_t iy;
    int32_t iv[NTAB];

public:    

	/*
	 * return a uniform deviate in (0.0, 1.0)
	 */
    double rand();
    
    inline float randf() {
		return (float)(rand());
	}
	
	/*
	 * return true with probability p
	 */
	inline bool flip(double p) {
		return (rand() <= p);
	}
	
	/*
	 * choose an unsigned int distributed uniformly in [0, n)
	 */
	inline uint32_t select(unsigned n) {
		return ((unsigned)(n * rand()));
	}
	
	/*
	 * choose an int distributed uniformly in [minValue, limitValue)
	 */
	inline int32_t uniformInt(int32_t minValue, int32_t limitValue) {
		return minValue + select(limitValue - minValue);
	}
	
	/*
	 * gaussian
	 *
	 * Jim Gray, Prakash Sundaresan, Susanne Englert, Ken Baclawski, Peter J. Weinberger. 
     * Quickly generating billion-record synthetic databases, SIGMOD 1994
     * (http://portal.acm.org/citation.cfm?id=191886)
	 */
	double gauss(double mu, double sigma) {
		double ans = 0.0;
		for (int i = 0; i < 12; i++){
			ans = ans + (rand()) - 0.5;
		}
		return (mu + sigma*ans/6.0) ;
	}


    RNG2(int32_t seed = 17);
    ~RNG2() {}
};

#endif /* __UTIL__RNG2__ */
