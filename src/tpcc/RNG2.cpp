/*
 *  RNG2.cpp
 *
 *  Created by Alan Demers on 12/08/11.
 *
 *  This is ran2 from "Numerical Recipes in C++" second edition.
 */

#include "RNG2.h"



/*
 * Constructor:
 *   install the seed
 *   load the shuffle table
 */
RNG2::RNG2(int32_t seed) {
    idum2 = idum = ((seed > 0) ? seed : ((seed < 0) ? (-seed) : 1));
    for( int32_t j = NTAB+7; j >= 0; j-- ) {
        int32_t k = idum/IQ1;
        idum = IA1 * (idum - k*IQ1) - k * IR1;
        if( idum < 0 ) idum += IM1;
        if( j < NTAB ) iv[j] = idum;
    }
    iy = iv[0];
}

/*
 * Generate uniform random deviate
 */
double RNG2::rand() {
    const double EPS = 3.0e-16;
    const double RNMX = 1.0 - EPS;
    const double AM = 1.0 / double(IM1);
    int32_t k = idum / IQ1;
    idum = IA1 * (idum - k*IQ1) - k * IR1;
    if( idum < 0 ) idum += IM1;
    k = idum2 / IQ2;
    idum2 = IA2 * (idum2 - k*IQ2) - k * IR2;
    if( idum2 < 0 ) idum2 += IM2;
    int32_t j = iy / NDIV;
    iy = iv[j] - idum2;
    iv[j] = idum;
    if( iy < 1 ) iy += IMM1;
    double temp = AM * iy;
    return ((temp > RNMX) ? RNMX : temp);
}
