//
//  DataGen.h
//
//  centiman TPCC
//
//  Primitives for generating random initial data for TPCC
//
//  Created by Alan Demers on 10/16/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__DataGen__
#define __TPCC__DataGen__

#include <string.h>
#include "tpcc/RNG2.h"

namespace TPCC {

class DataGen : public RNG2 {

public:

    /*
     * non-uniform random numbers (2.1.6)
     */
	unsigned NURand(unsigned A, unsigned x, unsigned y, unsigned C) {
		return ( (((select(A+1) | uniformInt(x, y+1)) + C) % (y-x+1)) + x );
	}
    
    static unsigned const A_for_C_LAST = 255;
    static unsigned C_for_C_LAST;
    
    static unsigned const A_for_C_ID = 1023;
    static unsigned C_for_C_ID;

    static unsigned const A_for_OL_I_ID = 8191;
    static unsigned C_for_OL_I_ID;
    
    void chooseCValues();

    /*
     * random field values (4.3)
     */
	char randomAlphanumericChar() {
		return (char)(uniformInt(32, 127));
	}
	
    unsigned randomAlphanumericString( char * b, unsigned min, unsigned lim ) {
        unsigned len = uniformInt(min, lim);
        for( int i = 0; i < (int)len; i++ ) b[i] = randomAlphanumericChar();
        return len;
    }

	char randomNumericChar() {
		return (char)(uniformInt('0', 1+'9'));
	}
	
    unsigned randomNumericString( char * b, unsigned min, unsigned lim ) {
        unsigned len = uniformInt(min, lim);
        for( int i = 0; i < (int)len; i++ ) b[i] = randomNumericChar();
        return len;
    }
    
    unsigned randomCustomerName( char * b, unsigned n /*, unsigned C */ );
    
    void randomZipCode( char * b ) {
        (void)randomNumericString(b, 4, 5);
        memcpy(b+4, "11111", 5);
    }
    
    uint32_t * randomPermutation(uint32_t n);
        // returns an array containing the permutation;
        // the caller is responsible for freeing it.

	DataGen( int32_t seed ) : RNG2(seed) {}
	~DataGen() {}
}; // DataGen

}; // TPCC

#endif /* __TPCC__DataGen__ */
