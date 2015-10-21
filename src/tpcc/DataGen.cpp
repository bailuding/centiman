//
//  DataGen.cpp
//
//  Created by Alan Demers on 10/16/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include "tpcc/DataGen.h"

namespace TPCC {

    unsigned DataGen::C_for_C_LAST = 0;
    unsigned DataGen::C_for_C_ID = 0;
    unsigned DataGen::C_for_OL_I_ID = 0;
    
    void DataGen::chooseCValues() {
        { /* C_for_C_LAST (must be compatible with previously used value) */
            unsigned C_proposed;
            unsigned C_delta;
            do {
                C_proposed = uniformInt( 0, A_for_C_LAST+1 );
                C_delta = ((C_proposed > C_for_C_LAST)
                                    ? (C_proposed - C_for_C_LAST)
                                    : (C_for_C_LAST - C_proposed));
            } while( (C_delta < 65) || (C_delta == 96)
                    || (C_delta == 112) || (C_delta > 119) );
            C_for_C_LAST = C_proposed;
        }
        { /* C_for_C_ID */
            C_for_C_ID = uniformInt( 0, A_for_C_ID+1 );
        }
        { /* C_for_OL_I_ID */
            C_for_OL_I_ID = uniformInt( 0, A_for_OL_I_ID+1 );
        }
    }
    
    
    unsigned DataGen::randomCustomerName( char * b, unsigned n /*, unsigned C */ ) {
        static const char * syllable[10] = {
        	"BAR", "OUGHT", "ABLE", "PRI", "PRES",
        	"ESE", "ANTI", "CALLY", "ATION", "EING"
        };
        char tmp[16];
        if( n >= 1000 ) {
        	n = NURand( A_for_C_LAST, 0, 999, C_for_C_LAST );
        }
        strcpy(tmp, syllable[n % 10]);  n = n / 10;
        strcat(tmp, syllable[n % 10]);  n = n / 10;
        strcat(tmp, syllable[n]);
        size_t nl = strlen(tmp);
        memcpy(b, tmp, nl);
        return ((unsigned)(nl));
    }
    
    /*
     * random permutation (4.3.2.6)
     * takes linear time and space
     * caller must free the permutation when done
     */
    uint32_t * DataGen::randomPermutation(uint32_t n) {
        uint32_t * ans = new uint32_t [n];
        for( int i = 0; i < n; i++ ) ans[i] = i;

        while( n > 1 ) {
            int r = select(n);
            n--;
            if( r != n ) { uint32_t tmp = ans[r];  ans[r] = ans[n];  ans[n] = tmp; }
        }
        return ans;
    }

}; // TPCC
