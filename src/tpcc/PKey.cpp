//
//  PKey.cpp
//
//  Created by Alan Demers on 10/16/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#include <cstdio>
#include <stdint.h>
#include <assert.h>

#include "tpcc/PKey.h"

namespace TPCC {
    
    void PKey::printTBLID(uint16_t & pos, uint16_t expected) {
        assert( mCnt >= 2 );
        assert( *((uint16_t *)(mVal)) == expected );
        fprintf(stdout, "ID OK ");
        pos = 2;
    }
    
    void PKey::printPart_uint16(uint16_t &pos, const char *partName) {
        fprintf(stdout, "%s ", partName);
        if( (pos + sizeof(uint16_t)) > mCnt ) {
            fprintf(stdout, "(NULL) ");
        } else {
            fprintf(stdout, "%d ", *((uint16_t *)(mVal + pos)) );
            pos += sizeof(uint16_t);
        }
    }
    
    void PKey::printPart_uint32(uint16_t &pos, const char *partName) {
        fprintf(stdout, "%s ", partName);
        if( (pos + sizeof(uint32_t)) > mCnt ) {
            fprintf(stdout, "(NULL) ");
        } else {
            fprintf(stdout, "%d ", *((uint32_t *)(mVal + pos)) );
            pos += sizeof(uint32_t);
        }
    }
    
    void PKey::printPart_chars(uint16_t &pos, const char *partName) {
        fprintf(stdout, "%s ", partName);
        if( pos > mCnt ) {
            fprintf(stdout, "(NULL) ");
        } else {
            while( pos < mCnt ) { fprintf(stdout, "%c", *((char *)(mVal+pos)));  ++pos; }
            fprintf(stdout, " ");
        }
    }
    
};
