//
//  PKey.h
//
//  Generic Primary Keys for TPC-C tables
//
//  Created by Alan Demers on 10/17/12.
//  Copyright (c) 2012 ademers. All rights reserved.
//

#ifndef __TPCC__PKey__
#define __TPCC__PKey__

#include <stdint.h>

#include <util/type.h>

namespace TPCC {

class PKey : public Key {
public:
    void printTBLID(uint16_t & pos, uint16_t expected);
    void printPart_uint16(uint16_t & pos, const char * partName);
    void printPart_uint32(uint16_t & pos, const char * partName);
    void printPart_chars(uint16_t & pos, const char * partName);
	PKey() : Key() {}
	~PKey() {}
}; // PKey

}; // TPCC

#endif /* __TPCC__PKey__ */
