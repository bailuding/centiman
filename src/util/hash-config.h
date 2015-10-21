/* 
 * File:   hash-config.h
 * Author: blding
 *
 * Created on March 26, 2015, 6:15 PM
 */

#ifndef HASH_CONFIG_H
#define	HASH_CONFIG_H

#include <vector>
#include <tuple>
#include <algorithm>
#include <iostream>

using namespace std;

class HashConfig
{
public:
    void add(int id, int low, int high);
    int getId(int key);
    bool hasId(int id);
    void print();
    int bucketNum;
private:
    vector< tuple<int, int, int> > configs;
};

inline
void HashConfig::add(int id, int low, int high)
{
    configs.push_back(make_tuple(id, low, high));
}

inline
int HashConfig::getId(int key)
{
    int bucket = key % bucketNum;
    for (auto it = configs.begin();
            it != configs.end(); ++it) {
        if (get<1>(*it) <= bucket
                && get<2>(*it) >= bucket)
            return get<0>(*it);
    }
    return -1;
}

inline
void HashConfig::print()
{
    cout << bucketNum << endl;
    for (auto config : configs) {
        cout << get<0>(config) << " " 
            << get<1>(config) << " "
            << get<2>(config) << endl;
    }   
}

inline
bool HashConfig::hasId(int id)
{
    for (auto it = configs.begin();
            it != configs.end(); ++it) {
        if (get<0>(*it) == id)
            return true;
    }
    return false;
}
#endif	/* HASH_CONFIG_H */
