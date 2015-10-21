/* 
 * File:   config-reader.h
 * Author: blding
 *
 * Created on March 26, 2015, 6:02 PM
 */

#ifndef __UTIL_CONFIG_H__
#define	__UTIL_CONFIG_H__

#include <cstdio>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "util/hash-config.h"
#include "util/ip-config.h"
#include "util/const.h"
#include "util/generator.h"

using namespace std;

class Config
{

public:
    Config(string config);
    void load();
    void print();
    int getSwitchStage(unsigned long ts);
    unsigned long getPrepareTime();

    // switch config
    int switchMode;

    // validator hash config
    HashConfig hashConfig;
    HashConfig newHashConfig;

    // ip config
    IpConfig sIps;
    IpConfig vIps;

private:
    IpConfig loadIpConfig(string filename);
    HashConfig loadHashConfig(string filename);
    unordered_map<string, string> load(string filename);

    string configFile;
    // switch config
    unsigned long prepareTime;
    unsigned long switchTime;
};

inline
Config::Config(string filename)
{
    configFile = filename;
    switchTime = prepareTime = -1;
    switchMode = 0;
}

inline
unsigned long Config::getPrepareTime()
{
    return prepareTime;
}

inline
int Config::getSwitchStage(unsigned long ts)
{
    if (!switchMode || ts < prepareTime)
        return 0;
    if (ts < prepareTime + switchTime)
        return 1;
    return 2;
}

inline
void Config::print()
{
    cout << configFile << endl;
    cout << "sIps" << endl;
    sIps.print();
    cout << "vIps" << endl;
    vIps.print();
    cout << "hashConfig" << endl;
    hashConfig.print();
    cout << "newHashConfig" << endl;
    newHashConfig.print();
}

inline
void Config::load()
{
//    cout << configFile << endl;

    // load config file
    unordered_map<string, string> files = load(configFile);

    // load const
    auto iter = files.find("const");
    assert(iter != files.end());
    Const::load(iter->second.c_str());
    
    // load generator config
    iter = files.find("generator");
    assert(iter != files.end());
    Generator::load(iter->second.c_str());

    // load ip config
    iter = files.find("storage_ip_config");
    assert(iter != files.end());
    sIps = loadIpConfig(iter->second);
    //sIps.print();
    cout << "load " << sIps.size() << " storage IPs" << endl;

    iter = files.find("validator_ip_config");
    assert(iter != files.end());
    vIps = loadIpConfig(iter->second);
    //vIps.print();
    cout << "load " << vIps.size() << " validator IPs" << endl;

    // load hash config
    iter = files.find("validator_hash_config");
    assert(iter != files.end());
    hashConfig = loadHashConfig(iter->second);
    hashConfig.print();

    // load new hash config if there is any
    iter = files.find("validator_new_hash_config");
    if (iter != files.end()) {
        newHashConfig = loadHashConfig(iter->second);
        newHashConfig.print();
    }

    // load switch config
    iter = files.find("prepare_time");
    if (iter != files.end()) {
        prepareTime = stoi(iter->second);
    }
    iter = files.find("switch_time");
    if (iter != files.end()) {
        switchTime = stoi(iter->second);
    }
    iter = files.find("switch_mode");
    if (iter != files.end()) {
        switchMode = stoi(iter->second);
    }
}

inline
IpConfig Config::loadIpConfig(string filename)
{
  //  cout << filename << endl;
    unordered_map<string, string> attrs = load(filename);
    IpConfig ips;
    for (auto iter = attrs.begin(); iter != attrs.end(); ++iter)
    {
        ips.add(stoi(iter->first), iter->second);
    }
    return ips;
}

inline
unordered_map<string, string> Config::load(string filename)
{
    FILE * file = fopen(filename.c_str(),"r");
    char attr[512];
    char val[512];
    unordered_map<string, string> attrs;
    while (fscanf(file, "%[^=]%*c%s ", attr, val) == 2) {
        attrs.emplace(string(attr), string(val));
    }
    fclose(file);

    return attrs;
}

inline
HashConfig Config::loadHashConfig(string filename)
{
    //cout << filename << endl;
    ifstream fin(filename);
    string line;
    int id;
    int low;
    int high;
    HashConfig hashConfig;
    
    // bucket number
    fin >> hashConfig.bucketNum;
    // omit \n
    getline(fin, line);

    while (getline(fin, line)) {
        //cout << line << endl;
        istringstream sin;
        sin.str(line);
        sin >> id >> low >> high;
        hashConfig.add(id, low, high);
    }
    fin.close();
    return hashConfig;
}

#endif	/* CONFIG_READER_H */

