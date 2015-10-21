#ifndef IP_CONFIG
#define IP_CONFIG

#include <utility>
#include <string>
#include <unordered_map>
#include <iostream>
#include <vector>

using namespace std;

class IpConfig
{
    public:
        void add(int, string);
        pair<string, string> getIp(int);
        int size();
        void print();

    private:
        unordered_map<int, string> ips;
};

inline
void IpConfig::add(int id, string ip)
{
    ips.emplace(id, ip);
}

inline
int IpConfig::size()
{
    return ips.size();
}

inline
void IpConfig::print()
{
    for (auto ip : ips) {
        cout << ip.first << " " << ip.second << endl;
    }    
}

inline
pair<string, string> IpConfig::getIp(int id)
{
    string ip = ips.find(id)->second;
    int pos = ip.find(':');
    return make_pair(ip.substr(0, pos),
           ip.substr(pos + 1, ip.size() - pos - 1));
}
#endif

