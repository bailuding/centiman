#ifndef __UTIL_COUNTER_H__
#define __UTIL_COUNTER_H__

#include <string>
#include <unordered_map>
#include <vector>
#include <iostream>
#include <ctime>
#include <cmath>

#include "util.h"

using namespace std;

class Counter
{
    public:
        Counter();
        int addList(string name);
        int add(string name, bool noReset = false);
        int addAvg(string name);
        int get(int id);
        int getId(string name);
        void update(int id, int delta);
        void updateMax(int id, int delta);
        void appendList(int id, int value);
        void inc(int id);
        string format();
        void reset();
        void set(int, int);
        void publishCounter();

    private:
        void reset(int id);
        void resetList(int id);
        string formatList(int id);
        
        unordered_map<string, int> ids;
        unordered_map<int,bool> avgs;
        vector<string> names;
        vector<int> sums;
        vector<int> cnts;
        vector<bool> noResets;
        int num;
        vector< pair<vector<int> *, vector<int> *> > lists;
        vector<string> listNames;
        int listNum;
};

inline
Counter::Counter()
{
    num = 0;
    listNum = 0;
}

inline
int Counter::add(string name, bool noReset)
{
    ids.emplace(name, num);
    names.push_back(name);
    sums.push_back(0);
    cnts.push_back(0);
    noResets.push_back(noReset);
    avgs[num] = false;
    ++num;
    return num - 1;
}

inline
int Counter::addAvg(string name)
{
    int id = add(name);
    avgs[id] = true;
    return id;
}

inline
int Counter::addList(string name)
{
    ids.emplace(name, listNum);
    listNames.push_back(name);
    lists.push_back(make_pair(new vector<int>(), new vector<int>()));
    ++listNum;
    return listNum - 1;
}

inline
void Counter::reset(int id)
{
    sums[id] = 0; 
    cnts[id] = 0;
}

inline
void Counter::resetList(int id)
{
    // clean the spare vector
    lists[id].second->clear();
    // swap current vector with the spare vector
    vector<int> * tmp = lists[id].first;
    lists[id].first = lists[id].second;
    lists[id].second = tmp;
}

inline
void Counter::reset()
{
    for (int i = 0; i < num; ++i) {
        if (!noResets[i])
            reset(i);
    }

    for (int i = 0; i < listNum; ++i) {
        resetList(i);
    }
}

inline
void Counter::update(int id, int delta)
{
    sums[id] += delta;
    ++cnts[id];
}

inline
void Counter::updateMax(int id, int delta)
{
    sums[id] = max(sums[id], delta);
    ++cnts[id];
}

inline
void Counter::appendList(int id, int value)
{
    if (!Const::NO_COUNTER_LIST)
        lists[id].first->push_back(value);
}

inline
int Counter::get(int id)
{
    return sums[id];
}

inline
int Counter::getId(string name)
{
    return ids.find(name)->second;
}

inline
void Counter::set(int id, int cnt)
{
    sums[id] = cnt;
    ++cnts[id];
}

inline
void Counter::inc(int id)
{
    ++sums[id];
    ++cnts[id];
}

inline
string Counter::formatList(int id)
{
    vector<int> * v = lists[id].first;
    string str = listNames[id];
    for (auto iter = v->begin(); iter != v->end(); ++iter) {
        str = str + " " + to_string(*iter);
    }
    return str;
}

inline
string Counter::format()
{
    string str = "";
    for (int i = 0; i < num; ++i) {
        int val = sums[i];
        if (avgs[i]) {
            val = cnts[i] == 0 ? 0 : sums[i] / cnts[i];
        }
        str = str + " | " + names[i] + " " + to_string(val);
    }
    for (int i = 0; i < listNum; ++i)
        str = str + " | " + formatList(i);
    return str;
}

#endif

