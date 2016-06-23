#ifndef Fpm_Base
#define Fpm_Base

#include <cstdio>
#include <iostream>
#include <thread>
#include <map>
#include <string>
#include <set>

#include "Setting.h"
#include "msec.h"
#include "TCPClient.h"
#include "TCPEpollServer.h"


using namespace std;
using namespace fpnn;

#define ONCE_QUERY_NUM 30000

struct item {
	string region, ip, project;
	item (string _region, string _ip, string _project): region(_region), ip(_ip), project(_project)
	{}
	bool operator < (const item &a) const 
	{
		if(region != a.region) return region < a.region;
		else if(ip != a.ip) return ip < a.ip;
		else return project < a.project;
	}
};

class infosMap {
	map<string, set<string>> _dataMap;
	mutex _lock;
 public:
	void put (string str1, string str2)
	{
		lock_guard<mutex> lock(_lock);
		if (_dataMap.find (str1) == _dataMap.end()) _dataMap[str1] = set<string>();
		_dataMap[str1].insert (str2);
	}
	FPAnswerPtr getInfos (const FPQuestPtr quest)
	{
		lock_guard<mutex> lock(_lock);
		FPAWriter aw(_dataMap.size(), quest);
		for (auto iter: _dataMap)
		{
			aw.paramArray(iter.first, iter.second.size());
			for (auto it: iter.second)
			{
				aw.param(it);
			}
		}
		return aw.take();
	}
	void init ()
	{
		_dataMap.clear();
	}
}; 


mutex setLock;

	int64_t near (int64_t time, int64_t gap) 
	{
		return time / 60 / gap * 60 * gap;
	}
	int64_t toDig (string str)
	{
		return atoi(str.c_str()); // return int
	}
	string toStr (int64_t num)
	{
		char _buf[100];
		sprintf (_buf, "%ld", num);
		return string (_buf);
	}
	bool setted (string str, set<string> &Set)
	{
		lock_guard<mutex> lock(setLock);
		if (Set.find(str) != Set.end()) return true;
		Set.insert(str);
		return false;
	}

#endif

