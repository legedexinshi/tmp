#include "Fpm_Base.h"

using namespace std;
using namespace fpnn;

typedef map<int64_t, map<item, pair<int64_t, int64_t>>> userDataMap;

class Fpm_Online_User
{
	int64_t _willExit;
	thread _thread;
	set <string> _regionSet, _ipSet, _projectSet;
	std::shared_ptr<TCPClient> _dbproxy;
	userDataMap _data;
	mutex _dataLock;
	char _buf[300];
	int64_t _lastId;
	int64_t _maxDay;
	int64_t _sleepSecond;
	int64_t _gap;
	infosMap _infosMap;
		
 public:
	Fpm_Online_User () 
	{
	} 
	~Fpm_Online_User () 
	{ 
		_willExit = 1; 
		_thread.join();
	}
	void init (string ip, int64_t port, int64_t sleepSecond = 70)
	{
		_infosMap.init();
		_gap = 5;
		_maxDay = Setting::getInt("Cache.Max.Day.OnlineUser", 30);
		_sleepSecond = sleepSecond; 
		_lastId = _willExit = 0;
		_dbproxy = TCPClient::createClient(ip, port);
		_dbproxy -> setQuestTimeout(300);
		_thread = thread (&Fpm_Online_User::cacheWork, this);
	}
	void cacheWork ()
	{
		_lastId = 0;
		while (!_willExit)
		{
			int64_t	preId = _lastId; 
			_lastId = update();
			if (_lastId - preId < ONCE_QUERY_NUM) break;
			sleep(1);
		}
cout<<"begin slow read"<<endl;
		while (!_willExit) 
		{
			{
				lock_guard<mutex> lock(_dataLock);
				while (_data.size() > 0 && _data.begin()->first < slack_real_sec() - 60 * 60 * 24 * _maxDay)
				{
					_data.erase (_data.begin());
				}
			}
			_lastId = update();
		
			sleep (_sleepSecond);
		}
	}
	
	int64_t update ()
	{
		if (_lastId == 0)
		{
			sprintf(_buf, "select id, region, ip, project_id, insert_time, num from rtm_online_user force index(insert_time) where insert_time > %ld order by id asc limit 100", (int64_t)slack_real_sec() - 60 * 60 * 24 * _maxDay);
		}
		else 
		{
			sprintf(_buf, "select id, region, ip, project_id, insert_time, num from rtm_online_user where id > %ld order by id asc limit %d", _lastId, ONCE_QUERY_NUM);
		}
		FPQWriter qw(2, "query");
		qw.param("hintId", 0);
		qw.param("sql", string(_buf));
		FPQuestPtr quest = qw.take();
		FPAnswerPtr answer = _dbproxy->sendQuest(quest);
		if (answer->answerStatus()){
			cout << "catch error " << _buf << endl;
			cout << answer->json()<<endl;
			sleep(5);
			return update();
		}
		return trans(answer);
	}
	int64_t trans (FPAnswerPtr &answer)
	{
		FPAReader rd (answer);
		vector<vector<string>> ret = rd.want ("rows", vector<vector<string>>());

		lock_guard<mutex> lock(_dataLock);
		for (auto elem: ret)
		{
			string region = elem[1], ip = elem[2], project = elem[3], insert_time = elem[4], num = elem[5];
			setted(region, _regionSet);
			setted(ip, _ipSet);
			setted(project, _projectSet);
			_infosMap.put(region, ip);
			_data[near(toDig(insert_time), _gap)][item (region, ip, project)].first += toDig(num);
			_data[near(toDig(insert_time), _gap)][item (region, ip, project)].second ++;
		}
		
		int64_t newId = _lastId;
		if (ret.size() > 0)
			newId = toDig(ret[ret.size()-1][0]);
//if(ret.size()>0)cout<<"User  LAST TIME IS   "<<toDig(ret[ret.size()-1][4])<<endl;
		return newId;
	}
	FPAnswerPtr getInfos (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
	{
		return _infosMap.getInfos(quest);
	}
	FPAnswerPtr query (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
	{
		lock_guard<mutex> lock(_dataLock);
		string region = args->wantString ("region");
		string ip = args->wantString ("ip");
		string split = args->getString ("split", "no");
		string from = args->wantString ("from");
		string to = args->wantString ("to");
		int64_t fromTime = toDig (from);
		int64_t toTime = toDig (to);
		if (region == "ALL") split = "no";	

cout<<ip<<' '<<region<<' '<<split<<' '<<from<<' '<<to<<endl;

		map <string, vector<int64_t>> tmpMap;

		set <string> regionList;
		if (region == "ALL")
			regionList = _regionSet;
		else
			regionList.insert(region);	

		set <string> ipList;
		if (ip == "ALL")
			ipList = _ipSet;
		else 
			ipList.insert(ip);

		if (split == "no")
		{
			for (auto Region: regionList)
			{
				vector<int64_t> ret;
				for (auto iter = _data.lower_bound(fromTime); iter != _data.end() && iter->first <= toTime; iter++)
				{
					int64_t total = 0;
					for (auto Ip: ipList)
					for (auto Project: _projectSet)
					{
						if (iter->second.find(item (Region, Ip, Project)) != iter->second.end())
							total += iter->second[item (Region, Ip, Project)].first / iter->second[item (Region, Ip, Project)].second;
					}
					if (total > 0)
					{
						ret.push_back(iter->first);
						ret.push_back(total);
					}
				}
				if (ret.size() > 0)
					tmpMap[Region] = ret;
			}
			return writeAnswer (tmpMap, quest);
		}
	   	else
		{
			for (auto Project: _projectSet)
			{
				vector<int64_t> ret;
				for (auto iter = _data.lower_bound(fromTime); iter != _data.end() && iter->first <= toTime; iter++)
				{
					int64_t total = 0;
					for (auto Ip: ipList)
					{
						if (iter->second.find(item (region, Ip, Project)) != iter->second.end())
							total += iter->second[item (region, Ip, Project)].first / iter->second[item (region, Ip, Project)].second;
					}
					if (total > 0)
					{
						ret.push_back(iter->first);
						ret.push_back(total);
					}
				}
				if (ret.size() > 0)
					tmpMap[Project] = ret;
			}
			return writeAnswer (tmpMap, quest);
		}
	}

	FPAnswerPtr writeAnswer (map<string, vector<int64_t>> &Map, const FPQuestPtr quest)
	{
		FPAWriter aw(1+Map.size(), quest);
		aw.paramArray ("name", Map.size());
		for (auto iter: Map)
			aw.param (iter.first);
		for (auto iter: Map)
		{
			aw.paramMap (iter.first, iter.second.size()/2); 
			for (int64_t i = 0; i < (int64_t)iter.second.size(); i+=2)
				aw.param (toStr(iter.second[i]*1000), toStr(iter.second[i+1]));
		}
		return aw.take();
	}



};

