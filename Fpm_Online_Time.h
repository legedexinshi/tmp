#include "Fpm_Base.h"

using namespace std;
using namespace fpnn;

typedef map<string, map<item, map<string, int64_t>>> timeDataMap;
//          date_id		start_time  num
typedef map<string, map<string, map<string, int64_t>>> timeDauMap;
// 	    date_id	region      pid	      num

class Fpm_Online_Time
{
	int64_t _willExit;
	thread _thread;
	set <string> _regionSet, _ipSet, _projectSet;
	std::shared_ptr<TCPClient> _dbproxy;
	timeDataMap _data;
	timeDauMap _dau;
	mutex _dataLock;
	char _buf[300];
	int64_t _lastId, _lastDauId;
	int64_t _maxDay;
	int64_t _sleepSecond;
	infosMap _infosMap;

 public:
	Fpm_Online_Time () 
	{
	} 
	~Fpm_Online_Time () 
	{ 
		_willExit = 1; 
		_thread.join();
	}
	void init (string ip, int64_t port, int64_t sleepSecond = 70)
	{
		_infosMap.init();
		_maxDay = Setting::getInt("Cache.Max.Day", 30);
		_sleepSecond = sleepSecond; 
		_lastId = _willExit = 0;
		_dbproxy = TCPClient::createClient(ip, port);
		_dbproxy -> setQuestTimeout(300);
		_thread = thread (&Fpm_Online_Time::cacheWork, this);
	}
	void cacheWork ()
	{
		_lastId = _lastDauId = 0;
		while (!_willExit)
		{
			int64_t	preId = _lastId, preDauId = _lastDauId; 
			update(_lastId, 1);
			update(_lastDauId, 2);
			if (_lastId - preId < ONCE_QUERY_NUM && _lastDauId - preDauId < ONCE_QUERY_NUM) break;
			sleep(1);
		}
		while (!_willExit) 
		{
			{
				lock_guard<mutex> lock(_dataLock);
				while ((int64_t)_data.size() > _maxDay + 2) 
				{
					_data.erase (_data.begin());
				}
				while ((int64_t)_dau.size() > _maxDay + 2)
				{
					_dau.erase (_dau.begin());
				}
			}
			update(_lastId, 1);
			update(_lastDauId, 2);
		
			sleep (_sleepSecond);
		}
	}
	
	void update (int64_t &_lastId, int64_t flag)
	{
		if (_lastId == 0)
		{
			int64_t t = slack_real_sec();
			t -= _maxDay * 24 * 60 * 60;
			struct tm timeInfo;
			struct tm *tmT = localtime_r(&t, &timeInfo);
			char buff[20];
			sprintf (buff, "%04d%02d%02d", tmT->tm_year+1900, tmT->tm_mon+1, tmT->tm_mday);
			buff[8] = 0;

			if (flag == 1)
			{
				sprintf(_buf, "select id, region, ip, start_time, date_id, project_id, num from rtm_online_time where date_id >= %s order by id asc limit %d", buff, ONCE_QUERY_NUM);
			}
			else if (flag == 2)
			{
				sprintf(_buf, "select id, region, pid, date_id, num from rtm_dau where date_id >= %s order by id asc limit %d", buff, ONCE_QUERY_NUM);
			}
		}
		else 
		{
			if (flag == 1)
			{
				sprintf(_buf, "select id, region, ip, start_time, date_id, project_id, num from rtm_online_time where id > %ld order by id asc limit %d", _lastId, ONCE_QUERY_NUM);
			}
			else if (flag == 2)
			{
				sprintf(_buf, "select id, region, pid, date_id, num from rtm_dau where id > %ld order by id asc limit %d", _lastId, ONCE_QUERY_NUM);
			}
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
			update(_lastId, flag);
			return ;
		}
		trans(answer, _lastId, flag);
	}
	void  trans (FPAnswerPtr &answer, int64_t &_lastId, int64_t flag)
	{
		FPAReader rd (answer);
		vector<vector<string>> ret = rd.want ("rows", vector<vector<string>>());

		lock_guard<mutex> lock(_dataLock);
		for (auto elem: ret)
		{
			if (flag == 1)
			{
				string region = elem[1], ip = elem[2], start_time = elem[3], date_id = elem[4], project_id = elem[5], num = elem[6];
				setted(region, _regionSet);
				setted(ip, _ipSet);
				setted(project_id, _projectSet);
				_infosMap.put(region, ip);
				_data[date_id][item(region, ip, project_id)][start_time] = 
					max(_data[date_id][item(region, ip, project_id)][start_time], toDig(num)); 
			}
			else if (flag == 2)
			{
				string region = elem[1], pid = elem[2], date_id = elem[3], num = elem[4];
				setted(region, _regionSet);
				setted(pid, _projectSet);
				_dau[date_id][region][pid] = toDig(num);
			}
		}
		
		if (ret.size() > 0) _lastId = toDig(ret[ret.size()-1][0]);
//if(ret.size()>0&&flag==1)cout<<"Time  LAST TIME IS   "<<toDig(ret[ret.size()-1][4])<<endl;
//if(ret.size()>0&&flag==2)cout<<"Dau  LAST TIME IS   "<<toDig(ret[ret.size()-1][3])<<endl;
	}

	FPAnswerPtr getInfos (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
        {
                return _infosMap.getInfos(quest);
        }

	FPAnswerPtr queryDau (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
	{
		lock_guard<mutex> lock(_dataLock);
		string region = args->wantString ("region");
		string project = args->wantString ("project");
		string split = args->getString ("split", "no");
		string from = args->wantString ("from");
		string to = args->wantString ("to");

cout<<project<<' '<<region<<' '<<split<<' '<<from<<' '<<to<<endl;

		map <string, vector<int64_t>> tmpMap;

		set <string> projectList;
		if (project == "ALL" || split == "yes")
			projectList = _projectSet;
		else 
			projectList.insert(project);

		if (split == "no")
		{
			vector<int64_t> ret;
			for (auto iter = _dau.lower_bound(from); iter != _dau.end() && iter->first <= to; iter++)
			{
				int64_t totalDau = 0; 
				for (auto Project: projectList)
				{ 
					totalDau += _dau[iter->first][region][Project];
				}
				if (totalDau > 0)
				{
					ret.push_back(toDig(iter->first));
					ret.push_back(totalDau);
				}
			}
			if (ret.size() > 0)
				tmpMap[region] = ret;
			return writeAnswer (tmpMap, quest);
		}
		else 
		{
			for (auto Project: projectList)
			{
				vector<int64_t> ret;
				int64_t totalDau; 
				for (auto iter = _dau.lower_bound(from); iter != _dau.end() && iter->first <= to; iter++)
				{ 
					totalDau = _dau[iter->first][region][Project];
					if (totalDau > 0)
					{
						ret.push_back(toDig(iter->first));
						ret.push_back(totalDau);
					}
				}
				if (ret.size() > 0)
					tmpMap[Project] = ret;
			}
			return writeAnswer (tmpMap, quest);
		}
	}

	FPAnswerPtr query (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
	{
		lock_guard<mutex> lock(_dataLock);
		string region = args->wantString ("region");
		string ip = args->wantString ("ip");
		string split = args->getString ("split", "no");
		string from = args->wantString ("from");
		string to = args->wantString ("to");
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
				for (auto iter = _data.lower_bound(from); iter != _data.end() && iter->first <= to; iter++)
				{
					int64_t totalDau = 0, totalNum = 0;
					for (auto Project : _projectSet)
					{
						totalDau += _dau[iter->first][Region][Project];
						for (auto Ip: ipList)
						{
							if (iter->second.find (item (Region, Ip, Project)) != iter->second.end())
								for (auto it:iter->second[item (Region, Ip, Project)]) 
									totalNum += it.second;
						}
					}
					if (totalDau > 0 && totalNum > 0)
					{
						ret.push_back(toDig(iter->first));
						ret.push_back(totalNum / totalDau / 60);
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
				for (auto iter = _data.lower_bound(from); iter != _data.end() && iter->first <= to; iter++)
				{
					int64_t totalNum = 0, totalDau = _dau[iter->first][region][Project];
					for (auto Ip: ipList)
					{
						if (iter->second.find (item (region, Ip, Project)) != iter->second.end())
							for (auto it:iter->second[item (region, Ip, Project)]) 
								totalNum += it.second;
					}
					if (totalDau > 0 && totalNum > 0)
					{
						ret.push_back(toDig(iter->first));
						ret.push_back(totalNum / totalDau / 60);
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
				aw.param (toStr(iter.second[i]), toStr(iter.second[i+1]));
		}
		return aw.take();
	}



};

