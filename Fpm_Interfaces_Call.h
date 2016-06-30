#include "Fpm_Base.h"

using namespace std;
using namespace fpnn;

typedef map<int64_t, map<item, map<char, int64_t>>> interfaceMap;
//           time                  ifname    num

class Fpm_Interfaces_Call
{
    int64_t _willExit;
    thread _thread;
    set <string> _regionSet, _ipSet, _projectSet, _interfaceSet;
    std::shared_ptr<TCPClient> _dbproxy;
    interfaceMap _data;
    mutex _dataLock;
    char _buf[300];
    int64_t _lastId;
    int64_t _maxDay;
    int64_t _sleepSecond;
    int64_t _gap;
    map <string, int64_t> indexMap;
    infosMap _infosMap;

    mutex _hashLock;
    map <string, char> _hash;
    char _hashChar;

 public:
    Fpm_Interfaces_Call ()
    {
    }
    ~Fpm_Interfaces_Call ()
    {
        _willExit = 1;
        _thread.join();
    }
    char gethash (string str)
    {
	lock_guard<mutex> lock(_hashLock);
	if (_hash.find(str) != _hash.end()) return _hash[str];
	_hash[str] = ++_hashChar;
	return _hashChar;
    }
    void init (string ip, int64_t port, int64_t sleepSecond = 70)
    {
	_hashChar = 1;
	_hash.clear();
	indexMap.clear();
	_infosMap.init();
        _interfaceSet = set<string> {"remoteGroupNote", "remoteGroupNote", "remoteGroupMessage", "sendNotes", "sendNote", "getOnlineUsers", "kickout", "sendGroupNote", "sendBroadcastMessage", "sendGroupMessage", "sendMessages", "sendMessage", "keepLive", "logout", "onlineStatus", "sendBroadcastNote", "login" };
        _gap = 5;
        _maxDay = Setting::getInt("Cache.Max.Day.InterfacesCall", 30);
        _sleepSecond = sleepSecond;
        _lastId = _willExit = 0;
        _dbproxy = TCPClient::createClient(ip, port);
        _dbproxy -> setQuestTimeout(300);
        _thread = thread (&Fpm_Interfaces_Call::cacheWork, this);
    }
    void cacheWork ()
    {
        _lastId = 0;
       //_lastId = 37319552;
        while (!_willExit)
        {
            int64_t preId = _lastId;
            _lastId = update();
            if (_lastId - preId < ONCE_QUERY_NUM) break;
            sleep(1);
            {
                lock_guard<mutex> lock(_dataLock);
                while (_data.size() > 0 && _data.begin()->first < slack_real_sec() - 60 * 60 * 24 * _maxDay)
                {
                    _data.erase (_data.begin());
                }
            }
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
        if (_lastId < 0) // ???????????????
        {
            sprintf(_buf, "select * from rtm_statistician force index(PRIMARY) where kind = 4 and insert_time > %ld order by id asc limit %d", (int64_t)slack_real_sec() - 60 * 60 * 24 * _maxDay, ONCE_QUERY_NUM);
        }
        else
        {
            sprintf(_buf, "select * from rtm_statistician force index(PRIMARY) where kind = 4 and id > %ld order by id asc limit %d", _lastId, ONCE_QUERY_NUM);
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
        if (indexMap.size() == 0)
        {
            vector<string> fields = rd.want ("fields", vector<string>());
            for (int64_t i = 0; i < (int64_t)fields.size(); i++) indexMap[fields[i]] = i;
        }

        lock_guard<mutex> lock(_dataLock);
        for (auto elem: ret)
        {
            string region = elem[indexMap["region"]], ip = elem[indexMap["ip"]], project = elem[indexMap["project_id"]], insert_time = elem[indexMap["insert_time"]];
            setted(region, _regionSet);
            setted(ip, _ipSet);
            setted(project, _projectSet);
	    _infosMap.put(region, project);
            for (auto ifName: _interfaceSet)
            {
		if (toDig(elem[indexMap[ifName]]) != 0)
			_data[near(toDig(insert_time), _gap)][item (region, ip, project)][gethash(ifName)] += toDig(elem[indexMap[ifName]]);
            }
        }

        int64_t newId = _lastId;
        if (ret.size() > 0)
            newId = toDig(ret[ret.size()-1][0]);
//if(ret.size()>0)cout<<"Statisician  LAST TIME IS   "<<toDig(ret[ret.size()-1][indexMap["insert_time"]])<<endl;
        return newId;
    }

    FPAnswerPtr getInfos (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
    { 
        return _infosMap.getInfos(quest);
    }

    FPAnswerPtr queryInterfaceData (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
    {
        lock_guard<mutex> lock(_dataLock);
        string ip = args->wantString ("ip");
        string region = args->wantString ("region");
        string from = args->wantString ("from");
        string to = args->wantString ("to");
        int64_t fromTime = toDig (from);
        int64_t toTime = toDig (to);

cout<<ip<<' '<<region<<' '<<' '<<from<<' '<<to<<endl;

        vector<string> ansName;
        vector<int64_t> ansData;

        set <string> ipList;
        if (ip == "ALL")
            ipList = _ipSet;
        else
            ipList.insert(ip);

        set <string> regionList;
        if (region == "ALL")
            regionList = _regionSet;
        else
            regionList.insert(region);

        for (auto ifName: _interfaceSet)
        {
            int64_t total = 0;
            for (auto iter = _data.lower_bound(fromTime); iter != _data.end() && iter->first <= toTime; iter++)
            {
                for (auto Project: _projectSet)
                for (auto Ip: ipList)
                for (auto Region: regionList)
                {
		    auto tt = iter->second.find (item (Region, Ip, Project));
		    if (tt != iter->second.end() && tt->second.find (gethash(ifName)) != tt->second.end()) 
			    total += iter->second[item (Region, Ip, Project)][gethash(ifName)];
                }
            }
            if (total > 0)
            {
                ansName.push_back(ifName);
                ansData.push_back(total);
            }
        }
        return writeAnswer (ansName, ansData, quest);
    }

    FPAnswerPtr queryInterfaceDetailData (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
    {
        lock_guard<mutex> lock(_dataLock);
        string ifname = args->wantString ("if_name");
        string ip = args->wantString ("ip");
        string region = args->wantString ("region");
        string from = args->wantString ("from");
        string to = args->wantString ("to");
        int64_t fromTime = toDig (from);
        int64_t toTime = toDig (to);

cout<<ip<<' '<<region<<' '<<ifname<<' '<<from<<' '<<to<<endl;

        vector<string> ansName;
        vector<int64_t> ansData;

        set <string> ipList;
        if (ip == "ALL")
            ipList = _ipSet;
        else
            ipList.insert(ip);

        set <string> regionList;
        if (region == "ALL")
            regionList = _regionSet;
        else
            regionList.insert(region);

        for (auto project: _projectSet)
        {
            int64_t total = 0;
            for (auto iter = _data.lower_bound(fromTime); iter != _data.end() && iter->first <= toTime; iter++)
            {
                for (auto Ip: ipList)
                for (auto Region: regionList)
                {
		    auto tt = iter->second.find (item (Region, Ip, project));
		    if (tt != iter->second.end() && tt->second.find (gethash(ifname)) != tt->second.end()) 
			    total += iter->second[item (Region, Ip, project)][gethash(ifname)];
                }
            }
            if (total > 0)
            {
                ansName.push_back(project);
                ansData.push_back(total);
            }
        }
        { // sort
            vector <int64_t> index;
            int64_t num = ansName.size();
            for (int64_t i = 0; i < num; i++) index.push_back(i);
            sort(index.begin(), index.end(), [=](int64_t i, int64_t j) {
                    return (ansData[i] > ansData[j]);
                    });
            FPAWriter aw(num, quest);
            for (int64_t i = 0; i < num; i++) aw.param(ansName[index[i]], toStr(ansData[index[i]]));
            return aw.take();
        }
    }

    FPAnswerPtr writeAnswer (vector<string> &ansName, vector<int64_t> &ansData, const FPQuestPtr quest)
    {
        FPAWriter aw(ansName.size(), quest);
        for (int64_t i = 0; i < (int64_t)ansName.size(); i++)
        {
            aw.param(ansName[i], toStr(ansData[i]));
        }
        return aw.take();
    }


    FPAnswerPtr queryQPSData (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
    {
        lock_guard<mutex> lock(_dataLock);
        string region = args->wantString ("region");
        string from = args->wantString ("from");
        string to = args->wantString ("to");
        string project = args->wantString ("project");
        int64_t fromTime = toDig (from);
        int64_t toTime = toDig (to);

cout<<project<<' '<<region<<' '<<' '<<from<<' '<<to<<endl;

        map <string, vector<int64_t>> tmpMap;

        set <string> projectList;
        if (project == "ALL")
            projectList = _projectSet;
        else
            projectList.insert(project);

        for (auto ifName: _interfaceSet)
        {
            vector<int64_t> ret;
            for (auto iter = _data.lower_bound(fromTime); iter != _data.end() && iter->first <= toTime; iter++)
            {
                int64_t total = 0;
                for (auto Project: projectList)
                for (auto Ip: _ipSet)
                {
		    auto tt = iter->second.find (item (region, Ip, Project));
		    if (tt != iter->second.end() && tt->second.find (gethash(ifName)) != tt->second.end()) 
			    total += iter->second[item (region, Ip, Project)][gethash(ifName)];
                }
                if (total)
                {
                    ret.push_back(iter->first * 1000);
                    ret.push_back(total);
                }
            }
            if (ret.size() > 0)
                tmpMap[ifName] = ret;
        }
        return writeAnswer (tmpMap, quest);
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
            {
                double x = iter.second[i+1];
                x /= 300;
                char tmpBuf[200];
                sprintf(tmpBuf, "%.2f", x);
                aw.param (toStr(iter.second[i]), tmpBuf);
            }
        }
        return aw.take();
    }

};

