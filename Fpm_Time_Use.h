#include "Fpm_Base.h"

using namespace std;
using namespace fpnn;

typedef map<int64_t, map<string, map<string, pair<int64_t, int64_t>>>> timeuseMap;
//           time        region       type          sum    count

class Fpm_Time_Use
{
    int64_t _willExit;
    thread _thread;
    set <string> _regionSet, _typeSet;
    std::shared_ptr<TCPClient> _dbproxy;
    timeuseMap _data;
    mutex _dataLock;
    char _buf[300];
    int64_t _lastId;
    int64_t _maxDay;
    int64_t _sleepSecond;
    int64_t _gap;

 public:
    Fpm_Time_Use ()
    {
    }
    ~Fpm_Time_Use ()
    {
        _willExit = 1;
        _thread.join();
    }
    void init (string ip, int64_t port, int64_t sleepSecond = 70)
    {
        _gap = 5;
        _maxDay = Setting::getInt("Cache.Max.Day.TimeUse", 30);
        _sleepSecond = sleepSecond;
        _lastId = _willExit = 0;
        _dbproxy = TCPClient::createClient(ip, port);
        _dbproxy -> setQuestTimeout(300);
        _thread = thread (&Fpm_Time_Use::cacheWork, this);
    }
    void cacheWork ()
    {
        _lastId = 0;
        while (!_willExit)
        {
            int64_t preId = _lastId;
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
            sprintf(_buf, "select id, region, type, num, insert_time from rtm_time_use where insert_time > %ld order by id asc limit 100", (int64_t)slack_real_sec() - 60 * 60 * 24 * _maxDay);
        }
        else
        {
            sprintf(_buf, "select id, region, type, num, insert_time from rtm_time_use where id > %ld order by id asc limit %d", _lastId, ONCE_QUERY_NUM);
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
            string region = elem[1], type = elem[2], num = elem[3], insert_time = elem[4];
            setted(region, _regionSet);
            setted(type, _typeSet);
            _data[near(toDig(insert_time), _gap)][region][type].first += toDig(num);
            _data[near(toDig(insert_time), _gap)][region][type].second ++;
        }

        int64_t newId = _lastId;
        if (ret.size() > 0)
            newId = toDig(ret[ret.size()-1][0]);
        return newId;
    }

     FPAnswerPtr query (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
    {
        lock_guard<mutex> lock(_dataLock);
        string region = args->wantString ("region");
        string from = args->wantString ("from");
        string to = args->wantString ("to");
        string type = args->wantString ("type");
        int64_t fromTime = toDig (from);
        int64_t toTime = toDig (to);

cout<<region<<' '<<type<<' '<<from<<' '<<to<<endl;

        map <string, vector<int64_t>> tmpMap;

        set <string> typeList;
        if (type == "ALL")
            typeList = _typeSet;
        else
            typeList.insert(type);

        for (auto Type: typeList)
        {
            vector<int64_t> ret;
            for (auto iter = _data.lower_bound(fromTime); iter != _data.end() && iter->first <= toTime; iter++)
            {
                if (iter->second.find(region) != iter->second.end() && iter->second[region].find(Type) != iter->second[region].end())
                {
                    int64_t number = iter->second[region][Type].first / iter->second[region][Type].second;
                    if (number > 0)
                    {
                        ret.push_back(iter->first);
                        ret.push_back(number);
                    }
                }
            }
            if (ret.size() > 0)
                tmpMap[Type] = ret;
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
                aw.param (toStr(iter.second[i]*1000), toStr(iter.second[i+1]));
            }
        }
        return aw.take();
    }

};

