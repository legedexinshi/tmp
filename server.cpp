#include "Fpm_Online_User.h"
#include "Fpm_Online_Time.h"
#include "Fpm_Gated_Connection.h"
#include "Fpm_Interfaces_Call.h"
#include "Fpm_Time_Use.h"

using namespace std;
using namespace fpnn;

class QuestProcessor: public IQuestProcessor
{
	QuestProcessorClassPrivateFields(QuestProcessor)

	Fpm_Online_User fpmOnlineUser;
	Fpm_Gated_Connection fpmGatedConnection;
	Fpm_Online_Time fpmOnlineTime;
	Fpm_Interfaces_Call fpmInterfacesCall;
	Fpm_Time_Use fpmTimeUse;

 public:

	FPAnswerPtr queryCache (const FPReaderPtr args, const FPQuestPtr quest, const ConnectionInfo& ci)
	{
		string table = args->want("table",string(""));
		string type = args->getString("type", "");
		if (table == "Fpm_Online_User") 
		{
			if (type == "Infos") return fpmOnlineUser.getInfos (args, quest, ci);
			return fpmOnlineUser.query (args, quest, ci);
		}
		else if (table == "Fpm_Gated_Connection") 
		{
			if (type == "Infos") return fpmGatedConnection.getInfos (args, quest, ci);
			else return fpmGatedConnection.query (args, quest, ci);
		}
		else if (table == "Fpm_Online_Time") 
		{
			if (type == "RTMDau") return fpmOnlineTime.queryDau (args, quest, ci);
			else if (type == "OnlineTime") return fpmOnlineTime.query (args, quest, ci);
			else if (type == "Infos") return fpmOnlineTime.getInfos (args, quest, ci);
			else printf ("no such type!\n");
		}
		else if (table == "Fpm_Interfaces_Call") 
		{
			string type = args->wantString("type");
			if (type == "QPSData") return fpmInterfacesCall.queryQPSData (args, quest, ci);
			else if (type == "InterfaceData") return fpmInterfacesCall.queryInterfaceData (args, quest, ci);
			else if (type == "InterfaceDetailData") return fpmInterfacesCall.queryInterfaceDetailData (args, quest, ci);
			else if (type == "Infos") return fpmInterfacesCall.getInfos (args, quest, ci);
			else printf ("no such type!\n");
		}
		else if (table == "Fpm_Time_Use")
		{
			return fpmTimeUse.query (args, quest, ci);
		}
		printf("No such table!\n");
		return NULL;
	}
	
	QuestProcessor()
	{
		string ip = Setting::getString("FPNN.server.ip");
		int64_t port = Setting::getInt("FPNN.server.port");
		fpmInterfacesCall.init (ip, port);
		fpmOnlineUser.init (ip, port);
		fpmGatedConnection.init (ip, port);
		fpmOnlineTime.init (ip, port);
		fpmTimeUse.init (ip, port);
		registerMethod("queryCache", &QuestProcessor::queryCache);
	}

	QuestProcessorClassBasicPublicFuncs
};


int main(int argc, char* argv[])
{	
	cout<<"QUERY ONCE NUM = "<<ONCE_QUERY_NUM<<endl;
	if (argc != 2)
	{
		cout<<"Usage: "<<argv[0]<<" config"<<endl;
		return 0;
	}
	if(!Setting::load(argv[1])){
		cout<<"Config file error:"<< argv[1]<<endl;
		return 1;
	}

	ServerPtr server = TCPEpollServer::create();
	server->setQuestProcessor(std::make_shared<QuestProcessor>());
	server->startup();
	server->run();

	return 0;
}

