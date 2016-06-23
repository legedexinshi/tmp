<?php
require_once ('../php/FpnnClient.php');

$client = new \Infra\Fpnn\FpnnClient ("127.0.0.1", 7089, 60000);

$choose = fgets(STDIN);

if ($choose == 0)
{	//  get region-ip or region-project infos
	//  return region-project only when table == Interfaces_Call 
	$answer = $client->sendQuest ("queryCache", array(
	    //"table" => "Fpm_Online_Time",
	    //"table" => "Fpm_Online_User",
	    //"table" => "Fpm_Gated_Connection",
	    "table" => "Fpm_Interfaces_Call",
	    "type" => "Infos"
	));
}
if ($choose == 1)
{	    //  Online Time
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Online_Time",
	    "from"  => "20160523",
	    "to"    => "20160621",
	   // "region" => "ALL",
	    "region" => "RTM-Singapore",
	    "ip" => "ALL",
	    //"ip" => "10.10.156.164",
	    "type" => "OnlineTime",
	    "split" => "yes"
	    //"split" => "no"
	));
}
if ($choose == 2)
{	    // Dau 
	 // split = "yes", 依次输出region里project Dau
	 // split = "no", 输出region里某个project 或者所有project 的Dau
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Online_Time",
	    "from"  => "20160525",
	    "to"    => "20160529",
	    "region" => "RTM-Ireland",
	    //"project" => "30005",
	    "project" => "ALL",
	    "type" => "RTMDau",
	    //"split" => "yes"
	    "split" => "no"
	));
}
if ($choose == 3)
{	    // Online User
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Online_User",
	    "from"  => "1464961600",
	    "to"    => "1464962800",
	    "region" => "RTM-Singapore",
	    //"region" => "ALL",
	    //"ip" => "ALL",
	    "ip" => "10.15.156.25",
	    "split" => "yes"
	    //"split" => "no"
	));
}
if ($choose == 4)
{	    // Gated Connection 
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Gated_Connection",
	    "from"  => "1464961600",
	    "to"    => "1464962800",
	    "region" => "RTM-EastAmerica",
	    //"region" => "ALL",
	    //"ip" => "ALL",
	    "ip" => "10.11.156.220",
	    "split" => "yes"
	    //"split" => "no"
	));
}	  
if ($choose == 5)
{	    // Interface Data 
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Interfaces_Call",
	    //"from"  => "1465920000",
	    //"to"    => "1566179200",
	    "from"  => "1265920000",
	    "to"    => "1466524800",
	    "type"  => "InterfaceData",
	    "region" => "RTM-WestAmerica",
	    "ip" => "ALL",
	    //"ip" => "10.10.156.158",
	));
}
if ($choose == 6)
{	    // Interface Detail Data 
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Interfaces_Call",
	    "from"  => "1465920000",
	    "to"    => "1466524800",
	    "type"  => "InterfaceDetailData",
	    "if_name" => "sendMessage",
	    "region" => "RTM-Ireland",
	    "ip" => "ALL",
	    //"ip" => "10.10.156.158",
	));
}
if ($choose == 7)
{	    // Interface QPS
	$answer = $client->sendQuest ("queryCache", array(
	    "table" => "Fpm_Interfaces_Call",
	    "from"  => "1465920000",
	    "to"    => "1466524800",
	    "type"  => "QPSData",
	    "region" => "RTM-Ireland",
	    //"project" => "ALL"
	    "project" => "30013"
	));
}

print_r($answer);
?>
