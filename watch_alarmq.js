// ESOCKETTIMEDOUT
process.env.UV_THREADPOOL_SIZE = 128;
//
const fs = require("fs");
const config = require('config');
const mysql = require('mysql');
const http = require("http");
const request = require('request');
const moment = require('moment');

var ESConfig = config.get('ES'),
    ESAddress = ESConfig.host+':'+ESConfig.port,
    ESAuth = ESConfig.username+':'+ESConfig.password;

var RedisConfig = config.get('Redis');
var redis = require("redis"),rclient,r2client;
    //rclient = redis.createClient(RedisConfig);  // move to retry function

var elasticsearch = require('elasticsearch');
var preindexname = ESConfig.alarmindexname;

var alarm_to_sms_api = true;  // true or false nodejs to SMS Alarm API

var eclient = new elasticsearch.Client({
  host: ESAddress,
  httpAuth: ESAuth,
  //log: 'trace'  // debug mode
});

var MySQLConfig = config.get('MYSQL'),connection;
// console logger
var ConsoleLogger = config.get('Setting.ConsoleLogger');
// Alarm API
var AlarmURL = config.get('Alarm.AlarmURL');

eclient.ping({
  // ping usually has a 3000ms timeout
  requestTimeout: 1000
}, function (error) {
  if (error) {
    console.trace('ERROR:elasticsearch cluster is down!');
  } else {
    console.log('### START INFO : Elasticsearch All is well ### ENV=' + process.env.NODE_ENV);
  }
});

var alarm_again_time = 48*60*60; // 48小時沒動作 Alarm again (將status update = 00 


function waitForPush () {
  rclient.brpop('ALARM_TO_ES',0, function (err, reply) {
    if (err) {
        console.log("ERROR: ALARM_TO_ES "+err);throw err;
    }
    //console.log("reply= "+reply);
    console.log("Key = "+reply[0]);
    console.log("### Got Redis Value = "+reply[1]);

    let json = reply[1];
    let rev_obj = JSON.parse(json);

    let obj = JSON.parse(json);
    let alarm_type = obj.alarm_type;
    let alarm_id = obj.alarm_id;
    let mismatch_type = obj.mismatch_type;
    let alarm_country = obj.alarm_nmoss_country;
    let radius_time = moment(obj.radius_time).format('YYYY-MM-DD HH:mm:ss');  // alarm time 15min point
    let alarm_time_desc='[異常時間:'+radius_time+']';
    let description = '';
    let gui_username = obj.username; // username from GUI
    let reverse_mismatch_type = '';

if(alarm_type != 'devices' && alarm_type != 'cells' && alarm_type != 'apns' &&　alarm_type != 'sites' &&　alarm_type != 'pgws')
{
  console.log("ERROR: alarm_type = "+alarm_type+", but only allow devices/cells/apns/sites/pgws");
  waitForPush();
  return;
}

// added by Yancey 2022-12-01 If Pause from GUI
let pause_status = (gui_username)?true:false;  // if usename is not empty Pause from GUI
if(gui_username) console.log("INFO: Got GUI username = "+gui_username);

switch (mismatch_type) {
  case 'IH':
    description = '異常原因:UpLink流量過高'; //input 
    reverse_mismatch_type = 'ILOK';
    break;
  case 'IHOK':
    description = '異常恢復:UpLink流量過高異常已恢復'; //input 
    break;    
  case 'IL':
    description = '異常原因:UpLink流量過低';
    reverse_mismatch_type = 'IHOK';
    break;
  case 'ILOK':
    description = '異常恢復:UpLink流量過低異常已恢復';
    break;    
  case 'OH':
    description = '異常原因:DownLink流量過高'; // output 
    reverse_mismatch_type = 'OLOK';
    break;
  case 'OHOK':
    description = '異常恢復:DownLink流量過高異常已恢復'; // output 
    break;    
  case 'OL':
    description = '異常原因:DownLink流量過低';
    reverse_mismatch_type = 'OHOK';
    break;
  case 'OLOK':
    description = '異常恢復:DownLink流量過低異常已恢復';
    break;    
  case 'ST':
    description = '異常原因:SessionTime異常';
    break;
  case 'STOK':
    description = '異常恢復:SessionTime異常異常已恢復';
    break;    
  case 'CC':
    description = '異常原因:活躍裝置數異常';
    break;
  case 'CCOK':
    description = '異常恢復:活躍裝置數異常已恢復';
    break;    
  case 'CCU':
    description = '異常原因:START訊令次數異常';
    break;
  case 'CCUOK':
    description = '異常恢復:START訊令次數異常已恢復';
    break;     
  case 'CCD':
    description = '異常原因:STOP訊令次數異常';
    break;
  case 'CCDOK':
    description = '異常恢復:STOP訊令次數異常已恢復';
    break;    
  case 'OK':
    description = '異常恢復';
    break;
  case 'CCH':
    description = '異常原因:活躍裝置數量異常高'; //add TWM not use at 2022-06-21
    reverse_mismatch_type = 'CCLOK';
    break;
  case 'CCHOK':
    description = '異常恢復:活躍裝置數量異常高異常已恢復'; //add TWM 2022-12-01
    break;
  case 'CCL':
    description = '異常原因:活躍裝置數量異常低'; //add TWM
    reverse_mismatch_type = 'CCHOK';
    break;
  case 'CCLOK':
    description = '異常恢復:活躍裝置數量異常低異常已恢復'; //add TWM 2022-12-01
    break;

  case 'CCUH':
    description = '異常原因:START連線(訊令)次數異常高'; //add TWM 2021-05-26
    reverse_mismatch_type = 'CCULOK';
    break;
  case 'CCUHOK':
    description = '異常恢復:START連線(訊令)次數異常高異常已恢復'; //add TWM 2022-12-01
    break;
  case 'CCUL':
    description = '異常原因:START連線(訊令)次數異常低'; //add TWM 2021-05-26
    reverse_mismatch_type = 'CCUHOK';
    break; 
  case 'CCULOK':
    description = '異常恢復:START連線(訊令)次數異常低異常已恢復'; //add TWM 2022-12-01
    break; 
   case 'CCDH':
    description = '異常原因:STOP連線(訊令)次數異常高'; //add TWM 2021-05-26
    reverse_mismatch_type = 'CCDLOK';
    break;
  case 'CCDHOK':
    description = '異常恢復:STOP連線(訊令)次數異常高異常已恢復'; //add TWM 2022-12-01
    break;
  case 'CCDL':
    description = '異常原因:STOP連線(訊令)次數異常低'; //add TWM 2021-05-26
    reverse_mismatch_type = 'CCDHOK';
    break; 
  case 'CCDLOK':
    description = '異常恢復:STOP連線(訊令)次數異常低異常已恢復'; //add TWM 2022-12-01
    break;
  case 'TAL':
    description = '異常原因:流量異常低 '; //add TWM 2021-11-10
    break;
  case 'CCAL':
    description = '異常原因:連線(訊令)異常低'; //add TWM 2021-11-10
    break;
  default:
    description='';
}

if(description =='')
{
  console.log("ERROR: mismatch_type = "+mismatch_type+", mismatch_type not allow");
  waitForPush();
  return;
}

// 2022-08-09 Turn off apns and pgws alarm code = IH/OH/CCUH/CCDH
if(alarm_type =='apns' || alarm_type =='pgws')
{
 if(mismatch_type =='IH' || mismatch_type =='OH' || mismatch_type =='CCUH' || mismatch_type =='CCDH')
 {
  console.log("WARN: alarm_type = "+alarm_type+", mismatch_type = "+mismatch_type+" is disable");
  waitForPush();
  return;
  }
}

// send reverse message 2022-12-09 (if got CCH send CCLOK)
if(reverse_mismatch_type !='')
{ 
  rev_obj.mismatch_type = reverse_mismatch_type;
  reverse_recover(rev_obj);
}

// 2022-06-21 Turn off All alarm code = CCH 
if(mismatch_type =='CCH')
{
  console.log("WARN: mismatch_type = "+mismatch_type+", mismatch_type all disable");
  waitForPush();
  return;
}

obj.description = description;

// ===================================================
// v20200514 check orign alarm status first
let ok_status = (mismatch_type.indexOf('OK') >= 0)?true:false;
let origin_mismatch_type = mismatch_type;
mismatch_type = mismatch_type.replace('OK','');
// ===================================================

// TWM alarm type apns , sites , pgws , cells
if(alarm_type =='apns' || alarm_type=='sites' || alarm_type=='pgws' || alarm_type=='cells')
{
      // only cells show description+alarm_time_desc
      let alarm_desc = description;

      alarm_desc = description+alarm_time_desc;

      //let sql = "replace into alarms set alarm_type='"+alarm_type+"',alarm_id='"+alarm_id+"',type='"+mismatch_type+"',description='"+description+"',status='0'";
      let timediff = "TIMESTAMPDIFF(SECOND,created_at,NOW()) as timediff";
      let sql = "select count(*) as count,status,DATE_FORMAT(created_at,'%Y-%m-%d %H:%i:%s') as created_at,"+timediff+" from alarms where alarm_type='"+alarm_type+"' and alarm_id='"+alarm_id+"' and type='"+mismatch_type+"'";
      if(ConsoleLogger) console.log("INFO: SQL = "+sql);
      connection.query(sql, function (error, results) {
      if (error) {console.log("ERROR:Mysql error = "+error);return;} //throw error;
        console.log("MySQL Results : "+JSON.stringify(results));
        console.log("MySQL Results Count: "+results[0].count);

        if(results[0].count == 0 && ok_status == false)  //if alarm not exist in DB
        {

          let sqlreplace="replace into alarms set alarm_type='"+alarm_type+"',alarm_id='"+alarm_id+"',type='"+mismatch_type+"',description='"+alarm_desc+"',status='0'";
          if(ConsoleLogger) console.log("INFO: SQL = "+sqlreplace);
          connection.query(sqlreplace, function(error){
            if (error) {console.log("ERROR:Mysql error = "+error);return;} //throw error;
                
                if (alarm_type == 'sites')  //20200519 if alarm_type=sites check to_alarm status
                  check_sites_alarm(alarm_type,alarm_id,mismatch_type,radius_time);   
                else
                  callAlarmAPI2(alarm_type,alarm_id,mismatch_type,radius_time);  //trigger Harry's alarm URL here
            
          });
          
          // if not exist in DB send Alarm to ES history
          myAsyncFunc(obj);
          return;

        }
        else
        {
           let created_at = results[0].created_at;
           console.log("INFO: Alarm created_at Time = "+created_at);
           //if(results[0].status > 0 && results[0].timediff >= alarm_again_time )
           if(results[0].status == 0)  // if alarm exist not PAUSE
           {
  
            if(ok_status == false)  // if mismatch_type is DOWN update DB again
            {
            let sqlreplace="replace into alarms set alarm_type='"+alarm_type+"',alarm_id='"+alarm_id+"',type='"+mismatch_type+"',description='"+alarm_desc+"',status='0',created_at='"+created_at+"'";
            if(ConsoleLogger) console.log("INFO: SQL Alarm = "+sqlreplace);
              connection.query(sqlreplace, function(error){
                  if(error) {console.log("ERROR:Mysql error = "+error);return;} //throw error;

                    // set devices alarm time to MySQL
                    if(alarm_type == 'devices') { devices_alarmtime(alarm_id); }
                  
              });
            }
            else  // if alarm exist and mismatch_type UP , do delete
            {

              if(pause_status == false)  // if UP not from GUI then delete DB
              {
                let sqlreplace="delete from alarms where alarm_type='"+alarm_type+"' and alarm_id='"+alarm_id+"' and type='"+mismatch_type+"' and status='0'";
                if(ConsoleLogger) console.log("INFO: SQL Alarm delete = "+sqlreplace);
                connection.query(sqlreplace, function(error){
                  if(error) {console.log("ERROR:Mysql error = "+error);return;} //throw error;
                              
                });
              }

                if (alarm_type == 'sites')  // check
                  check_sites_alarm(alarm_type,alarm_id,origin_mismatch_type,radius_time);                
                else
                  callAlarmAPI2(alarm_type,alarm_id,origin_mismatch_type,radius_time); //  

                 // if delete send clear to ES History
                myAsyncFunc(obj);
                return;      

            }


           }
           else
            {

              if(pause_status == true)  // if UP not from GUI then delete DB
              {
                console.log("INFO: UP Message from GUI mismatch_type = "+origin_mismatch_type+" , gui_username = "+gui_username);
                if (alarm_type == 'sites')  // check
                  check_sites_alarm(alarm_type,alarm_id,origin_mismatch_type,radius_time,gui_username);                
                else
                  callAlarmAPI2(alarm_type,alarm_id,origin_mismatch_type,radius_time,gui_username); 
                obj.description = description + '(*Pause by '+gui_username+')';
                obj.mismatch_type = origin_mismatch_type;
                myAsyncFunc(obj);
              }    
              // if Alarm is PAUSE 
              console.log("INFO: alarm_status = "+results[0].status+", alarm_again_time = "+alarm_again_time+" , timediff = "+results[0].timediff+"");
              return;
            }

        }
        
      }); // end of SQL

}

    waitForPush();

  });
}

// start watch alarmq
dbconnect();
redisconnect();
waitForPush();

async function myAsyncFunc(json) {

	//let obj = JSON.parse(json);
  let obj = json;
	let alarm_type = obj.alarm_type;
	let alarm_id = obj.alarm_id;
  let update_time = obj.update_time;
  let mismatch_type = obj.mismatch_type;
  
  //let short_date = GetYearMonth();
  let short_date = moment(update_time).format('YYYY-MM');
  if(ConsoleLogger) console.log('INFO: Get Alarm short_date='+short_date);

  let index_name = preindexname+short_date; // alarm-2019-10
  
  // check if empty
  obj['radius_time'] = obj['radius_time'] || update_time;
  obj['description'] = obj['description'] || null;

  //========================
  if(ConsoleLogger) console.log("INFO: Index Name = "+index_name);
  if(ConsoleLogger) console.log("=================================");
  if(ConsoleLogger) console.log("### Prepare Post to ES json ### \n"+JSON.stringify(obj));
  if(ConsoleLogger) console.log("=================================");

	if(alarm_type == null || alarm_type =='')
		{
			console.log("ERROR-ERROR alarm_type = "+alarm_type);
			return;
		}

	if(alarm_id == null || alarm_id =='')
		{
			console.log("ERROR-ERROR alarm_id = "+alarm_id);
			return;
		}

  if(mismatch_type == null || mismatch_type =='')
    {
      console.log("ERROR-ERROR alarm_id = "+alarm_id);
      return;
    }

	if(update_time == null || update_time =='')
		{
			console.log("ERROR-ERROR update_time = "+update_time);
			return;
		}	

    // update alarm status
    //update_alarm_status(obj);
// =======================================================
 try {

   let rs_json = await eclient.index({
      index: index_name,
      body: obj
    });
   
   if(ConsoleLogger) console.log("INFO: ES Return Result = "+JSON.stringify(rs_json));
   if(ConsoleLogger) console.log("INFO: RS Return Success = "+rs_json._shards.successful);

   if( rs_json._shards.successful != '1')
   {
     console.log("ERROR - Post to ES error! eid = "+eid);
   }
   
    } catch (e) {
        console.log("ERROR:POST ES "+e);
    }
  //=====================================================
  return;
}


function update_alarm_status(obj)
{

  // check if empty
  obj['radius_time'] = obj['radius_time'] || obj.update_time;
  obj['description'] = obj['description'] || '';
  console.log("description = "+obj.description);

  let datetime = new Date().getTime();
  let alarm_key = obj.alarm_type+':'+obj.alarm_id;
  let msisdn_key = 'MISMATCHALARMS:'+ alarm_key;
  console.log("INFO: msisdn_key = "+msisdn_key);
  rclient.hset(msisdn_key,'alarm_type', obj.alarm_type);
  rclient.hset(msisdn_key,'alarm_id', obj.alarm_id);
  rclient.hset(msisdn_key,'mismatch_type',obj.mismatch_type);
  rclient.hset(msisdn_key,'description',obj.description);
  rclient.hset(msisdn_key,'radius_time',obj.radius_time);

}


function GetYearMonth() {
  let date = new Date();
  return date.getFullYear() + '-' + padLeft(date.getMonth()+1,2);
}

function padLeft(str, len) {
    str = '' + str;
    return str.length >= len ? str : new Array(len - str.length + 1).join("0") + str;
}


function handleError (err) {
  if (err) {
      //console.log("WARN: MySQL handleError = "+err);
      //console.log("WARN: MySQL error code = "+err.code);
    if(err.code === 'ECONNRESET')
    {
      console.log("WARN: MySQL handleError = "+err);      
      setTimeout(dbconnect, 200); // connect right now
    }
    else
    {
      console.log("WARN: MySQL handleError = "+err);
      console.log("WARN: MySQL error code = "+err.code);      
      setTimeout(dbconnect, 2000);
    }

  }
}

function dbconnect() {

  //console.log("INFO: *** New connection with function dbconnect...");
  connection = mysql.createConnection(MySQLConfig);
  connection.connect(handleError);
  connection.on('error', handleError);
  //connection.on('uncaughtException', handleError);

}


function redisconnect () {

  rclient = redis.createClient(RedisConfig);
  rclient.auth('twmiot');

  r2client = redis.createClient(RedisConfig);
  r2client.auth('twmiot');

  console.log("INFO: Try Redis connection !");
  rclient.on("error", function (err) {
    console.log("ERROR:redis " + err);
    throw err;
  });
  
}



function devices_alarmtime(msisdn)
{
  if(msisdn)
  {
  let sqlalarm="update devices set alarmed_at=NOW() where msisdn='"+msisdn+"'";
            if(ConsoleLogger) console.log("INFO: SQL = "+sql);
              connection.query(sqlalarm, function(error){
                  if(error) {console.log("ERROR:Mysql error = "+error);return;} //throw error;
                  if(ConsoleLogger) console.log("AlARM OK: SQL Alarm = "+sql);
              });
  }
}

function callAlarmAPI2(alarm_type,alarm_id,alarm_code,radius_time,gui_username){

if(alarm_to_sms_api)
{
  console.log('INFO: Call SMS Alarm API : ',alarm_type,alarm_id,alarm_code,radius_time);
}
else
{
  console.log('WARN: Call SMS API here but turn off!',alarm_type,alarm_id,alarm_code);
  return; // just return , don't need send syslog to splunk
}

  let pausebyuser = (gui_username)?'&pauseby='+gui_username:'';

 if(alarm_type != '' && alarm_id != '' && alarm_code != '')
 {

    //change to enb_cell_id format Ex:  312599‬/25 
    //alarm_id=enb_cell_id(alarm_type,alarm_id); // TWM trun off

    let alarm_url = AlarmURL+'?alarm_type='+alarm_type+'&alarm_id='+encodeURI(alarm_id)+'&alarm_code='+alarm_code+'&alarm_time='+encodeURI(radius_time)+pausebyuser;
    console.log('INFO: AlarmURL = '+alarm_url);

    http.get(alarm_url, (res) => {
      console.log(`Got response: ${res.statusCode}`);
      // consume response body
      res.resume();
    }).on('error', (e) => {
        console.log(`ERROR:SMS-API ${e.message}`);
    });

  } 

}

function enb_cell_id(alarm_type,alarm_id)
{

  if(alarm_type == 'cells')
  {
    let str_array = alarm_id.split('-');
    let ecid = str_array[0];
    //console.log('ecid='+ecid);

    if(ecid.length > 7)
    {
      let ecid_hex = parseInt(ecid).toString(16);
      //console.log('ecid_hex='+ecid_hex);
      let enb_id_hex = ecid_hex.substr(0,5);
      //console.log('enb_id_hex='+enb_id_hex);
      let enb_cell_id_hex = ecid_hex.substr(5,2);
      //console.log('enb_cell_id_hex='+enb_cell_id_hex);
      let enb_id = parseInt(enb_id_hex,16);
      //console.log('enb_id='+enb_id);
      let enb_cell_id = parseInt(enb_cell_id_hex,16);
      //console.log('enb_cell_id='+enb_cell_id);
      let Cell_id = enb_id+'/'+enb_cell_id;
      //console.log('## Cell_id='+Cell_id);
      return Cell_id;
    }
  }

return alarm_id;

}

function alarm_queue(alarm_type,alarm_id,alarm_code,alarm_country)
{
    alarm_country = (alarm_country == 'NULL' || alarm_country == null || alarm_country == '') ? '未知地點':alarm_country;
    let alarm_key = 'ALARM:'+alarm_type+':'+alarm_code+':'+alarm_country;
    let system_time = moment().format();
    console.log('Write to SMS Queue : '+alarm_key+';'+alarm_id+','+system_time);
    let rs = r2client.hset(alarm_key, alarm_id , system_time);  
    //let rs = r2client.lpush(alarm_key, alarm_id);  
    console.log('SMS Queue result = '+rs);
}


function check_sites_alarm(alarm_type,alarm_id,alarm_code,radius_time,gui_username)
{

  let sites_sql="select id,name,to_alarm from sites where id='"+alarm_id+"'";
  if(ConsoleLogger) console.log("INFO: SQL = "+sites_sql);
  connection.query(sites_sql, function (error, results) {
      console.log("INFO: Site id query Result = "+JSON.stringify(results));

      if (error) {console.log("ERROR:Mysql error = "+error);return;} //throw error;

      if (results[0].to_alarm == '1')
          callAlarmAPI2(alarm_type,alarm_id,alarm_code,radius_time,gui_username); //if site alarm status is ON
      else
          console.log("INFO: Alarm Site id = "+alarm_id+" , but alarm status is OFF");

  });

}


function reverse_recover(rev_obj)
{
  rev_obj = JSON.stringify(rev_obj);
  console.log('INFO: Send ALARM_TO_ES LPUSH reverse Message = '+ rev_obj);
  let rs = r2client.lpush('ALARM_TO_ES', rev_obj);
}