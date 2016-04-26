var elasticsearch = require('elasticsearch');
var fs = require('graceful-fs');
var setting = JSON.parse(fs.readFileSync("config/setting"));

var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');

var LineByLineReader = require('line-by-line');
var S = require('string');
const exec = require('child_process').exec;


var ip = setting['db_ip'];
var port = setting['db_port'];
var dbname = setting['ptt_dbname'];
var table = setting['ptt_table'];
var column = setting['ptt_column'];
var dataDir = setting['dataDir'];

var import_record_nums=0;

var client;
var tag;

connect2DB(ip,port,function(stat){
    var table = process.argv[2];
    var scolumn = process.argv[3];
    var pattern = process.argv[4];
    var fields = "";
    search(dbname,table,scolumn,pattern,fields);
});


function connect2DB(dbip,dbport,fin){
    client = new elasticsearch.Client({
        host:dbip+':'+dbport,
    });
    fin("connect ok");
}

function search(dname,tname,scolumn,pattern,fields){
    client.search({
        index:dname,
        type:tname,
        q:scolumn+":"+pattern
    },function(err,response){
        console.log(response.hits.total);
        fs.writeFile("./search.result",JSON.stringify(response,null,2),function(){});
    });
}
