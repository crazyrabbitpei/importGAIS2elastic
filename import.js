var elasticsearch = require('elasticsearch');
var fs = require('graceful-fs');
var setting = JSON.parse(fs.readFileSync("config/setting"));

var CronJob = require('cron').CronJob;
var dateFormat = require('dateformat');

var LineByLineReader = require('line-by-line');
var S = require('string');
const exec = require('child_process').exec;
//var sleep = require('sleep'); 

var ip = setting['db_ip'];
var port = setting['db_port'];
var dbname = setting['ptt_dbname'];
//var table = setting['ptt_table'];

var column = setting['ptt_column'];
var dataDir = setting['dataDir'];

var import_record_nums=0;

var client;
var tag;
var import_again=0;

var HashMap = require('hashmap');
var importedList = new HashMap();

var job = new CronJob({
    cronTime:"00 30 00 * * *",
    onTick:function(){
        var today = new Date();
        console.log("--\ntoday:"+today);
        today.setDate(today.getDate()-1);
        console.log("yesterday:"+today);
        var import_filedate = dateFormat(today,'yyyymmdd');
        console.log('Starting import ['+import_filedate+']...');

        fs.writeFile('./cronJob/import.daily','import files:'+today+' filename:'+import_filedate,function(err){
            if(err){
                fs.appendFile('./cronJob/import.err',err,function(){});
            }
        });

        start(import_filedate);
    },
    start:false,
    timeZone:'Asia/Taipei'
});
job.start();



function start(import_filename){
    //readImportedList("/home/crazyrabbit/importGAIS2elastic/logs/8_total_list.list",function(){
    connect2DB(ip,port,function(stat){
        var date = dateFormat(new Date(), "yyyymmdd");
        readlist(dataDir,import_filename,function(fname,total_column){
            var nums = fname.length-1;
            var i=0;
            var count_importfile=0;
            /*
               for(i=0;i<nums;i++,count_importfile++){
               if(importedList.get(fname[i])!==undefined){
            //console.log(fname[i]+" imported");
            continue;
            }
            else{
            break;
            }
            }
            */
            var promise1 = new Promise(function(resolve,reject){
                console.log("["+fname[i]+"] start");
                gais2json(total_column,fname[i],function(result){
                    resolve(result);   
                });

            });
            promise1.then(function(value){
                console.log("["+value+"] done");
                fs.appendFile("logs/import_"+date+".list",value+"\n",function(err){
                    if(err){
                        //console.log("write log false:"+error);
                    }
                });
                if(count_importfile==nums){
                    console.log("All list imported.");
                }
            }).catch(function(error){
                fs.appendFile("logs/err_"+date+".log",error+"\n",function(err){
                    if(err){
                        //console.log("write log false:"+error);
                    }
                });
                clearInterval(tag);
            });

            i++;
            count_importfile++;
            /*
               for(;i<nums;i++,count_importfile++){
               if(importedList.get(fname[i])!==undefined){

            //console.log(fname[i]+" imported");
            continue;
            }
            else{
            break;
            }
            }
            */
            if(count_importfile==nums){
                console.log("All list imported.");
                return;
            }

            tag = setInterval(function(){
                var promise = new Promise(function(resolve,reject){
                    console.log("["+fname[i]+"] start");
                    gais2json(total_column,fname[i],function(result){
                        resolve(result);   
                    });

                });
                promise.then(function(value){
                    console.log("["+value+"] done");
                    fs.appendFile("logs/import_"+date+".list",value+"\n",function(err){
                        if(err){
                            //console.log("write log false:"+error);
                        }
                    });

                    count_importfile++;
                    if(count_importfile==nums){
                        console.log("All list imported.");
                    }
                }).catch(function(error){
                    fs.appendFile("logs/err_"+date+".log",error+"\n",function(err){
                        if(err){
                            //console.log("write log false:"+error);
                        }
                    });
                    clearInterval(tag);
                });

                i++;
                /*
                   for(;i<nums;i++){
                   if(importedList.get(fname[i])!==undefined){
                //console.log(fname[i]+" imported");
                continue;
                }
                else{
                break;
                }
                }
                */
                if(i==nums){
                    //console.log("Stop interval and watting....");
                    clearInterval(tag);
                }
            },300*1000);
        });
        //job.start();
    });

    //});

}






function connect2DB(dbip,dbport,fin){
    client = new elasticsearch.Client({
        host:dbip+':'+dbport,
    });
    fin("connect ok");
}

function readlist(dir,filename,fin){
//get all board, read in array, recording file name (use exec find . -name 2016* | grep -c ""   doesn't need _stop file)
    //console.log(`find `+dir+` -name `+filename);
    const child = exec(`find `+dir+` -name `+filename+" -not -name *_stop",(error,stdout,stderr) => {
        if(error!=null){
            console.log(`exec error: ${error}`);
        }
        else{
            //console.log(`stdout: ${stdout}`);
            var fname = stdout.split("\n");
            var total_column = [];
            var i;
            //read gais column's name
            var column_name = Object.keys(column);
            column_name.forEach(function(cname){
                var items = Object.keys(column[cname]);
                items.forEach(function(item) {
                        var value = column[cname][item];
                        //console.log(cname+': '+item+' = '+value);
                        total_column.push(item);

                });
            });
            /*
            for(i=0;i<total_column.length;i++){
                console.log(total_column[i]+":"+total_column[i].length);
            }
            */
           fin(fname,total_column);

        }
    });

}

function gais2json(cname,dir,fin){
    //read file 2016...
    readGaisdata(cname,dir,function(stat){
        fin(stat);
    });   
}

function readImportedList(filename,fin){
    var i;
    var body_flag=0;
    var content = [];
    var body="";

    var options = {
        skipEmptyLines:true
    }
    var lr = new LineByLineReader(filename,options);
    lr.on('error', function (err) {
        console.log("["+filename+"]error:"+err);
    });
    lr.on('line', function (line) {
        //var parts = line.split("/");
        //var newdir = "./"+parts[parts.length-2]+"/"+parts[parts.length-1];
        importedList.set(line,"1");
    });
    lr.on('end',function(){
        fin("ok");
    });
}
function readGaisdata(cname,filename,fin){
    var i;
    var body_flag=0;
    var content = [];
    var body="";

    var options = {
        skipEmptyLines:true
    }
    var lr = new LineByLineReader(filename,options);
    lr.on('error', function (err) {
        console.log("error:"+err);
    });
    lr.on('line', function (line) {
        //cut and get column info
        for(i=0;i<cname.length;i++){
            if(line=="@"){
                body_flag=0;
                //conbert to json
                if(body!=""){
                    var record = JSON.stringify({
                        title:content[0],
                        source:content[1],
                        url:content[2],
                        time:content[3],
                        body:body
                    });
                    body="";
                    var tname = S(filename).right(8).s;
                    tname = S(tname).left(6).s;//use YYYYMM ex:201602 for table's name

                    import_record_nums++;

                    setTimeout(function(){
                        import2db(dbname,tname,record);
                    },import_record_nums*1000);
                    
                    if(import_record_nums>60){
                        import_record_nums=1;
                        lr.pause();
                        setTimeout(function(){
                            lr.resume();
                        },10*1000);

                    }
                    content = [];
                    //console.log(record);
                }
                break;
            }
            else if(S(line).left(cname[i].length).s=="@body"&&cname[i]=="@body"){
                body_flag=1;
                //console.log(cname[i]+":");
                break;
            }
            else if(body_flag==1){
                //console.log(line);
                body += line;

                break;
            }
            else if(S(line).left(cname[i].length).s==cname[i]){
                //console.log(cname[i]+":"+S(line).right(line.length-cname[i].length-1).s);
                content.push(S(line).right(line.length-cname[i].length-1).s);
                break;
            }
            
        }

    });
    lr.on('end',function(){
        var record = JSON.stringify({
            title:content[0],
            source:content[1],
            url:content[2],
            time:content[3],
            body:body
        });
        var tname = S(filename).right(8).s;
        tname = S(tname).left(6).s;

        import_record_nums++;
        setTimeout(function(){
            import2db(dbname,tname,record);
        },import_record_nums*1000);
        
        if(import_record_nums>60){
            import_record_nums=1;
            lr.pause();
            setTimeout(function(){
                lr.resume();
            },60*1000);

        }
        content = [];
        //console.log("read ["+filename+"] done");
        fin(filename);
    });
}
function import2db(dname,tname,content){
    var date = dateFormat(new Date(), "yyyymmdd");
    var for_id = JSON.parse(content);
    var url = for_id['url'];
    client.create({
        index:dname,
        type:"201602",
        id:url,
        body:content
    },function(error,response){
        if(!error&&!response.error){
            /*
            var result = JSON.stringify(response);
            fs.appendFile("logs/success.log",result+"\n",function(err){
                if(err){
                    //console.log("write log false:"+error);
                }
            });
            */
        }
        else{
            //console.log("write log false");
            var temp="",code="";
            if(response!==undefined){
                temp = JSON.stringify(response.error);
                code = response.status;
                if(code!="409"&&code!="200"&&code!="201"){
                    fs.appendFile("logs/err_"+date+".content",content+"\n",function(err){
                        if(err){
                            //console.log("write log false:"+error);
                        }
                    });

                }
            }
            else{
                temp = error;
                code = "503";
            }

            if(code=="503"){
                fs.appendFile("logs/restart_"+date+".log","code:"+code+"\n"+temp+"\ncontent:"+content+"--\n",function(err){
                    if(err){
                        //console.log("write log false:"+error);
                    }

                });
                //console.log("Sleep for 1 munutes...");
                //sleep.sleep(60);
                console.log("Reimport after 1 minutes");
                setTimeout(function(){
                    import2db(dname,tname,content);
                },60*1000);
            }
            else if(code!="409"){
                fs.appendFile("logs/err_"+date+".log","code:"+code+"\n"+temp+"\n--\n",function(err){
                    if(err){
                        //console.log("write log false:"+error);
                    }

                });
            }
        }
    });

}
