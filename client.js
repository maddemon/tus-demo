var fs = require("fs");
var http = require("http");
var urlUtils = require("url");
var crypto = require("crypto");
var HOST = "localhost"
var PORT = "8088"
var CHUNK_SIZE = 128 * 1024;
var MAX_CONNECTIONS = 5;

//=================================================================================
// File Stat
//=================================================================================
function getCachePath(filePath){
    var md5 = crypto.createHash("md5").update(filePath).digest("hex");
    return __dirname + "/cache/" + md5;
}

function FileStat(filePath,stat){
    if(!stat){
        var cachePath = getCachePath(filePath);
        if(fs.existsSync(cachePath)){
            stat = JSON.parse(fs.readFileSync(cachePath));
        }else{
            stat = fs.statSync(filePath);
        }
    }

    this.filePath = filePath;
    this.fileSize = stat.fileSize || stat.size;
    this.remoteId = stat.remoteId || 0;
    this.chunkSize = stat.chunkSize || CHUNK_SIZE;

    fs.writeFile(getCachePath(filePath),JSON.stringify(this),function(){})
}

FileStat.prototype.update = function(callback){
    var self = this;
    fs.writeFile(getCachePath(this.filePath),JSON.stringify(this),function(){
        callback(self);
    })
}

FileStat.prototype.getUnPutRanges = function(completedRanges){
    var ranges = {};
    var start = 0;
    while(start < this.fileSize){
        var end = start + this.chunkSize - 1;
        if((completedRanges && !completedRanges[start]) || !completedRanges)
            ranges[start] = end;
        start += this.chunkSize;
    }
    var count = 0;
    for(var r in ranges){
        count++;
    }
    return count == 0 ? null : ranges;
}

FileStat.prototype.printProceeding = function(){

};

//=================================================================================
// main
//=================================================================================
(function(){
    //args: node client.js C:\test.jpg 128
    var args = process.argv;
    if(args.length < 3){
        throw new Error("miss argument:filePath");
    }
    if(args.length < 5){
        CHUNK_SIZE = parseInt(args[3]) * 1024;
    }
    var filePath = args[2];

    fs.mkdir(__dirname+"/cache/",function(){
        uploadFile(filePath);

    })

})();

function createRequest(path,method,headers,callback){
    console.log("begin request:http://" + HOST + ":" + PORT + path + " method:"+method);

    var req = http.request({
        host: HOST,
        path: path,
        port: PORT,
        method:method,
        headers:headers
    },function(res){
        var data = "";
        res.on("data",function(chunk){
            data += chunk;
        });
        res.on("end",function(){
            callback(res,data);
        })
    });

    req.on("error",function(err){
        console.log(err.stack);
        throw new Error();
        //callback();
    })
    return req;
}

function sendRequest(req,data){
    if(data) req.write(data);
    req.end();
}

function reply(stat,callback){
    callback(stat);
}

function uploadFile(filePath){

    var stat = new FileStat(filePath);

    postFile(stat,function(){
        headFile(stat,function(stat,completedRanges){
            putFile(stat,completedRanges)
        })
    });
}

function postFile(fileStat,callback){

    if(fileStat.remoteId) {
        callback(fileStat);
        return;
    };


    var req = createRequest("/files/","POST",
        {
            "content-range":"bytes */"+ fileStat.fileSize,
            "range-size":fileStat.chunkSize
        },
        function(res){
            //console.log(res.headers);
            var location = res.headers.location;
            fileStat.remoteId = location.substring(location.lastIndexOf('/')+1);
            fileStat.update(function(){
                reply(fileStat,callback);
            })
        });
    sendRequest(req);
}

function headFile(fileStat,callback){

    var req = createRequest("/file/"+fileStat.remoteId,"HEAD",null,function(res){

        var ranges = {};
        var val = res.headers.range.substring(6);
        //bytes=x-y/max | bytes=max/max
        if(val.indexOf("-") > -1){
            val.substring(0,val.lastIndexOf("/")).split(",").forEach(function(range){
                var arr = range.split('-');
                var start = parseInt((arr[0]));
                var end = parseInt(arr[1]);
                if((start === 0 || start) && end)
                    ranges[start] = end;
            });
        }else{
            var max = parseInt(val.split('/')[1]);
            var min = parseInt(val.split('/')[0]);
            if(max === min){
                reply(fileStat,callback);
                return;
            }

            if(min === 0){
                ranges = null;
            }
        }

        callback(fileStat,ranges);
    })

    sendRequest(req);
}

function putFile(fileStat,completedRanges){

    var unPutRanges = fileStat.getUnPutRanges(completedRanges);
    if(!unPutRanges){
        reply(fileStat,completedRanges);
        return;
    }

    //read file to buffer
    fs.open(fileStat.filePath,"r",function(err,fd){
        var buffer = new Buffer(fileStat.fileSize);
        fs.read(fd,buffer,0,buffer.length,0,function(){
            fs.close(fd,function(){
                //put chunks
                putChunks(unPutRanges,buffer);
            })
        });
    });
    function putChunks(ranges,buffer){

        var connections = 0;
        for(var start in ranges){

            var end = ranges[start];
            if(!end) return;

            connections++;
            if(connections > MAX_CONNECTIONS){
                //return; remove the "//"  chars, you can test resumable upload...
            }

            putChunkFile(fileStat,buffer,parseInt(start),end,function(){
                connections --;
                //remove this range;
                ranges[start] = undefined;

                reply(fileStat,function(){
                    fileStat.printProceeding();
                });
            })
        }
    }
}

function putChunkFile(fileStat,buffer,start,end,callback){
    if(!end || !buffer) return;

    var req = createRequest("/file/"+fileStat.remoteId,"PUT",{"content-range":"bytes " + start + "-" + end},
        function(res){
            console.log("put chunk:"+start+"-"+end);
            callback();
        });

    var data = new Buffer(end - start + 1);
    buffer.copy(data,0,start,end + 1);

    sendRequest(req,data);
}

