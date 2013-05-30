var urlUtils = require("url");
//var ex = require("./exceptions");
var fileManager = require("./fileManager");
var uid = require("./uid");


var routes = {
    "file":/\/file\/([\w-]+)\/?/,
    "files":/\/files\/?/,
    "index":/\//
};
var STATUS_CODE = {
    "NotFound":404,
    "BadRequest":400,
    "OK":200,
    "Error":500,
    "Created":201
};

require("http").createServer(start).listen(8088);

function start(req,res){

    if(req.method == "OPTIONS"){
        reply(res,STATUS_CODE.OK);
        return;
    }

    var url = getCurrentUrl(urlUtils.parse(req.url).pathname);
    if(!url || url == ""){
        reply(res,STATUS_CODE.NotFound);
        return;
    }

    console.log("received request:" + url + " content-range:" + req.headers["content-range"]);

    if(url == "/"){
        reply(res,"Hello NodeJs!");
        return;
    }

    if(url.indexOf("/files") === 0 && req.method == "POST"){
        postFiles(req,res);
        return;
    }

    var fileId = getFileId(url);
    if(url.match(routes.file) && fileId == ""){
        reply(res,STATUS_CODE.BadRequest);
        return;
    }

    if(fileId != ""){
        switch(req.method){
            case "GET":
                getFile(req,res,fileId);
                return;
            case "HEAD":
                headFile(req,res,fileId);
                return;
            case "PUT":
                putFile(req,res,fileId);
                return;
            default:
                return reply(res,STATUS_CODE.BadRequest)
        }
    }

    reply(res,STATUS_CODE.NotFound);
}

function reply(res,statusCode,content){
    if(typeof (statusCode) == "string"){
        content = statusCode;
        statusCode = 200;
    }
    if(content == ""){
        switch(STATUS_CODE){
            case STATUS_CODE.BadRequest:
                content = "Bad Request!";
                break;
            case STATUS_CODE.NotFound:
                content = "Page not found!";
                break;
            case STATUS_CODE.Error:
                content = "Internal Server Error";
                break;
        }
    }
    res.writeHeader(statusCode,{"Content-Type":"text/plain"});
    res.write(content || "");
    res.end();
    if(content)  console.log(content);
}

function getCurrentUrl(pathname){
    var currentUrl = "";
    var paths = pathname.split('\n');
    for(var p in paths) {
        if(currentUrl != "") return;
        for(var r in routes){
            var groups = paths[p].toString().match(routes[r]);
            if(groups){
                currentUrl = groups[0];
                break;
            }
        }

    }
    return currentUrl;
}

function getFileId(url){
    var groups =  url.match(routes.file);
    if(groups){
        return groups[1];
    }
    return "";
}

function ContentRange(req){
    var contentRangeValue = req.headers["content-range"];
    if(!contentRangeValue){
        throw new ex.ArgumentsException("no content-range");
    }

    //value: bytes 0-99/100 | bytes */100 | bytes 0-99
    //PostFile 必须有size
    //PutFile 必须有start和end，如果是*则start为0 end为size

    var re = /bytes (((\d+)-(\d+))|\*)(\/(\d+))?/;
    var groups = contentRangeValue.match(re);
    if(groups){
        this.start = parseInt(groups[3] || 0);
        this.end = parseInt(groups[4] || groups[6]);
        if(this.end < this.start){
            throw new ex.ArgumentsException("range end must grater than start")
        }
        this.size = parseInt(groups[6] || this.end);
    }else{
        this.start = this.end = this.size = -1;
    }

    if(this.end > this.size) this.end = this.size - 1;
}

function postFiles(req,res){
    //console.log(req.headers);
    var contentRange = new ContentRange(req);
    if(!contentRange.size || contentRange.size < 0){
        reply(res,STATUS_CODE.BadRequest,"Content-Range must indicate total file size.");
        return;
    }

    var fileId = uid.v4();
    fileManager.createFile(fileId,contentRange.size,
	{
		contentType:req.headers["content-type"] || "application/octet-stream",
		contentDescription:req.headers["content-disposition"],
		fileName:req.headers["file-name"]
	},function(fileMetaData){

		res.setHeader("Location","/file/" + fileId);
		setFileHeader(res,fileMetaData);
		reply(res,STATUS_CODE.Created);
	});
}

function getFile(req,res,fileId){
    fileManager.getFile(fileId,function(metadata,readStream){
        setFileHeader(res,metadata);
        res.writeHead(STATUS_CODE.OK);
        if(readStream){
            readStream.on("open",function(){
                readStream.pipe(res);
            });
            readStream.on("end",function(){
                res.end();
            });
        }else{
            res.end();
        }
    });
}

function headFile(req,res,fileId){
    var metadata = new fileManager.FileMetadata(fileId);
    setFileHeader(res,metadata);
    reply(res,STATUS_CODE.OK);
}

function putFile(req,res,fileId){
    var contentRange = new ContentRange(req);
    var buffer = new Buffer(contentRange.end - contentRange.start + 1);
    var offset = 0;
    req.on("data",function(data){
        //console.log(data);
        data.copy(buffer,offset);
        offset += data.length;
    }).on("end",function(){
        fileManager.writeFileChunk(fileId,contentRange.start,buffer,function(fileMetadata){
            reply(res,STATUS_CODE.OK);
        });
    });
}

function setFileHeader(res,metadata){
    
	var ranges = metadata.getWriteRanges();
	var values = [];
	for(var start in ranges){
		values.push(start+"-"+ranges[start]);
	}

    res.setHeader("range","bytes=" + (values.join() || 0) + "/"+metadata.fileSize);
    res.setHeader("content-type",metadata.contentType);
}
