var fs = require("fs");

function FileMetadata(id,fileSize,options){
    if(!fileSize && !options){
        //read metadata from file
        var data = fs.readFileSync(getFilePath(id)+".metadata");
        if(data.length == 0 ) throw new Error();
        var instance = JSON.parse(data);
        return new FileMetadata(instance.id,instance.fileSize,instance);
    }
    this.id = id;
    this.fileSize = fileSize;
    this.contentType = options.contentType;
    this.contentDescription = options.contentDescription;
    this.chunkSize = options.chunkSize;
    this.fileName = options.fileName;
}

FileMetadata.prototype.update = function(){
    var filePath = getFilePath(this.id)+".metadata";
    fs.writeFileSync(filePath,JSON.stringify(this));
}

FileMetadata.prototype.getChunks = function(){
    var self = this;
    var chunks = [];
    fs.readdirSync(__dirname + "/files/").forEach(function(fileName){
        if(fileName.indexOf(self.id) > -1 && fileName.indexOf(".part")>-1){
            var start = parseInt(fileName.split('.')[1]);
            var end = start + self.getChunkSize(start) -1;
            chunks.push({start:start,end:end});
        }
    });
    return chunks;
}

FileMetadata.prototype.getChunkPath = function(start){
    return getFilePath(this.id)+"." + start + ".part";
}

FileMetadata.prototype.getChunkSize = function(start){
    var chunkPath = this.getChunkPath(start);
    if(fs.existsSync(chunkPath)){
        var stat = fs.statSync(chunkPath);
        return stat.size;
    }
    return 0;
    //return fs.existsSync(getFilePath(this.id)+".part."+start);
};

FileMetadata.prototype.hasCompleted = function(){
    var result = true;
    var start = 0;
    while(start < this.fileSize - 1){
        var chunkSize = this.getChunkSize(start);
        if(chunkSize == 0){
            result = false;
            break;
        }
        start += chunkSize;
    }
    return result;
};

FileMetadata.prototype.hasCombined = function(callback){
    var self = this;
    fs.stat(getFilePath(this.id),function(stat){
        callback(stat != null && stat.size == self.fileSize);

    })
};

function reply(metadata,callback){

    if(!metadata.hasCompleted()){
        callback(metadata);
        return;
    }

    metadata.hasCombined(function(combined){
        if(!combined){
            combineChunkFiles(metadata,function(){
                callback(metadata);
            });
        }else{
            callback(metadata);
        }
    });
}

function getFilePath(id){
    return __dirname + "/files/" + id;
}

function createFile(fileId,fileSize,options,callback){
    fs.mkdir(__dirname + "/files/")
    var buffer = new Buffer(0);
    fs.writeFile(getFilePath(fileId),buffer,function(){
        var metadata = new FileMetadata(fileId,fileSize,options);
        metadata.update();
        reply(metadata,callback);
    });
}

function getFile(fileId,callback){
    var metadata = new FileMetadata(fileId);
    metadata.hasCombined(function(combined){
        if(combined){
            //response file
            var readStream = fs.createReadStream(getFilePath(fileId));
            callback(metadata,readStream);
        }else{
            //response header
            callback(metadata);
        }
    })
}

function combineChunkFiles(metadata,callback){

    if(global[metadata.id] && global[metadata.id].combining){
        callback();
        return;
    }

    var filePath = getFilePath(metadata.id);

    global[metadata.id] = {combining : true};

    combineOneChunk(0);

    function combineOneChunk(start){

        var chunkPath = metadata.getChunkPath(start);

        fs.readFile(chunkPath,function(err,data){

            fs.writeFile(filePath,data,{flag:'a'},function(){

                start += metadata.getChunkSize(start);

                fs.unlink(chunkPath,function(){});

                if(start >= metadata.fileSize){
                    //global[metadata.id] = undefined;
                    console.log("upload completed!")
                    reply(metadata,callback);
                }else{
                    combineOneChunk(start);
                }
            })
        })
    }
}

function writeFileChunk(fileId,start,end,buffer,callback){
    var metadata = new FileMetadata(fileId);
    metadata.hasCombined(function(combined){
        if(combined){
            reply(metadata,callback);
            return;
        }else{
            //check chunk have wrote?
            if(metadata.getChunkSize(start,end) > 0){
                reply(metadata,callback)
                return;
            }

            fs.open(metadata.getChunkPath(start),"w",function(err,fd){
                fs.write(fd,buffer,0,buffer.length,0,function(err){
                    if(err) throw err;
                    fs.close(fd,function(){
                        reply(metadata,callback);
                    });
                });
            });
        }

    });

}

exports.createFile = createFile;
exports.writeFileChunk = writeFileChunk;
exports.FileMetadata = FileMetadata;
exports.getFile = getFile;