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

FileMetadata.prototype.update = function(callback){
    var filePath = getFilePath(this.id)+".metadata";
    fs.writeFile(filePath,JSON.stringify(this),callback);
}

FileMetadata.prototype.getChunks = function(callback){
    var self = this;
    fs.readdir(__dirname + "/files/",function(err,files){

        var chunks = [];
        var count = 0;
        files.forEach(function(fileName){
            count++;
            if(fileName.indexOf(self.id) > -1 && fileName.indexOf(".part")>-1){
                var start = parseInt(fileName.split('.')[1]);
                self.getChunkSize(start,function(size){
                    var end = start + size -1;
                    //console.log(size);
                    chunks.push({start:start,end:end});
                    if(count === files.length){
                        callback(chunks);
                    }
                })
            }else{
                if(count === files.length){
                    callback(chunks);
                }
            }
        });
    })
}

FileMetadata.prototype.getChunkPath = function(start){
    return getFilePath(this.id)+"." + start + ".part";
}

FileMetadata.prototype.getChunkSize = function(start,callback){
    fs.stat(this.getChunkPath(start),function(err,stats){
        if(err){
            callback(0);
        }else{
            callback(0 || stats.size);
        }
    })
};

FileMetadata.prototype.hasCombined = function(callback){
    var self = this;
    fs.stat(getFilePath(this.id),function(err,stats){
        callback(stats.size === self.fileSize);
    })
}

FileMetadata.prototype.hasCompleted = function(callback){
    var result = true;
    var self = this;
    hasChunk(0)
    function hasChunk(start){
        if(start >= self.fileSize){
            callback(true);
            return;
        }

        self.getChunkSize(start,function(chunkSize){
            if(chunkSize == 0){
                result = false;
                callback(false)
                return;
            }

            hasChunk(start += chunkSize);
        })
    }
};

function reply(metadata,callback){
    metadata.hasCompleted(function(result){
        if(!result){
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
    });
}

function getFilePath(id){
    return __dirname + "/files/" + id;
}

function createFile(fileId,fileSize,options,callback){
    fs.mkdir(__dirname + "/files/",function(){
        var buffer = new Buffer(0);
        fs.writeFile(getFilePath(fileId),buffer,function(){
            var metadata = new FileMetadata(fileId,fileSize,options);
            metadata.update();
            reply(metadata,callback);
        });
    })
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

                metadata.getChunkSize(start,function(chunkSize){
                    start += chunkSize;
                    fs.unlink(chunkPath,function(){});

                    if(start >= metadata.fileSize){
                        //global[metadata.id] = undefined;
                        console.log("upload completed!")
                        reply(metadata,callback);
                    }else{
                        combineOneChunk(start);
                    }
                });
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
        }
        metadata.getChunkSize(start,function(chunkSize){
            if(chunkSize > 0){
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
        });
    });

}

exports.createFile = createFile;
exports.writeFileChunk = writeFileChunk;
exports.FileMetadata = FileMetadata;
exports.getFile = getFile;
