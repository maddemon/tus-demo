var fs = require("fs");
var ex = require("./exceptions");

function FileMetadata(id,fileSize,chunkSize,options){
    if(global[id]){
        return global[id];
    }
    if(!fileSize && !options){
        //read metadata from file
        var data = fs.readFileSync(getFilePath(id)+".metadata");
        if(data.length == 0 ) throw new Error();
        options = JSON.parse(data);
    }

    this.id = id;
    this.fileSize = fileSize;
    this.chunkSize = chunkSize;

    this.contentType = options.contentType;
    this.contentDescription = options.contentDescription;
    this.fileName = options.fileName;
    this.chunks = options.chunks || {};
    this.combined = options.combined || false;

    global[id] = this;
}

FileMetadata.prototype.update = function(callback){
    global[this.id] = this;
    var filePath = getFilePath(this.id)+".metadata";
    fs.writeFile(filePath,JSON.stringify(this),callback);
}

FileMetadata.prototype.getChunkPath = function(start){
    return getFilePath(this.id)+"." + start + ".part";
}

FileMetadata.prototype.hasCombined = function(){
    return this.combined || global[this.id].Combining;
}

FileMetadata.prototype.hasCompleted = function(){

    var start = 0;
    while(start < this.fileSize){
        //start += this.chunkSize;
        if(!this.chunks[start])
            return false;
        start += this.chunkSize;
    }

    return true;
};

function reply(metadata,callback){

    if(metadata.hasCompleted() && !metadata.hasCombined()){
        combineChunkFiles(metadata);
    }

    return callback(metadata);
}

function getFilePath(id){
    return __dirname + "/files/" + id;
}

function createFile(fileId,fileSize,chunkSize,options,callback){
    fs.mkdir(__dirname + "/files/",function(){
        var buffer = new Buffer(0);
        fs.writeFile(getFilePath(fileId),buffer,function(){
            var metadata = new FileMetadata(fileId,fileSize,chunkSize,options);
            metadata.update();
            reply(metadata,callback);
        });
    })
}

function getFile(fileId,callback){
    var metadata = new FileMetadata(fileId);
    if(metadata.hasCombined()){
        var readStream = fs.createReadStream(getFilePath(fileId));
        callback(metadata,readStream);
    }else{
        callback(metadata);
    }
}

function combineChunkFiles(metadata){

    if(metadata.hasCombined()){
        callback(metadata);
        return;
    }

    var filePath = getFilePath(metadata.id);

    global[metadata.id].combining = true;

    combineOneChunk(0);

    function combineOneChunk(start){

        var chunkPath = metadata.getChunkPath(start);

        fs.readFile(chunkPath,function(err,data){

            fs.writeFile(filePath,data,{flag:'a'},function(){

                start += metadata.chunkSize;

                fs.unlink(chunkPath,function(){});

                if(start >= metadata.fileSize){
                    console.log("upload completed!");
                    metadata.combined = true;
                    metadata.update();
                }else{
                    combineOneChunk(start);
                }
            })
        })
    }
}

function writeFileChunk(fileId,start,end,buffer,callback){
    if(buffer.length > (end - start + 1)){
        throw new ex.ArgumentsException("data length exceed range");
    }
    var metadata = new FileMetadata(fileId);
    if(metadata.hasCombined() || metadata.hasCompleted()){
        reply(metadata,callback);
        return;
    }

    if(metadata.chunks[start]){
        reply(metadata,callback);
        return;
    }

    fs.open(metadata.getChunkPath(start),"w",function(err,fd){
        fs.write(fd,buffer,0,buffer.length,0,function(err){
            if(err) throw err;
            fs.close(fd,function(){
                metadata.chunks[start] = end;
                metadata.update();
                reply(metadata,callback);
            });
        });
    });
}

exports.createFile = createFile;
exports.writeFileChunk = writeFileChunk;
exports.FileMetadata = FileMetadata;
exports.getFile = getFile;
