var fs = require("fs");
var ex = require("./exceptions");

function FileMetadata(id, fileSize, options) {
    if (global[id]) {
        return global[id];
    }
    if (!fileSize && !options) {
        //read metadata from file
        var data = fs.readFileSync(getFilePath(id) + ".metadata");
        if (data.length == 0) throw new Error();
        options = JSON.parse(data);
    }

    this.id = id;
    this.fileSize = fileSize || options.fileSize;

    this.contentType = options.contentType;
    this.contentDescription = options.contentDescription;
    this.fileName = options.fileName;
    this.chunks = options.chunks || {};

    global[id] = this;
}

//更新元数据缓存并同步到文件
FileMetadata.prototype.update = function () {
    global[this.id] = this;
    var filePath = getFilePath(this.id) + ".metadata";
    fs.writeFile(filePath, JSON.stringify(this));
};
//把已写入的chunk合并成ranges
FileMetadata.prototype.getWriteRanges = function () {

    var ranges = {};
    var starts = [];
    for (var start in this.chunks) {
        starts.push(start);
    }
    starts.sort();

    var start = 0;
    var end = 0;

    //获取所有chunks的合集
    for (var i = 0; i < starts.length; i++) {
        end = this.chunks[starts[i]];
        ranges[start] = end;

        if (i + 1 == starts.length) {
            ranges[start] = end;
            break;
        }
        var nextStart = starts[i + 1];
        if (end + 1 < parseInt(nextStart)) {
            start = nextStart;
        }
    }

    return ranges;
};

//设置写入锁的状态
FileMetadata.prototype.setLockStatus = function (locked) {
    global[this.id].lock = locked;
};

//获取写入锁的状态
FileMetadata.prototype.getLockStatus = function () {
    return global[this.id].lock;
};

//判断该区域是否上传完毕
FileMetadata.prototype.hasChunk = function(start,end){
    return this.chunks[start] <= end;
};

FileMetadata.prototype.writeChunk = function(start,end){
    this.chunks[start] = end;
    this.update();
}

//是否上传完毕
FileMetadata.prototype.hasCompleted = function () {
    var ranges = this.getWriteRanges();
    return ranges[0] === this.fileSize;
};

function reply(metadata, callback) {
    return callback(metadata);
}

function getFilePath(fileId) {
    return __dirname + "/files/" + fileId;
}

function createFile(fileId, fileSize, options, callback) {
    fs.mkdir(__dirname + "/files/", function () {
        var buffer = new Buffer(fileSize);
        fs.writeFile(getFilePath(fileId), buffer, function () {
            var metadata = new FileMetadata(fileId, fileSize, options);
            metadata.update();
            reply(metadata, callback);
        });
    })
}

function getFile(fileId, callback) {
    var metadata = new FileMetadata(fileId);
    if(metadata.hasCompleted()){
        var readStream = fs.createReadStream(getFilePath(fileId));
        callback(metadata, readStream);
    }else{
        callback(metadata);
    }
}

//将上传的分块写入文件
function writeFileChunk(fileId, start, buffer, callback) {
	
	var metadata = new FileMetadata(fileId);

    //判断文件是否上传完毕
    if (metadata.hasCompleted()) {
        reply(metadata, callback);
        return;
    }

    //判断该块数据是否写入
    if (metadata.hasChunk(start, buffer.length)) {
        reply(metadata, callback);
        return;
    }


    //等待解锁
    var waite = setInterval(function () {
        if (!metadata.getLockStatus()) {
            //立即锁住
            metadata.setLockStatus(true);
            clearInterval(waite);
            //写入块
            writeChunk();
        }else{
            //console.log("waitting");
        }
    }, 10);

    var end = start + buffer.length - 1;
    if(end>metadata.fileSize){
        end = metadata.fileSize;
    }

    //写入块到文件
    function writeChunk() {
        fs.open(getFilePath(metadata.id), "r+", function (err, fd) {
            if (err) throw err;

            fs.write(fd, buffer, 0, end - start + 1, start, function (err) {
                if (err) throw err;

                fs.close(fd,function(){
                    //解锁
                    metadata.setLockStatus(false);
                    //更新写入的块到元数据
                    metadata.writeChunk(start,end);
                    reply(metadata, callback);
                });
            });
        });
    }
}

exports.createFile = createFile;
exports.writeFileChunk = writeFileChunk;
exports.FileMetadata = FileMetadata;
exports.getFile = getFile;