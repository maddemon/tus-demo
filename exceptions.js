function Exception(message){
    this.message = message;
}

Exception.prototype = new Error();
Exception.toString = function(){
    return this.message;
}

function ArgumentsException(message){
    this.name = "ArgumentsException";
    this.message = message;
}

ArgumentsException.prototype = new Exception();

function ConflictException(message){
    this.name = "ConflictException";
    this.message = message;
}
ConflictException.prototype = new Exception();


exports.ArgumentsException = ArgumentsException;
exports.ConflictException = ConflictException;
