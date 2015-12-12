'use strict';

var assert = require('assert');

var MongoClient = require('mongodb').MongoClient;

function MongoConnection(config){
    assert.notEqual(config.hosts, null);

    this.hosts = config.hosts;
    this.db = config.db;
    this.user = config.user;
    this.password = config.password;
    this.replicaSet = config.replicaSet;
}

MongoConnection.prototype.connect = function(cb){
    MongoClient.connect(this.getConnectionUri(), cb);
}

MongoConnection.prototype.getConnectionUri = function(){
    var uri = 'mongodb://';

    if(this.user){
        uri += this.user;

        if(this.password){
            uri += ':' + this.password;
        }

        uri += '@';
    }
    uri += this.hosts + '/';

    if(this.db){
        uri += this.db;
    }

    if(this.replicaSet){
        uri += '?replicaSet=' + this.replicaSet;
    }

    return uri;
}

module.exports = MongoConnection;