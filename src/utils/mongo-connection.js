const assert = require('assert');

const MongoClient = require('mongodb').MongoClient;

function MongoConnection(config) {
  if (config.mongoUri) {
    this.connectionUri = config.mongoUri;
  }
  else {
    assert.notEqual(config.hosts, null);
    this.hosts = config.hosts;
    this.db = config.db;
    this.user = config.user;
    this.password = config.password;
    this.authDb = config.authDb || config.db;
    this.replicaSet = config.replicaSet;
  }
}

MongoConnection.prototype.connect = async function() {
  const client = await MongoClient.connect(this.connectionUri || this.getConnectionUri(), {
    useNewUrlParser: true,
    useUnifiedTopology: true
  });
  this.client = client;
  return client.db(this.db);
};

MongoConnection.prototype.close = async function() {
  return this.client.close();
};

MongoConnection.prototype.getConnectionUri = function() {
  var uri = 'mongodb://';

  if (this.user) {
    uri += this.user;

    if (this.password) {
      uri += ':' + this.password;
    }

    uri += '@';
  }
  uri += this.hosts + '/';

  if (this.authDb) {
    uri += this.authDb;
  }

  if (this.replicaSet) {
    uri += '?replicaSet=' + this.replicaSet;
  }

  return uri;
};

module.exports = MongoConnection;
