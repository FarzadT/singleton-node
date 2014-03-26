
var SINGLTN = "SINGLTN:";
var QUEUE = "SINGLTN:QUEUE:";

var Singleton = function(in_name,
                         in_interval,
                         in_redisConnection){

  this._redisConnection = in_redisConnection;
  this._key = SINGLTN + in_name;
  this._interval = in_interval;
  this._refresh = Math.floor(in_interval*0.5);
  this._isMaster = false;

    // instance guid
  this._guid = 'xxxxxxxxxx'.replace(/x/g,function(){return Math.floor(Math.random()*16).toString(16)});
};

Singleton.prototype.start = function(masterCallback){

  var that = this;

  // we store the master callback to avoid a closure for the
  // settimeout.
  this._masterCB = this._masterCB || masterCallback;

  if(this._timeout){
    clearTimeout(this._timeout);
    this._timeout = null;
  }

  this._redisConnection.setnx([this._key, this._guid], function(error, success){
    if(error || !success){

      that._timeout = setTimeout(that.start.bind(that),that._interval);

      if(!that._queued){
        that._redisConnection.blpop(QUEUE, 1, that.start.bind(that));
        that._queued = true;
      }

    } else if(success){
      that._isMaster = true;
      // set the key expiration
      that._redisConnection.expire([that._key, Math.round(that._interval/1000)],function(){});
      that._refreshInterval = setInterval(
        function(){
          that._redisConnection.expire([that._key, Math.round(that._interval/1000)],function(){});
        },
        that._refresh);

      // callback the master callback
      that._masterCB();
    }
  });
};

Singleton.prototype.stop = function(){
  if(this._isMaster){
    clearInterval(this._refreshInterval);
    clearTimeout(this._timeout);

    var that = this;
    this._redisConnection.del([this._key], function(error){
      that._redisConnection.rpush(QUEUE, that._guid);
    });
    this._isMaster = false;
  }
};

module.exports = Singleton;