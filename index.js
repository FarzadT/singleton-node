
var SINGLTN = "SINGLTN:";
var QUEUE = "SINGLTN:QUEUE:";

var Singleton = function(in_name, in_interval, in_redisConnection){

  this._redisConnection = in_redisConnection;
  this._key = SINGLTN + in_name;
  this._interval = in_interval;
  this._refresh = Math.floor(in_interval*0.9);

    // instance guid
  this._guid = 'xxxxxxxxxx'.replace(/x/g,function(){return Math.floor(Math.random()*16).toString(16)});
};

Singleton.prototype.start = function(masterCallback){

  var that = this;

  // we store the master callback to avoid a closure for the
  // settimeout.
  this._masterCB = this._masterCB || masterCallback;

  if(that._timeout){
    clearTimeout(that._timeout);
    that._timeout = null;
  }

  this._redisConnection.setnx([this._key, this._guid], function(error, success){
    if(error || !success){

      that._timeout = setTimeout(that.start.bind(that),that._interval);

      if(!that._queued){
        that._redisConnection.blpop(QUEUE, 0, that.start.bind(that));
        that._queued = true;
      }

    } else if(success){
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
  var that = this;

  this._redisConnection.del([this._key], function(error){
    that._redisConnection.rpush(QUEUE, that._guid);
  });
};

module.exports = Singleton;