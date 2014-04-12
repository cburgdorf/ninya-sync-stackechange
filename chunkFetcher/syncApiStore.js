var Q = require('q');
var SyncApi = require('ninya-sync-api');

var SyncApiStore = function(syncService){
    var self = {};
    var data = [];
    var length = 0;
    var counter = 0;

    self.getAll = function(){
        return [];
    };

    self.getLength = function(){
        return length;
    };

    self.exists = function(id){
        return syncService.hasEntity(id);
    };

    self.append = function(chunk){
        length += chunk.length;

        chunk.forEach(function(entity){
            syncService.updateEntity(entity._ninya_id, entity);
        });
    };

    return self;
};

module.exports = SyncApiStore;
