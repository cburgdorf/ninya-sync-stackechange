/*
   options.elasticsearchEndpoint,
   options.index: 'production_v4',
   options.stackexchangeSite = 'stackoverflow,'
   options.maxEntityCount = 150000,
   options.pageSize = 10,
   options.maxRunTime = 9 * 60 * 1000
*/

function StackExchangeSync (options) {

    var syncApi = require('ninya-sync-api');

    var ChunkFetcher = require('./chunkFetcher/chunkFetcher.js');
    var SyncApiStore = require('./chunkFetcher/syncApiStore.js');
    var UserTagInterceptor = require('./interceptor/userTagInterceptor.js');
    var https = require('https');

    var infoConnectionFactory = new syncApi.factories.ElasticSearchConnectionFactory({
        elasticsearchEndpoint: options.elasticsearchEndpoint,
        index:                 options.index,
        type:                  'info'
    });

    var entityConnectionFactory = new syncApi.factories.ElasticSearchConnectionFactory({
        elasticsearchEndpoint: options.elasticsearchEndpoint,
        index:                 options.index,
        type:                  'user'
    });

    var _syncService = new syncApi.SyncService({
        syncInfoRepository: new syncApi.repositories.ElasticSearchRepository(infoConnectionFactory),
        entityRepository: new syncApi.repositories.ElasticSearchRepository(entityConnectionFactory),
        lastEntityResolver: new syncApi.resolvers.LowestReputationResolver(entityConnectionFactory)
    });

    var ConnectedSyncApiStore = function(){
        return new SyncApiStore(_syncService);
    };

    var PAGE_SIZE = options.pageSize,
        MAX_RUN_TIME_MS = options.maxRunTime;

    setTimeout(function () {
        console.log('reached maximum job uptime...going down.');
        // This is for safety. We don't won't multiple jobs to run at the same time.
        // In the worst case job A gets the resume point (e.g. 200 rep) at startup while
        // job B is just about to wipe out the data. Then job A would insert users with
        // rep < 200 AFTER job B already wiped the data. This would mean the sync would
        // be locked in < 200 rep land.
        // TODO: Figure out how to avoid multiple instances
        process.exit(0);
    }, MAX_RUN_TIME_MS);


    var getTarget = function(){
        return options.stackexchangeSite + '_production';
    };

    var rebuild = function(){

        _syncService
            .sync({ target: getTarget() })
            .then(function(syncInfo) {

                new ChunkFetcher({
                    url: 'http://api.stackexchange.com/2.2/users?order=desc&site=' + options.stackexchangeSite,
                    key: 'items',
                    pageSize: PAGE_SIZE,
                    maxLength: 20000,
                    interceptor: new UserTagInterceptor(new ConnectedSyncApiStore(), options.stackexchangeSite),
                    store: ConnectedSyncApiStore
                })
                .fetch()
                .then(function(users){
                    console.log(users);
                });
            });
    };

    var safeResume = function(){

        _syncService
            .sync({ target: getTarget() })
            .then(function(syncInfo){
                if (syncInfo.count >= options.maxEntityCount) {
                    _syncService
                        .remove()
                        .then(function(){
                            rebuild();
                        });
                }
                else if (syncInfo.count === 0) {
                    rebuild();
                }
                else {
                    resume();
                }
            }, function(error){
                console.log(error);
            });
    };

    var resume = function(){

        _syncService
            .sync({ target: getTarget() })
            .then(function(syncInfo){
                if (syncInfo.empty){
                    // this should be exceptional
                    rebuild();
                }
                else if(!syncInfo.data || !syncInfo.data.reputation){
                    throw new Error('Corrupt sync. Can not resolve reentry point');
                }
                else {
                    var reputation = syncInfo.data.reputation;

                    new ChunkFetcher({
                        url: 'http://api.stackexchange.com/2.2/users?order=desc&site=' + options.stackexchangeSite + '&max=' + reputation,
                        key: 'items',
                        pageSize: PAGE_SIZE,
                        maxLength: 20000,
                        interceptor: new UserTagInterceptor(new ConnectedSyncApiStore(), options.stackexchangeSite),
                        store: ConnectedSyncApiStore
                    })
                    .fetch()
                    .then(function(users){
                        console.log(users);
                    });
                }
            }, function (error) {
                console.log(error);
            });
    };

    return{
        resume: resume,
        safeResume: safeResume,
        rebuild: rebuild
    }
};

module.exports = StackExchangeSync;
