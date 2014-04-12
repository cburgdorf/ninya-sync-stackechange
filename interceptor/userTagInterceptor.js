var Q = require('q');
var ChunkFetcher = require('../chunkFetcher/chunkFetcher.js');

var UserTagInterceptor = function(userStore, stackexchangeSite){

    var generateNinyaId = function(user) {
        return stackexchangeSite + '_' + user.user_id;
    };

    return function(users){

        if(users.length > 0){
            var lastUser = users[users.length - 1];

            return userStore
                .exists(generateNinyaId(lastUser))
                .then(function(exists){

                    if (exists){
                        console.log('chunk already exists...skipping');
                        return [];
                    }
                    else {
                        return Q.all(users.map(function(user){
                            return new ChunkFetcher({
                                url: 'http://api.stackexchange.com/2.2/users/' + user.user_id + '/top-answer-tags?site=' + stackexchangeSite,
                                key: 'items',
                                pageSize: 30,
                                maxLength: 30,
                                maxPage: 1,
                                waitAfterErrorMs: 1500
                            })
                            .fetch()
                            .then(function(userTags){
                                console.log('fetched ' + userTags.length + ' tags for user ' + user.user_id + ' at ' + new Date())

                                // no matter what is the actual id used by the StackExchange site,
                                // we use this one and will use that one for the actual id that gets
                                // feed to elasticsearch.
                                user._ninya_id = generateNinyaId(user);

                                user._ninya_location_lowercase = user.location && user.location.toLowerCase();
                                user._ninya_location = user.location;
                                user._ninya_site = stackexchangeSite;

                                user.top_tags = userTags;
                                return user;
                            });
                      }));
                    }
                });

        }
        else {
            return Q.fcall(function(){
                return users
            });
        }

    };
}

module.exports = UserTagInterceptor;
