var _ = require("underscore");

var Recommender = function(){
    this.bagOfWords      = {};
    this.similarityGraph = {};
}

Recommender.prototype.train = function(data){
    
    var recommender = this;
    
    _.each(data, function(d, i){
        console.log(i + "/" +  data.length);
        recommender.trainOne(d);
    });
}

Recommender.prototype.trainOne = function(d){
    
    var recommender = this;
    
    var bow = recommender.tokenize(d.content);
    recommender.bagOfWords[d.id] = bow;
    recommender.similarityGraph[d.id] = {};
    
    _.each(recommender.bagOfWords, function(otherBow, otherId){
        if(d.id !== otherId){
            
            var simil = recommender.similarity(bow, otherBow);
            
            recommender.similarityGraph[d.id][otherId] = simil;
            recommender.similarityGraph[otherId][d.id] = simil;
        }
    });
}

Recommender.prototype.similarity = function(a, b){
    
    var nIntersection = _.intersection(a, b).length;
    var nUnion        = _.union(a, b).length;
    
    return nIntersection/nUnion;
}

Recommender.prototype.tokenize = function(text){
    
    text = text.replace(/[.,\/#!$%\^&\*;:{}=\-_`~()]/g, "");
    
    var tokens = text.toLowerCase().split(" ");
    var stopwords = [
        'about', 'after', 'all', 'also', 'am', 'an', 'and', 'another', 'any', 'are', 'as', 'at', 'be',
        'because', 'been', 'before', 'being', 'between', 'both', 'but', 'by', 'came', 'can',
        'come', 'could', 'did', 'do', 'each', 'for', 'from', 'get', 'got', 'has', 'had',
        'he', 'have', 'her', 'here', 'him', 'himself', 'his', 'how', 'if', 'in', 'into',
        'is', 'it', 'like', 'make', 'many', 'me', 'might', 'more', 'most', 'much', 'must',
        'my', 'never', 'now', 'of', 'on', 'only', 'or', 'other', 'our', 'out', 'over',
        'said', 'same', 'see', 'should', 'since', 'some', 'still', 'such', 'take', 'than',
        'that', 'the', 'their', 'them', 'then', 'there', 'these', 'they', 'this', 'those',
        'through', 'to', 'too', 'under', 'up', 'very', 'was', 'way', 'we', 'well', 'were',
        'what', 'where', 'which', 'while', 'who', 'with', 'would', 'you', 'your',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
        'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '$', '1',
        '2', '3', '4', '5', '6', '7', '8', '9', '0', '_'];
    tokens = _.difference(tokens, stopwords);
    
    return _.sortBy( _.unique(tokens) );
}

Recommender.prototype.test = function(post){
    
    if(!this.similarityGraph[post.id]){
        this.trainOne(post);
    }
    
    var alternatives = _.map(this.similarityGraph[post.id], function(s,id){
        return {
            "id": id,
            "similarity": s
        };
    });
    
    var sortedAlternatives = _.sortBy(alternatives, function(a){
        return a.similarity;
    });
    
    return _.first( sortedAlternatives.reverse(), 5);
}

module.exports = Recommender;