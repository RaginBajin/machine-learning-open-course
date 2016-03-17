
var _           = require("underscore");
var Recommender = require("./Recommender");
var data        = require("./food.json");

console.log("============\n");

var model = new Recommender();
model.train(data);

//console.log("\n Bag of Words:\n", model.bagOfWords);
//console.log("\n Similarity Graph:\n", model.similarityGraph);

_.times(10, function(){
    var random = _.random(0, data.length);
    var oldPost = data[random];
    
    reply = model.test(oldPost);
    console.log("\n recommend for known post:\n", oldPost, "\n\t", reply);
});
    
var newPost = {
    "id"      : "a-new-post",
    //"content" : "We invented a pasta dish with bacon, black pepper, eggs and cheese"
    "content" : "tagliolini with nutella"
};
reply = model.test(newPost);
console.log("\n recommend for new post \n", newPost, "\n\t", reply);

