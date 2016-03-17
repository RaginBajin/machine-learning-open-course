var natural = require('natural'),
    classifier = new natural.BayesClassifier();


classifier.addDocument('my unit-tests failed.', 'software');
classifier.addDocument('tried the program, but it was buggy.', 'software');
classifier.addDocument('the drive has a 2TB capacity.', 'hardware');
classifier.addDocument('i need a new power supply.', 'hardware');

classifier.train();

classifier.save('classifier.json', function(err, classifier) {
    // the classifier is saved to the classifier.json file!
    
    natural.BayesClassifier.load('classifier.json', null, function(err, classifier) {
        console.log(classifier.classify('did you buy a new drive?'));
    });
});