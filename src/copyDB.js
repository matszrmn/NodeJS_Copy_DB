let urlSource = "mongodb://localhost:27017/";
let dbSource = "ep2";
let MongoClientSource = require('mongodb').MongoClient;

let urlTarget = "mongodb://localhost:27017/";
let dbTarget = "ep3";
let MongoClientTarget = require('mongodb').MongoClient;

let chunks = 100;
let timeOut = 7000;


let totalReplicas = 0;
let completedReplicas = 0;


function copyDocumentsInChunks(skip, limit, count, collectionSource) {
    if(skip >= count) {
        completedReplicas++;
        console.log("Copy of '" + collectionSource + "' completed! (" 
                    + completedReplicas + " of " + totalReplicas + " collections)");
        if(totalReplicas == completedReplicas) process.exit(); // Remove this line if there is a trouble
        return;
    }
    console.log("Written " + skip + " of " + count + " documents from '" + collectionSource + "'");
    MongoClientSource.connect(urlSource, { useNewUrlParser: true }, function(error, mongo) {
        if (error) throw error;
        
        let db = mongo.db(dbSource);
        db.collection(collectionSource).find({}).sort({_id:1}).skip(skip).limit(limit).toArray(function(err, result) {
            if (err) throw err;
            
            insertDocuments(result, collectionSource);
            setTimeout(copyDocumentsInChunks, timeOut, skip + limit, limit, count, collectionSource);
            //copyDocumentsInChunks(skip + limit, limit, count, collectionSource);
            mongo.close();
        });
    });
}
function insertDocuments(documents, collectionTarget) {
    MongoClientTarget.connect(urlTarget, { useNewUrlParser: true }, function(error, mongo) {
        if (error) throw error;
        
        let db = mongo.db(dbTarget);
        db.collection(collectionTarget).insertMany(documents, function(err, result) {
            if(err) throw err;
            mongo.close();
        });
    });
}
function countDocumentsDBSource(callback, collectionSource, limit) {
    MongoClientSource.connect(urlSource, { useNewUrlParser: true }, function(error, mongo) {
        if (error) throw error;
        
        let db = mongo.db(dbSource);
        db.collection(collectionSource).countDocuments().then((count) => {
            callback(0, limit, count, collectionSource);
            mongo.close();
        });
    });
}
function listCollectionsDBSource(limit) {
    console.log("");
    MongoClientSource.connect(urlSource, { useNewUrlParser: true }, function(error, mongo) {
        if (error) throw error;
        
        let db = mongo.db(dbSource);
        db.listCollections().toArray(function(err, collections) {
            if(err) throw err;
            for(let i in collections) {
                totalReplicas = collections.length - 1; // Length excluding "system.indexes" collection
                if(collections[i].name !== 'system.indexes') {
                    countDocumentsDBSource(copyDocumentsInChunks, collections[i].name, limit);
                }
            }
            mongo.close();
        });
    });
}

listCollectionsDBSource(chunks);
