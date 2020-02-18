var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

const insertHereos = (db, callback) => {
  const collection = db.collection(collectionName);
  const heroes = [];
  fs.createReadStream("all-heroes.csv")
    .pipe(csv())
    .on("data", data => heroes.push(data))
    .on("end", () => {
      collection.insertMany(heroes, (err, result) => {
        callback(result);
      });
      console.log("finished parsing heroes");
    });
};

MongoClient.connect(mongoUrl, (err, client) => {
  if (err) {
    console.error(err);
    throw err;
  }
  const db = client.db(dbName);
  insertHereos(db, result => {
    console.log(`${result.insertedCount} hereos inserted`);
    client.close();
  });
});
