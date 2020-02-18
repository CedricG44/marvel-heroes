const csv = require("csv-parser");
const fs = require("fs");

const { Client } = require("@elastic/elasticsearch");
const client = new Client({ node: "http://localhost:9200" });
const heroesIndexName = "heroes";

async function run() {
  // Création de l'indice
  client.indices.create({ index: heroesIndexName }, (err, resp) => {
    if (err) {
      console.trace(err.message);
    }
  });

  // Lecture du fichier CSV
  const heroes = [];
  fs.createReadStream("all-heroes.csv")
    .pipe(csv())
    .on("data", data => heroes.push(data))
    .on("end", () => {
      client.bulk(createBulkInsertQuery(heroes), (err, resp) => {
        if (err) console.trace(err.message);
        else console.log(`${resp.body.items.length} héros insérés`);
        client.close();
      });
    });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans Elasticsearch
function createBulkInsertQuery(heroes) {
  const body = heroes.reduce((acc, hero) => {
    acc.push({
      index: { _index: heroesIndexName, _type: "_doc", _id: hero.id }
    });
    delete hero.id;
    acc.push(hero);
    return acc;
  }, []);

  return { body };
}

run().catch(console.error);
