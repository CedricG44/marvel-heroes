const csv = require("csv-parser");
const fs = require("fs");

const { Client } = require("@elastic/elasticsearch");
const client = new Client({ node: "http://localhost:9200" });
const heroesIndexName = "heroes";

async function run() {
  // Création de l'indice
  await client.indices.create({ index: heroesIndexName });

  // Ajout du mapping pour la suggestion
  await client.indices.putMapping({
    index: heroesIndexName,
    body: {
      properties: {
        suggest: {
          type: "completion"
        }
      }
    }
  });

  // Lecture du fichier CSV
  const heroes = [];
  fs.createReadStream("all-heroes.csv")
    .pipe(csv())
    .on("data", data => {
      const hero = {
        id: data.id,
        name: data.name,
        description: data.description,
        imageUrl: data.imageUrl,
        universe: data.universe,
        gender: data.gender,
        aliases: data.aliases,
        secretIdentities: data.secretIdentities,
        partners: data.partners,
        suggest: [
          {
            input: data.name,
            weight: 8
          },
          {
            input: data.aliases,
            weight: 4
          },
          {
            input: data.secretIdentities,
            weight: 4
          },
          {
            input: data.description,
            weight: 2
          },
          {
            input: data.partners,
            weight: 1
          }
        ]
      };
      heroes.push(hero);
    })
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
    const {
      id,
      name,
      description,
      imageUrl,
      universe,
      gender,
      aliases,
      secretIdentities,
      partners,
      suggest
    } = hero;
    acc.push({
      index: { _index: heroesIndexName, _type: "_doc", _id: id }
    });
    acc.push({
      name,
      description,
      imageUrl,
      universe,
      gender,
      aliases,
      secretIdentities,
      partners,
      suggest
    });
    return acc;
  }, []);

  return { body };
}

run().catch(console.error);
