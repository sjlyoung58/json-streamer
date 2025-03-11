// Description: Filters spansh galaxy.json.gz for systems within 'n' LY of Sol (the bubble)
//              extracting a subset of body data for each system and discarding the stations data

// Usage: 
//     1 bash - create filtered subset of json dump (in project directory)
// cat galaxy.json.gz | gunzip | sed '1d;$d;s/,$//' | node main.mjs > bubbleBodies500.json
//
//     2 sql - create staging table to load subset into
// create table ed_stg.stg_bubble500 (body_data jsonb);
//
//     3 postgres COPY - load subset into staging table
// cxnstring='postgresql://postgres:<password>@<address>:<port>/postgres'
//
// date
// cat bubbleBodies500.json | psql $cxnstring -c "COPY ed_stg.stg_bubble500 (body_data) FROM STDIN"
// date

import { createInterface } from "node:readline"

let accepted = 0;
let rejected = 0;

const bubsize = 500
const xmin = bubsize * -1;
const xmax = bubsize;
const ymin = bubsize * -1;
const ymax = bubsize;
const zmin = bubsize * -1;
const zmax = bubsize;

let minx = 0;
let maxx = 0;
let miny = 0;
let maxy = 0;
let minz = 0;
let maxz = 0;

for await (const line of createInterface({ input: process.stdin })) {
  processSystem(line);
};

function processSystem(line) {
  const system = JSON.parse(line);
  const populated = system.population > 0 ? system.population : 0; // catches undefined too
  const x = system.coords.x;
  const y = system.coords.y;
  const z = system.coords.z;
  const bubble = x > xmin && x < xmax && y > ymin && y < ymax && z > zmin && z < zmax;

  const keep = bubble && !populated;

  if (keep) {
    minx = x < minx ? x : minx;
    maxx = x > maxx ? x : maxx;
    miny = y < miny ? y : miny;
    maxy = y > maxy ? y : maxy;
    minz = z < minz ? z : minz;
    maxz = z > maxz ? z : maxz;
    // console.log(`${system.id64} ${system.name} ${system.population} x:${x} y:${y} z:${z}`);
    console.log(JSON.stringify(getBodyJson(system)));
    accepted++;
  } else {
    rejected++;
  }
}

function getBodyJson(fullSysData) {
  return {
    id64: fullSysData.id64,
    name: fullSysData.name,
    population: fullSysData.population,
    coords: {
      x: fullSysData.coords.x,
      y: fullSysData.coords.y,
      z: fullSysData.coords.z
    },
    bodies: getBodySubset(fullSysData.bodies)
  };
}

function getBodySubset(bodyData) {
  return bodyData.map(body => {
    const { stations, ...rest } = body;
    return {
      id64: rest.id64,
      bodyId: rest.bodyId,
      name: rest.name,
      type: rest.type,
      subType: rest.subType,
      isLandable: rest.isLandable,
      isTerraformable: rest.terraformingState === 'Terraformable'?true:false,
      distanceToArrival: rest.distanceToArrival,
      parents: rest.parents
    };
  });
}
