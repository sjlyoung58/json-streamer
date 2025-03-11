with jdata as (
select '[{"id64": 36036064774399400, "name": "ICZ ZK-X b1-3 A", "type": "Star", "bodyId": 1, "parents": [{"Null": 0}], "subType": "M (Red dwarf) Star", "distanceToArrival": 0}, {"id64": 72064861793363380, "name": "ICZ ZK-X b1-3 B", "type": "Star", "bodyId": 2, "parents": [{"Null": 0}], "subType": "L (Brown dwarf) Star", "distanceToArrival": 60424.593133}, {"id64": 108093658812327340, "name": "ICZ ZK-X b1-3 A 1", "type": "Planet", "bodyId": 3, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Rocky Ice world", "isLandable": true, "distanceToArrival": 348.815358}, {"id64": 144122455831291300, "name": "ICZ ZK-X b1-3 A 2", "type": "Planet", "bodyId": 4, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Rocky Ice world", "isLandable": false, "distanceToArrival": 489.416915}, {"id64": 180151252850255260, "name": "ICZ ZK-X b1-3 A 2 a", "type": "Planet", "bodyId": 5, "parents": [{"Planet": 4}, {"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 489.602728}, {"id64": 216180049869219230, "name": "ICZ ZK-X b1-3 A 3", "type": "Planet", "bodyId": 6, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Rocky Ice world", "isLandable": true, "distanceToArrival": 686.021549}, {"id64": 252208846888183200, "name": "ICZ ZK-X b1-3 A 4", "type": "Planet", "bodyId": 7, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 935.657828}, {"id64": 288237643907147200, "name": "ICZ ZK-X b1-3 barycentre 8", "type": "Barycentre", "bodyId": 8}, {"id64": 324266440926111170, "name": "ICZ ZK-X b1-3 A 5", "type": "Planet", "bodyId": 9, "parents": [{"Null": 8}, {"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 1356.071274}, {"id64": 360295237945075140, "name": "ICZ ZK-X b1-3 A 6", "type": "Planet", "bodyId": 10, "parents": [{"Null": 8}, {"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 1357.782384}, {"id64": 396324034964039100, "name": "ICZ ZK-X b1-3 A 7", "type": "Planet", "bodyId": 11, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 2077.457998}, {"id64": 432352831983003100, "name": "ICZ ZK-X b1-3 A 8", "type": "Planet", "bodyId": 12, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": false, "distanceToArrival": 2956.081759}, {"id64": 468381629001967040, "name": "ICZ ZK-X b1-3 A 8 a", "type": "Planet", "bodyId": 13, "parents": [{"Planet": 12}, {"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 2952.072466}, {"id64": 504410426020931000, "name": "ICZ ZK-X b1-3 A 9", "type": "Planet", "bodyId": 14, "parents": [{"Star": 1}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 5108.279367}, {"id64": 540439223039895000, "name": "ICZ ZK-X b1-3 B 1", "type": "Planet", "bodyId": 15, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 60252.171578}, {"id64": 576468020058858900, "name": "ICZ ZK-X b1-3 B 2", "type": "Planet", "bodyId": 16, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 59894.791556}, {"id64": 612496817077822800, "name": "ICZ ZK-X b1-3 B 2 a", "type": "Planet", "bodyId": 17, "parents": [{"Planet": 16}, {"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 59894.026195}, {"id64": 648525614096786800, "name": "ICZ ZK-X b1-3 B 3", "type": "Planet", "bodyId": 18, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 59715.00212}, {"id64": 684554411115750800, "name": "ICZ ZK-X b1-3 B 4", "type": "Planet", "bodyId": 19, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": false, "distanceToArrival": 60251.411448}, {"id64": 720583208134714800, "name": "ICZ ZK-X b1-3 B 5", "type": "Planet", "bodyId": 20, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 58751.084583}, {"id64": 756612005153678700, "name": "ICZ ZK-X b1-3 B 5 a", "type": "Planet", "bodyId": 21, "parents": [{"Planet": 20}, {"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 58748.509111}, {"id64": 792640802172642700, "name": "ICZ ZK-X b1-3 B 6", "type": "Planet", "bodyId": 22, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 62632.977876}, {"id64": 828669599191606700, "name": "ICZ ZK-X b1-3 B 7", "type": "Planet", "bodyId": 23, "parents": [{"Star": 2}, {"Null": 0}], "subType": "Icy body", "isLandable": true, "distanceToArrival": 60806.475538}]'::jsonb
as body_data::jsonb
)
SELECT (body_data->>'id64')::bigint as system_address,
       body_data->>'name' as system,
       body_data->>'population' as population,
       (body_data->'coords'->>'x')::float8 as x,
       (body_data->'coords'->>'y')::float8 as y,
       (body_data->'coords'->>'z')::float8 as z,
       jsonb_array_length(body_data->'bodies') as bodies,
       jsonb_path_query(body_data, '$[*].type') as type
   from jdata;