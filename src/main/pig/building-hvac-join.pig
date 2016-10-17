building = LOAD '${buildingPath}' USING PigStorage(',') AS (buildingid:int, buildingmgr:chararray, buildingage:int, hvacproduct:chararray, country:chararray);
hvac = LOAD '${hvacPath}' USING PigStorage(',') AS (date:chararray, time:chararray, targettemp:int, actualtemp:int, system:int, systemage:int, buildingid:int);

hvac_building = JOIN building BY buildingid, hvac BY buildingid;

