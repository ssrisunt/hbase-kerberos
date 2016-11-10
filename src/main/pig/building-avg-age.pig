building = LOAD '$input' USING PigStorage(',') AS (buildingid:int, buildingmgr:chararray, buildingage:double, hvacproduct:chararray, country:chararray);
building_group = GROUP building ALL;
building_avg = FOREACH building_group GENERATE AVG(building.buildingage);
STORE building_avg INTO '$output';
