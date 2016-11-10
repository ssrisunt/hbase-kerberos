package com.srisunt.pig;

import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.test.Util;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class BuildingAvgPigTest {
    static final String BUILDING_SCHEMA = "(buildingid:int, buildingmgr:chararray, buildingage:double, hvacproduct:chararray, country:chararray)";
    static final String CLUSTER_BUILDING_CSV = "pig-cluster/building.csv";
    static final String AVG_AGE_SCRIPT = "src/main/pig/building-avg-age.pig";

    static Cluster cluster;

    String[] args = new String[]{
            "input=" + CLUSTER_BUILDING_CSV,
            "output=output"
    };

    PigTest pigTest;

    @BeforeClass
    public static void init() throws Exception {
        System.getProperties().setProperty("pigunit.exectype", Util.getLocalTestMode().toString());
        cluster = PigTest.getCluster();

        cluster.update(
                new Path("src/test/resources/data/test-building.csv"),
                new Path(CLUSTER_BUILDING_CSV));
    }

    @Before
    public void setUp() throws Exception {
        pigTest = new PigTest(AVG_AGE_SCRIPT, args);
    }

    @Test
    public void makeSureTestDataFromFileProperlyProducesAverage() throws Exception {
        pigTest.assertOutput("building_avg", new String[]{"(24.25)"});
    }

    @Test
    public void calculatingBuildingAverageShouldProperlyStoreAverage() throws Exception {
        String[] building = new String[]{
                "1,m1,20,p1,c1",
                "2,m2,10,p2,c2",
                "3,m3,15,p3,c3",
                "4,m4,10,p4,c4",
                "5,m5,40,p5,c5"
        };

        pigTest.mockAlias("building", building, BUILDING_SCHEMA, ",");
        pigTest.assertOutput("building_avg", new String[]{"(19.0)"});
    }
}
