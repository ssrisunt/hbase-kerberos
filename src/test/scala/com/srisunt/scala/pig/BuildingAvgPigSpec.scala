package com.srisunt.scala.pig

import org.apache.hadoop.fs.Path
import org.apache.pig.pigunit.{Cluster, PigTest}
import org.apache.pig.test.Util
import org.scalatest._

class BuildingAvgPigSpec extends FlatSpec with BeforeAndAfter {
  val buildingSchema = "(buildingid:int, buildingmgr:chararray, buildingage:double, hvacproduct:chararray, country:chararray)"
  val clusterBuildingCsv = "pig-cluster/building.csv"
  val avgAgeScript = "src/main/pig/building-avg-age.pig"
  val args: Array[String] = Array(
    s"input=$clusterBuildingCsv",
    "output=output"
  )

  System.getProperties.setProperty("pigunit.exectype", Util.getLocalTestMode.toString)
  val cluster: Cluster = PigTest.getCluster
  cluster.update(new Path("src/test/resources/data/test-building.csv"), new Path(clusterBuildingCsv))

  var pigTest: PigTest = _

  before {
    pigTest = new PigTest(avgAgeScript, args)
  }

  "Building Average Pig script" should "return the correct average for mock data from file" in {
    pigTest.assertOutput("building_avg", Array[String]("(24.25)"))
  }

  it should "properly generate average with mocked data" in {
    val building = Array[String](
      "1,m1,20,p1,c1",
      "2,m2,10,p2,c2",
      "3,m3,15,p3,c3",
      "4,m4,10,p4,c4",
      "5,m5,40,p5,c5"
    )

    pigTest.mockAlias("building", building, buildingSchema, ",")
    pigTest.assertOutput("building_avg", Array[String]("(19.0)"))
  }
}