package bcg.gamma


import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger


object caseStudyBCG {
  def main(args: Array[String]): Unit = {

    val A1OutputPath = "/user/HAASBCG/outbound/Analytics1"
    val A2OutputPath = "/user/HAASBCG/outbound/Analytics2"
    val A3OutputPath = "/user/HAASBCG/outbound/Analytics3"
    val A4OutputPath = "/user/HAASBCG/outbound/Analytics4"
    val A5OutputPath = "/user/HAASBCG/outbound/Analytics5"
    val A6OutputPath = "/user/HAASBCG/outbound/Analytics6"
    val A7OutputPath = "/user/HAASBCG/outbound/Analytics7"
    val A8OutputPath = "/user/HAASBCG/outbound/Analytics8"


    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("caseStudyBCG")
      .getOrCreate()


    try{

      Logger.getLogger(getClass).info("Loading and Transforming DataFrame")

      val personSchema = StructType(Array(
        StructField("CRASH_ID",IntegerType,true),
        StructField("UNIT_NBR",IntegerType,true),
        StructField("PRSN_NBR",IntegerType,true),
        StructField("PRSN_TYPE_ID", StringType, true),
        StructField("PRSN_OCCPNT_POS_ID", StringType, true),
        StructField("PRSN_INJRY_SEV_ID", StringType, true),
        StructField("PRSN_AGE",IntegerType, true),
        StructField("PRSN_ETHNICITY_ID",StringType,true),
        StructField("PRSN_GNDR_ID",StringType,true),
        StructField("PRSN_EJCT_ID", StringType, true),
        StructField("PRSN_REST_ID", StringType, true),
        StructField("PRSN_AIRBAG_ID",StringType,true),
        StructField("PRSN_HELMET_ID",StringType,true),
        StructField("PRSN_SOL_FL",StringType,true),
        StructField("PRSN_ALC_SPEC_TYPE_ID", StringType, true),
        StructField("PRSN_ALC_RSLT_ID", StringType, true),
        StructField("PRSN_BAC_TEST_RSLT", DoubleType, true),
        StructField("PRSN_DRG_SPEC_TYPE_ID",StringType,true),
        StructField("PRSN_DRG_RSLT_ID",StringType,true),
        StructField("DRVR_DRG_CAT_1_ID",StringType,true),
        StructField("PRSN_DEATH_TIME", StringType, true),
        StructField("INCAP_INJRY_CNT", IntegerType, true),
        StructField("NONINCAP_INJRY_CNT", IntegerType, true),
        StructField("POSS_INJRY_CNT",IntegerType,true),
        StructField("NON_INJRY_CNT",IntegerType,true),
        StructField("UNKN_INJRY_CNT",IntegerType,true),
        StructField("TOT_INJRY_CNT", IntegerType, true),
        StructField("DEATH_CNT", IntegerType, true),
        StructField("DRVR_LIC_TYPE_ID",StringType,true),
        StructField("DRVR_LIC_STATE_ID",StringType,true),
        StructField("DRVR_LIC_CLS_ID",StringType,true),
        StructField("DRVR_ZIP", IntegerType, true)
      ))

      val unitSchema = StructType(Array(
        StructField("CRASH_ID",IntegerType,true),
        StructField("UNIT_NBR",IntegerType,true),
        StructField("UNIT_DESC_ID",StringType,true),
        StructField("VEH_PARKED_FL", StringType, true),
        StructField("VEH_HNR_FL", StringType, true),
        StructField("VEH_LIC_STATE_ID", StringType, true),
        StructField("VIN",StringType, true),
        StructField("VEH_MOD_YEAR",IntegerType,true),
        StructField("VEH_COLOR_ID",StringType,true),
        StructField("VEH_MAKE_ID", StringType, true),
        StructField("VEH_MOD_ID", StringType, true),
        StructField("VEH_BODY_STYL_ID",StringType,true),
        StructField("EMER_RESPNDR_FL",StringType,true),
        StructField("OWNR_ZIP",IntegerType,true),
        StructField("FIN_RESP_PROOF_ID", StringType, true),
        StructField("FIN_RESP_TYPE_ID", StringType, true),
        StructField("VEH_DMAG_AREA_1_ID", DoubleType, true),
        StructField("VEH_DMAG_SCL_1_ID",StringType,true),
        StructField("FORCE_DIR_1_ID",IntegerType,true),
        StructField("VEH_DMAG_AREA_2_ID",StringType,true),
        StructField("VEH_DMAG_SCL_2_ID", StringType, true),
        StructField("FORCE_DIR_2_ID", IntegerType, true),
        StructField("VEH_INVENTORIED_FL", StringType, true),
        StructField("VEH_TRANSP_NAME",StringType,true),
        StructField("VEH_TRANSP_DEST",StringType,true),
        StructField("CONTRIB_FACTR_1_ID",StringType,true),
        StructField("CONTRIB_FACTR_2_ID", StringType, true),
        StructField("CONTRIB_FACTR_P1_ID", StringType, true),
        StructField("VEH_TRVL_DIR_ID",StringType,true),
        StructField("FIRST_HARM_EVT_INV_ID",StringType,true),
        StructField("INCAP_INJRY_CNT",IntegerType,true),
        StructField("NONINCAP_INJRY_CNT", IntegerType, true),
        StructField("POSS_INJRY_CNT", IntegerType, true),
        StructField("NON_INJRY_CNT",IntegerType,true),
        StructField("UNKN_INJRY_CNT", IntegerType, true),
        StructField("TOT_INJRY_CNT", IntegerType, true),
        StructField("DEATH_CNT", IntegerType, true)
      ))

      val Person_use_DF = spark.read.format("csv").option("header","true").schema(personSchema).load("/user/HAASBCG/inbound/Primary_Person_use.csv")
      val Units_use_DF = spark.read.format("csv").option("header","true").schema(unitSchema).load("/user/HAASBCG/inbound/Units_use.csv")
      val Damages_use_DF = spark.read.format("csv").option("header","true").load("/user/HAASBCG/inbound/Damages_use.csv")
      val PU_JoinedDF = Person_use_DF.as("P").join(Units_use_DF.as("U"), col("P.CRASH_ID") === col("U.CRASH_ID") && col("P.UNIT_NBR") === col("U.UNIT_NBR") ,"inner").dropDuplicates()
      val DU_JoinedDF = Units_use_DF.as("U").join(Damages_use_DF.as("D"), col("U.CRASH_ID") === col("D.CRASH_ID"),"inner").dropDuplicates()

      Logger.getLogger(getClass).info("Counting No of persons killed are male")
      val male_killed = Person_use_DF.filter($"PRSN_GNDR_ID"==="MALE" && $"PRSN_INJRY_SEV_ID"==="KILLED").count()

      Logger.getLogger(getClass).info("Counting No of two wheelers booked for crashes")
      val twoWheeler = Units_use_DF.filter($"VEH_BODY_STYL_ID".isin("POLICE MOTORCYCLE","MOTORCYCLE") || $"UNIT_DESC_ID"==="PEDALCYCLIST" ).count()

      Logger.getLogger(getClass).info("Finding State that has highest number of accidents in which females are involved")
      val female_state_DF = Person_use_DF.filter($"PRSN_GNDR_ID"==="FEMALE").groupBy("DRVR_LIC_STATE_ID").count()

      //Filtering state with  highest number of accidents in which females are involved based on DRVR_LIC_STATE_ID with max count
      val higheststate = female_state_DF.filter($"count"===female_state_DF.agg(max("count")).collect()(0)(0)).collect()(0)(0)

      Logger.getLogger(getClass).info("Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death")
      //Total Injury count does not account take ino account deaths so to remove ambiguity i have added both the columns
      val df_tot_injry = Units_use_DF.withColumn("DEATH+INJURY",$"TOT_INJRY_CNT"+$"DEATH_CNT")
      val df_tot_injry_count = df_tot_injry.filter($"VEH_MAKE_ID"=!="NA").groupBy("VEH_MAKE_ID").sum("DEATH+INJURY").withColumnRenamed("sum(DEATH+INJURY)","Total_Count")
      //Ranking and Filetring
      val windowSpec  = Window.orderBy(desc("Total_Count"))
      val df_car_rank = df_tot_injry_count.select($"VEH_MAKE_ID", $"Total_Count",row_number.over(windowSpec).as("Rank"))
      val df_top_VEH_MAKE_ID = df_car_rank.filter($"Rank">=5 && $"Rank"<=15).select($"VEH_MAKE_ID",$"Rank")

      Logger.getLogger(getClass).info("Top ethnic user group of each unique body style")
      val ethnic_veh_DF = PU_JoinedDF.groupBy("P.PRSN_ETHNICITY_ID","U.VEH_BODY_STYL_ID").count()
      val windowSpec  = Window.partitionBy("U.VEH_BODY_STYL_ID").orderBy(desc("count"))
      val top_ethnic_veh_DF = ethnic_veh_DF.withColumn("row_number",row_number.over(windowSpec)).filter($"row_number" === 1).select($"P.PRSN_ETHNICITY_ID",$"U.VEH_BODY_STYL_ID")

      Logger.getLogger(getClass).info("Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor")
      //Alcohol may be mentioned as a contributing factor in any of the CONTRIB_FACTR column hence considering all 3
      val alcohol_zip_DF = PU_JoinedDF.filter($"U.CONTRIB_FACTR_1_ID".like("%ALCOHOL%") || $"U.CONTRIB_FACTR_2_ID".like("%ALCOHOL%") || $"U.CONTRIB_FACTR_P1_ID".like("%ALCOHOL%")).groupBy("P.DRVR_ZIP").count().na.drop(Seq("P.DRVR_ZIP"))
      val windowSpec  = Window.orderBy(desc("count"))
      val top_alcohol_zip_DF = alcohol_zip_DF.withColumn("row_number",row_number.over(windowSpec)).filter($"row_number"<=5).select($"P.DRVR_ZIP")

      Logger.getLogger(getClass).info("Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level is above 4 and car avails Insurance")
      val distinct_crash = DU_JoinedDF.select(col("*"), col("U.VEH_DMAG_SCL_1_ID").substr(9,1).as("VEH_DAM_RATING_1").cast(IntegerType),col("U.VEH_DMAG_SCL_2_ID").substr(9,1).as("VEH_DAM_RATING_2").cast(IntegerType)).filter($"D.DAMAGED_PROPERTY".isNull && $"VEH_DAM_RATING_1">4 || $"VEH_DAM_RATING_2">4 && $"U.FIN_RESP_TYPE_ID".like("%INSURANCE%")).distinct().count()

      Logger.getLogger(getClass).info("Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and licensed with the Top 25 states with highest no of offences")
      val unlicensedlist = Seq("NA","UNKNOWN","UNLICENSED")

      //Fetching top 10 vehicle colors in topcolorlist
      val colorDF = Units_use_DF.groupBy("VEH_COLOR_ID").count()
      val windowSpec  = Window.orderBy(desc("count"))
      val topcolorsDF = colorDF.withColumn("row_number",row_number.over(windowSpec)).filter($"row_number"<=10).select("VEH_COLOR_ID")
      val topcolorlist = topcolorsDF.as[String].collect.toList

      //Fetching top 25 states with highest offences in topstatelist
      val topstate = Units_use_DF.groupBy("VEH_LIC_STATE_ID").count().filter($"VEH_LIC_STATE_ID"=!="NA")
      val topstateDF = topstate.withColumn("row_number",row_number.over(windowSpec)).filter($"row_number"<=25).select("VEH_LIC_STATE_ID")
      val topstatelist = topstateDF.as[String].collect.toList

      //Filtering speeding related offences,licensed Drivers,top 10 used vehicle colours & top 25 states with highest number of offences
      val base = PU_JoinedDF.filter($"U.CONTRIB_FACTR_1_ID".like("%SPEED%") || $"U.CONTRIB_FACTR_2_ID".like("%SPEED%") || $"U.CONTRIB_FACTR_P1_ID".like("%SPEED%")).filter(!col("P.DRVR_LIC_TYPE_ID").isin(unlicensedlist:_*)).filter(col("U.VEH_COLOR_ID").isin(topcolorlist:_*)).filter(col("U.VEH_LIC_STATE_ID").isin(topstatelist:_*)).groupBy("U.VEH_MAKE_ID").count()

      //Ranking and filtering
      val top_DF = base.withColumn("Rank",row_number.over(windowSpec)).filter($"Rank"<=5).select("VEH_MAKE_ID","Rank")


      Logger.getLogger(getClass).info("Cleaning up the output paths before writing data")
      CleanUp(spark,A1OutputPath)
      CleanUp(spark,A2OutputPath)
      CleanUp(spark,A3OutputPath)
      CleanUp(spark,A4OutputPath)
      CleanUp(spark,A5OutputPath)
      CleanUp(spark,A6OutputPath)
      CleanUp(spark,A7OutputPath)
      CleanUp(spark,A8OutputPath)

      Logger.getLogger(getClass).info("Writing data to hdfs")

      spark.sparkContext.parallelize(Seq(male_killed)).coalesce(1).saveAsTextFile(A1OutputPath)
      spark.sparkContext.parallelize(Seq(twoWheeler)).coalesce(1).saveAsTextFile(A2OutputPath)
      spark.sparkContext.parallelize(Seq(higheststate)).coalesce(1).saveAsTextFile(A3OutputPath)
      df_top_VEH_MAKE_ID.repartition(1).write.format("csv").option("header","true").save(A4OutputPath)
      top_ethnic_veh_DF.repartition(1).write.format("csv").option("header","true").save(A5OutputPath)
      top_alcohol_zip_DF.repartition(1).write.format("csv").option("header","true").save(A6OutputPath)
      spark.sparkContext.parallelize(Seq(distinct_crash)).coalesce(1).saveAsTextFile(A7OutputPath)
      top_DF.repartition(1).write.format("csv").option("header","true").save(A8OutputPath)

      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val file1 = fs.globStatus(new Path(A1OutputPath + "/part*"))(0).getPath().getName()
      val file2 = fs.globStatus(new Path(A2OutputPath + "/part*"))(0).getPath().getName()
      val file3 = fs.globStatus(new Path(A3OutputPath + "/part*"))(0).getPath().getName()
      val file4 = fs.globStatus(new Path(A4OutputPath + "/part*"))(0).getPath().getName()
      val file5 = fs.globStatus(new Path(A5OutputPath + "/part*"))(0).getPath().getName()
      val file6 = fs.globStatus(new Path(A6OutputPath + "/part*"))(0).getPath().getName()
      val file7 = fs.globStatus(new Path(A7OutputPath + "/part*"))(0).getPath().getName()
      val file8 = fs.globStatus(new Path(A8OutputPath + "/part*"))(0).getPath().getName()

      fs.rename(new Path(A1OutputPath + "/" +file1), new Path(A1OutputPath + "/Analytics1_Results.csv"))
      fs.rename(new Path(A2OutputPath + "/" +file2), new Path(A2OutputPath + "/Analytics2_Results.csv"))
      fs.rename(new Path(A3OutputPath + "/" +file3), new Path(A3OutputPath + "/Analytics3_Results.csv"))
      fs.rename(new Path(A4OutputPath + "/" +file4), new Path(A4OutputPath + "/Analytics4_Results.csv"))
      fs.rename(new Path(A5OutputPath + "/" +file5), new Path(A5OutputPath + "/Analytics5_Results.csv"))
      fs.rename(new Path(A6OutputPath + "/" +file6), new Path(A6OutputPath + "/Analytics6_Results.csv"))
      fs.rename(new Path(A7OutputPath + "/" +file7), new Path(A7OutputPath + "/Analytics7_Results.csv"))
      fs.rename(new Path(A8OutputPath + "/" +file8), new Path(A8OutputPath + "/Analytics8_Results.csv"))



    }
    catch {
      case e: Throwable => e.printStackTrace()
    }

  }
  def CleanUp(spark : SparkSession, absOutputPath: String): Unit ={
    try{
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(new Path(absOutputPath))){
        fs.delete(new Path(absOutputPath))
      }catch{
        case e: Throwable =>
          e.printStackTrace()
          Logger.getLogger(getClass).error("Cannot remove directory because of exception : " + e.toString + " with stack-trace: "+e)
      }
    }
  }

}
