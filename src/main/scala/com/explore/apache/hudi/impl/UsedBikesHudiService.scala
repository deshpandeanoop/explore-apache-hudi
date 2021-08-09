package com.explore.apache.hudi.impl

import com.explore.apache.beans.UsedBike
import com.explore.apache.hudi.HudiService
import com.explore.apache.utils.SparkJobHelper
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, QuickstartUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

class UsedBikesHudiService extends Serializable with HudiService[UsedBike] {

  private val _inputCsvAbsPath = "<>"

  private val _hudiTableBasePath = "<>"

  private val _hudiTableName = "used_bikes"

  private val _usedBikesTemporaryView = "used_bikes_temporary_view"

  private val _colHudiCommitTime = "_hoodie_commit_time"

  def execute : Unit = {
    val sparkSession = SparkJobHelper.getLocalSparkSession("used-bike-hudi-service")
//    sparkSession.sparkContext.setLogLevel("ERROR")

//    createTableFromCsv(inputCsvAbsPath = _inputCsvAbsPath, hudiTableBasePath = _hudiTableBasePath, hudiTableName = _hudiTableName, sparkSession = sparkSession)

   showSnapShotView(_hudiTableBasePath, sparkSession)
//   upsertTable(getDummyUsedBikesListForUpsert, tableName = _hudiTableName, hudiBasePath = _hudiTableBasePath, sparkSession)

//     showIncrementalView(hudiTableBasePath = _hudiTableBasePath, sparkSession)

//    showPointInTimeView(_hudiTableBasePath, sparkSession)
//    deleteDataFromTable(getDummyUsedBikesListForDelete, _hudiTableBasePath, _hudiTableName, sparkSession)
//    showSnapShotView(_hudiTableBasePath, sparkSession)
  }

  override def createTableFromCsv(inputCsvAbsPath: String,
                                  hudiTableBasePath : String,
                                  hudiTableName : String,
                                  sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession
      .read
      .option("header", "false")
      .csv(inputCsvAbsPath)
      .map(row => UsedBike(
        bikeName = row.getString(0),
        price = row.getString(1),
        city = row.getString(2),
        kmsDriven = row.getString(3),
        owner = row.getString(4),
        age = row.getString(5),
        power = row.getString(6),
        brand = row.getString(7)))
      .write
      .format("hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "bikeName")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "brand")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "city")
      .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
      .mode(SaveMode.Overwrite)
      .save(hudiTableBasePath)
  }

  override def showSnapShotView(hudiTablePath: String, sparkSession: SparkSession): Unit = {
    sparkSession
      .read
      .format("hudi")
      .load(s"${hudiTablePath}/*/*")
      .createOrReplaceTempView(_usedBikesTemporaryView)

    sparkSession
      .sql(s"Select * From ${_usedBikesTemporaryView}")
      .show(100)
  }

  override def upsertTable(upsertDataset: List[UsedBike],
                           tableName : String,
                           hudiBasePath : String,
                           sparkSession: SparkSession): Unit = {

    import sparkSession.implicits._

    sparkSession
      .createDataset(upsertDataset)
      .write
      .format("hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "bikeName")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "brand")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "city")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .mode(SaveMode.Append)
      .save(hudiBasePath)
  }

  override def deleteDataFromTable(tobeDeletedDataset: List[UsedBike],
                                   hudiBasePath : String,
                                   hudiTableName : String,
                                   sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession
      .createDataset(tobeDeletedDataset)
      .write
      .format("hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, "delete")
      .option(HoodieWriteConfig.PRECOMBINE_FIELD_PROP, "bikeName")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "bikeName")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "city")
      .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
      .mode(SaveMode.Append)
      .save(hudiBasePath)
  }

  override def showIncrementalView(hudiTableBasePath: String, sparkSession: SparkSession): Unit = {
     sparkSession
      .read
      .format("hudi")
      .load(s"${hudiTableBasePath}/*")
      .createOrReplaceTempView("used_bikes_temporary_view")

    import sparkSession.implicits._

    val commits = sparkSession.sql("select distinct(_hoodie_commit_time) as commitTime from  used_bikes_temporary_view order by commitTime").map(k => k.getString(0)).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    sparkSession
      .read
      .format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime)
      .load(hudiTableBasePath)
      .createOrReplaceTempView("used_bikes_temporary_incremental_view")

    sparkSession
      .sql("Select * From used_bikes_temporary_incremental_view").show(100)
  }

  override def showPointInTimeView(hudiTablePath: String, sparkSession: SparkSession): Unit = {
    val beginTime = "20210728005516"

    import sparkSession.implicits._

    sparkSession
      .read
      .format("hudi")
      .load(s"${hudiTablePath}/*")
      .createOrReplaceTempView(_usedBikesTemporaryView)

    val commits = sparkSession.sql(s"Select distinct(${_colHudiCommitTime}) as commitTime From ${_usedBikesTemporaryView} order by commitTime")
      .map(row => row.getString(0))
      .take(50)

    val endTime = commits(commits.length - 2) // latest timestamp

    sparkSession
      .read
      .format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime)
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, endTime)
      .load(s"${_hudiTableBasePath}/*")
      .select("bikeName", "city", "brand")
      .show(100)
  }

  private def getDummyUsedBikesListForUpsert : List[UsedBike] = {
    List(UsedBike(
      bikeName = "KTM RC 925CC",
      city ="KamaReddy",
      price = "500.0",
      owner = "Third Owner",
      age = "5.0",
      power = "1500.0",
      brand = "KTM_TEST_2",
      kmsDriven = "2000.0"))
  }

  private def getDummyUsedBikesListForDelete : List[UsedBike] = {

    List(
      UsedBike(
        bikeName = "KTM RC 125CC",
        city ="Mumbai"
      ),
      UsedBike(
        bikeName = "KTM RC 425CC",
        city ="Adilabad"
      ),
      UsedBike(
        bikeName = "KTM RC 725CC",
        city = "Khammam"
      )
    )
  }
}
