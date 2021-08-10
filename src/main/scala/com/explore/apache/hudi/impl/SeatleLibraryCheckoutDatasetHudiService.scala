package com.explore.apache.hudi.impl

import com.explore.apache.beans.{LibraryCheckoutIndexInfo, LibraryCheckoutInfo}
import com.explore.apache.constants.{AppConstants, HudiColumns, HudiServiceConstants}
import com.explore.apache.hudi.HudiService
import com.explore.apache.utils.{HudiServiceHelper, SparkJobHelper}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, QuickstartUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * <code>SeatleLibraryCheckoutDatasetHudiService</code> is <code>HudiService</code>
 * implementation for Seattle Library checkout info data-set
 */
class SeatleLibraryCheckoutDatasetHudiService extends HudiService[LibraryCheckoutInfo] with Serializable {

  /**
   * Starting point of the program
   */
  def execute() : Unit = {
    val inputCsvAbsPath = HudiServiceHelper.getInputCsvLoadPath

    val sparkSession = SparkJobHelper.getLocalSparkSession("seatle-library-checkout-hudi-service")

    sparkSession.sparkContext.setLogLevel("ERROR")

//    createTableFromCsv(inputCsvAbsPath, HudiServiceConstants.SLC_HUDI_BASE_PATH, HudiServiceConstants.SLC_HUDI_TABLE_NAME, sparkSession)
//    showSnapShotView(HudiServiceConstants.SLC_HUDI_BASE_PATH, sparkSession)
//    deleteDataFromTable(List.empty, HudiServiceConstants.SLC_HUDI_BASE_PATH, HudiServiceConstants.SLC_HUDI_TABLE_NAME, sparkSession)
    upsertTable(getDummyUpsertData, HudiServiceConstants.SLC_HUDI_TABLE_NAME, HudiServiceConstants.SLC_HUDI_BASE_PATH, sparkSession)
   showIncrementalView(HudiServiceConstants.SLC_HUDI_BASE_PATH, sparkSession)
  }

  /**
   * Creates new hudi table with given Seattle Library checkout info csv extract
   *
   * @param inputCsvAbsPath
   *                Seattle Library extract
   * @param hudiTableBasePath
   *                Hudi table base path
   * @param hudiTableName
   *                Hudi table name
   * @param sparkSession
   *                sparkSession instance of the job
   */
  override def createTableFromCsv(inputCsvAbsPath: String, hudiTableBasePath: String, hudiTableName: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession
      .read
      .csv(inputCsvAbsPath)
      .map(row => LibraryCheckoutInfo(
        bibNumber = row.getString(0),
        itemBarcode = row.getString(1),
        itemType = row.getString(2),
        collection = row.getString(3),
        callNumber = row.getString(4)))
      .write
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "bibNumber")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "itemType")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "collection")
      .option(HoodieWriteConfig.TABLE_NAME, hudiTableName)
      .mode(SaveMode.Overwrite)
      .save(hudiTableBasePath)
  }

  /**
   * Shows snapshot view of Seattle Library Checkout data-set
   * @param hudiTablePath
   *              Hudi table base path
   * @param sparkSession
   *              sparkSession instance of the job
   */
  override def showSnapShotView(hudiTablePath: String, sparkSession: SparkSession): Unit = {
    sparkSession
      .read
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .load(s"${hudiTablePath}/nadvd/*")
      .createOrReplaceTempView(HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW)

    sparkSession.sql(s"Select bibNumber, itemBarcode, callNumber, itemType, collection from ${HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW} Where itemBarcode ='0010063255633'")
      .show(20, false)
  }

  /**
   * Performs upsert operation on seatle library check-in info table
   *
   * @param upsertDataset
   * @param tableName
   * @param hudiBasePath
   * @param sparkSession
   */
  override def upsertTable(upsertDataset: List[LibraryCheckoutInfo], tableName: String, hudiBasePath: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._

    sparkSession
      .createDataset(upsertDataset)
      .write
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "bibNumber")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "itemType")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "collection")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .mode(SaveMode.Append)
      .save(hudiBasePath)
  }

  /**
   * Performs delete operation
   *
   * @param tobeDeletedDataset
   * @param hudiBasePath
   * @param hudiTableName
   * @param sparkSession
   */
  override def deleteDataFromTable(tobeDeletedDataset: List[LibraryCheckoutInfo], hudiBasePath: String, hudiTableName: String, sparkSession: SparkSession): Unit = {
    val libraryCheckoutInfo = LibraryCheckoutIndexInfo (
      bibNumber = "2320119",
      itemType = "acdvd")

    import sparkSession.implicits._

    sparkSession
      .createDataset(List(libraryCheckoutInfo))
      .write
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, "delete")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "bibNumber")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "itemType")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "collection")
      .option(HoodieWriteConfig.TABLE_NAME, HudiServiceConstants.SLC_HUDI_TABLE_NAME)
      .mode(SaveMode.Append)
      .save(hudiBasePath)
  }

  /**
   *
   * @param hudiBaseTablePath
   * @param sparkSession
   */
  override def showIncrementalView(hudiBaseTablePath: String, sparkSession: SparkSession): Unit = {
    sparkSession
      .read
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .load(s"${hudiBaseTablePath}/nadvd/*")
      .createOrReplaceTempView(HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW)

    import sparkSession.implicits._

    val commits = sparkSession.sql(s"Select distinct(${HudiColumns.HOODIE_COMMIT_TIME}) from ${HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW} " +
      s" order by ${HudiColumns.HOODIE_COMMIT_TIME}").map(row => row.getString(0)).take(50)

    println(s"Commits = ${commits.mkString(",")}")

    val beginTimeStamp = commits(commits.length - 2)

    sparkSession
      .read
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTimeStamp)
      .load(s"${hudiBaseTablePath}/*/*")
      .createOrReplaceTempView(HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW)

    sparkSession
      .sql(s"Select bibNumber, itemType, collection, callNumber From ${HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW}")
      .show(10, false)
  }

  /**
   *
   * @param hudiTablePath
   * @param sparkSession
   */
  override def showPointInTimeView(hudiTablePath: String, sparkSession: SparkSession): Unit = {

  }

  private def getDummyUpsertData : List[LibraryCheckoutInfo] = {
    List(
      LibraryCheckoutInfo(
        bibNumber = "2554635",
        itemBarcode = "0010063255633",
        itemType = "acdvd",
        collection = "nadvd",
        callNumber = "DVD NARUTO125-129_New"
      )
    )
  }
}
