package com.explore.apache.hudi.impl

import com.explore.apache.beans.{LibraryCheckoutIndexInfo, LibraryCheckoutInfo}
import com.explore.apache.constants.{AppConstants, HudiServiceConstants}
import com.explore.apache.hudi.HudiService
import com.explore.apache.utils.{HudiServiceHelper, SparkJobHelper}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, QuickstartUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

class SeatleLibraryCheckoutDatasetHudiService extends HudiService[LibraryCheckoutInfo] with Serializable {

  def execute() : Unit = {
    val inputCsvAbsPath = HudiServiceHelper.getInputCsvLoadPath

    val sparkSession = SparkJobHelper.getLocalSparkSession("seatle-library-checkout-hudi-service")

//    createTableFromCsv(inputCsvAbsPath, HudiServiceConstants.SLC_HUDI_BASE_PATH, HudiServiceConstants.SLC_HUDI_TABLE_NAME, sparkSession)
    showSnapShotView(HudiServiceConstants.SLC_HUDI_BASE_PATH, sparkSession)
//    deleteDataFromTable(List.empty, HudiServiceConstants.SLC_HUDI_BASE_PATH, HudiServiceConstants.SLC_HUDI_TABLE_NAME, sparkSession)
  }

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

  override def showSnapShotView(hudiTablePath: String, sparkSession: SparkSession): Unit = {
    sparkSession
      .read
      .format(AppConstants.SPARK_FORMAT_HUDI)
      .load(s"${hudiTablePath}/nadvd/*")
      .createOrReplaceTempView(HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW)

    sparkSession.sql(s"Select bibNumber, itemBarcode, callNumber, itemType, collection from ${HudiServiceConstants.SLC_HUDI_TEMPORARY_VIEW} Where itemBarcode ='0010063255433'")
      .show(20)
  }

  override def upsertTable(upsertDataset: List[LibraryCheckoutInfo], tableName: String, hudiBasePath: String, sparkSession: SparkSession): Unit = ???

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

  override def showIncrementalView(hudiBaseTablePath: String, sparkSession: SparkSession): Unit = ???

  override def showPointInTimeView(hudiTablePath: String, sparkSession: SparkSession): Unit = ???
}
