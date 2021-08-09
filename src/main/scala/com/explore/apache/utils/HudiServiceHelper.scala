package com.explore.apache.utils

import com.explore.apache.constants.AppConstants

import scala.util.Properties

object HudiServiceHelper {

  def getInputCsvLoadPath : String = Properties.envOrElse("input-csv-path", AppConstants.EMPTY_STRING)

  def getHudiStoreBasePath : String = Properties.envOrElse("hudi-store-base-path", AppConstants.EMPTY_STRING)
}
