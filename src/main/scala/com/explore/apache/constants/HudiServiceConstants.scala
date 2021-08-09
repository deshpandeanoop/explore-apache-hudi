package com.explore.apache.constants

import com.explore.apache.utils.HudiServiceHelper

object HudiServiceConstants {

  //SLC = Seatle Library Checkout
  val SLC_HUDI_TABLE_NAME = "slc_hudi_table"

  val SLC_HUDI_TEMPORARY_VIEW = "slc_hudi_temporary_view"

  val SLC_HUDI_BASE_PATH = s"${HudiServiceHelper.getHudiStoreBasePath}/seatle-library-checkout-dataset-new"
}
