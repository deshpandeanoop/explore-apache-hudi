package com.explore.apache.hudi

import com.explore.apache.hudi.impl.{SeatleLibraryCheckoutDatasetHudiService, UsedBikesHudiService}

object ApacheHudiDriverJob extends App {

  val hudiService = new SeatleLibraryCheckoutDatasetHudiService

  hudiService.execute
}
