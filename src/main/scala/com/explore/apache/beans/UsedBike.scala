package com.explore.apache.beans

import com.explore.apache.constants.AppConstants._


case class UsedBike(bikeName : String,
                    price : String = EMPTY_STRING,
                    city : String,
                    kmsDriven : String = EMPTY_STRING,
                    owner : String = EMPTY_STRING,
                    age : String = EMPTY_STRING,
                    power : String = EMPTY_STRING,
                    brand : String = EMPTY_STRING)
