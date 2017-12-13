package org.bom4v.ti.data.generators

//import org.apache.spark.sql._
//import org.apache.spark.sql.Dataset
// Data preparation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType


object CDRFeatureProcessor {

  def extractFields(dataset: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val cdr_data_temp : org.apache.spark.sql.DataFrame = dataset
      .selectExpr (
      "specificationVersionNumber",
 	    "releaseVersionNumber",
 	    "fileName",
 	    "fileAvailableTimeStamp",
 	    "fileUtcTimeOffset",
 	    "sender",
 	    "recipient",
 	    "sequenceNumber",
 	    "cast(callEventsCount as Int) callEventsCount",
 	    "eventType",
 	    "cast(imsi as String) imsi",
 	    "cast(imei as String) imei",
 	    "callEventStartTimeStamp",
 	    "utcTimeOffset",
 	    "callEventDuration",
 	    "causeForTermination",
 	    "accessPointNameNI",
 	    "accessPointNameOI",
 	    "cast(dataVolumeIncoming as Int) dataVolumeIncoming",
 	    "cast(dataVolumeOutgoing as Int) dataVolumeOutgoing",
 	    "sgsnAddress",
 	    "ggsnAddress",
 	    "chargingId",
 	    "cast(chargeAmount as Int) chargeAmount",
 	    "teleServiceCode",
 	    "bearerServiceCode",
 	    "supplementaryServiceCode",
 	    "cast(dialledDigits as String) dialledDigits",
 	    "cast(connectedNumber as String) connectedNumber",
 	    "cast(thirdPartyNumber as String) thirdPartyNumber",
 	    "cast(callingNumber as String) callingNumber",
 	    "recEntityId",
 	    "callReference",
 	    "locationArea",
 	    "cellId",
 	    "cast(msisdn as String) msisdn",
 	    "servingNetwork")

    val cdr_data : org.apache.spark.sql.DataFrame = cdr_data_temp
      .withColumn ("callEventStartDay", cdr_data_temp ("callEventStartTimeStamp").cast(DateType))
    cdr_data
  }

}
