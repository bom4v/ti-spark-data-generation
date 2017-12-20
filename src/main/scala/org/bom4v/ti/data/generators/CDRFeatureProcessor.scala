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

    val callEventDuration = cdr_data
      .groupBy ("imsi")
      .agg(mean ("callEventDuration"), min("callEventDuration"),
	    max("callEventDuration"), sum("callEventDuration"))

    var prefix = "callEventDuration_"
    var renamedColumns = callEventDuration
      .columns
      .map (c => callEventDuration(c).as(s"$prefix$c"))
    val callEventDurationRename = callEventDuration.select(renamedColumns: _*)

    val dataVolumeIncoming = cdr_data
      .groupBy("imsi")
      .agg (mean("dataVolumeIncoming"), min("dataVolumeIncoming"),
	    max("dataVolumeIncoming"), sum("dataVolumeIncoming"))

    prefix = "dataVolumeIncoming_"
    renamedColumns = dataVolumeIncoming
      .columns
      .map (c => dataVolumeIncoming(c).as(s"$prefix$c"))
    val dataVolumeIncomingRename = dataVolumeIncoming.select(renamedColumns: _*)

    val dataVolumeOutgoing = cdr_data
      .groupBy("imsi")
      .agg (mean("dataVolumeOutgoing"), min("dataVolumeOutgoing"),
	    max("dataVolumeOutgoing"), sum("dataVolumeOutgoing"))

    prefix = "dataVolumeOutgoing_"
    renamedColumns = dataVolumeOutgoing
      .columns
      .map(c => dataVolumeOutgoing(c).as(s"$prefix$c"))
    val dataVolumeOutgoingRename = dataVolumeOutgoing.select(renamedColumns: _*)

    val chargeAmount = cdr_data
      .groupBy("imsi")
      .agg(mean("chargeAmount"), min("chargeAmount"),
	    max("chargeAmount"), sum("chargeAmount"))

    prefix = "chargeAmount_"
    renamedColumns = chargeAmount
      .columns
      .map(c => chargeAmount(c).as(s"$prefix$c"))
    val chargeAmountRename = chargeAmount.select(renamedColumns: _*)

    val callingNumberN = cdr_data.groupBy("imsi").agg(countDistinct("callingNumber"))
    val locationAreaN = cdr_data.groupBy("imsi").agg(countDistinct("locationArea"))
    val cellIdN = cdr_data.groupBy("imsi").agg(countDistinct("cellId"))
    val activeDaysN = cdr_data.groupBy("imsi").agg(countDistinct("callEventStartDay"))

    val callingNumbersTechN = cdr_data.groupBy("imsi","callingNumber").count()
    val locationAreasTechN = cdr_data.groupBy("imsi","locationArea").count()
    val cellIdsTechN = cdr_data.groupBy("imsi","cellId").count()
    val activeDaysTechN = cdr_data.groupBy("imsi","callEventStartDay").count()

    val locationAreasTech = cdr_data
      .groupBy("imsi","locationArea")
      .pivot("locationArea")
      .agg(mean("callEventDuration"), min("callEventDuration"),
	    max("callEventDuration"), sum("callEventDuration"))

    val cellIdsTech = cdr_data
      .groupBy("imsi","cellId")
      .pivot("cellId")
      .agg(mean("callEventDuration"), min("callEventDuration"),
	    max("callEventDuration"), sum("callEventDuration"))

    val activeDaysTech = cdr_data
      .groupBy("imsi","callEventStartDay")
      .pivot("callEventStartDay")
      .agg(mean("callEventDuration"), min("callEventDuration"),
	    max("callEventDuration"), sum("callEventDuration"))

    // join
    val cdr_data_start = callEventDurationRename
      .join (dataVolumeIncomingRename,
        dataVolumeIncomingRename.col("dataVolumeIncoming_imsi") === callEventDurationRename.col("callEventDuration_imsi"))

    val cdr_data_start1 = cdr_data_start
      .drop ("dataVolumeIncoming_imsi")
      .withColumnRenamed ("callEventDuration_imsi", "imsi_id")

    val cdr_data_trans = cdr_data_start1
      .join (dataVolumeOutgoingRename, dataVolumeOutgoingRename.col("dataVolumeOutgoing_imsi") === cdr_data_start1.col("imsi_id"))
      .join(chargeAmountRename, chargeAmountRename.col("chargeAmount_imsi") === cdr_data_start1.col("imsi_id"))
      .join(callingNumberN, callingNumberN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(locationAreaN, locationAreaN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(cellIdN, cellIdN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(activeDaysN, activeDaysN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(callingNumbersTechN, callingNumbersTechN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(locationAreasTechN, locationAreasTechN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(cellIdsTechN, cellIdsTechN.col("imsi") === cdr_data_start1.col("imsi_id"))
      .join(activeDaysTechN, activeDaysTechN.col("imsi") === cdr_data_start1.col("imsi_id"))

    val cdr_data_transponed = cdr_data_trans
      .drop("dataVolumeOutgoing_imsi", "chargeAmount_imsi", "imsi")

    //
    cdr_data_transponed
  }

}

