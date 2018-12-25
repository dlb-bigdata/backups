package com.dlb

import com.dlb.common.ConfigManager
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object etlSQLTool {

  def main(args: Array[String]): Unit = {

    val path = "hdfs://dlb01:9000/dlbjson/cibndata/20181126"
    val tableName = "dlb_history"

    val sc = new SparkConf().setAppName("SQLContextApp").setMaster("local[*]").set("spark.executor.memory", "12G")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.debug.maxToStringFields", "100")
      .registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable]))
    val spark = SparkSession.builder().config(sc).getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","dlb01,dlb02,dlb03")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val jobConf = new JobConf(hbaseConf)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val json = spark.read.json(path).cache()

    val jsontable = json.createTempView("cibn")

    /**
      * 统计数据条目数
      */
    import spark.implicits._
    val jsoncount = spark.sql("select count(uuid) as usercount,cid from cibn group by cid")
        jsoncount.toDF.write
        .format("jdbc")
        .option("url", ConfigManager.config.getString("jdbc.url"))
        .option("dbtable", "numcount")
        .option("user", ConfigManager.config.getString("jdbc.user"))
        .option("password", ConfigManager.config.getString("jdbc.password"))
        .mode(SaveMode.Overwrite)
        .save()


//    import spark.implicits._
    val ans = spark.sql("select uuid,androidId,bdCpu,bdModel,buildBoard,channel,analyticId," +
      "cid,city,cityCode,country,cpuCnt,cpuName,definition,deviceId,deviceName,dpi,duration," +
      "endTime,entry1,entry1Id,eth0Mac,eventKey,eventName,eventType,eventValue,ip,ipaddr,isVip," +
      "isp,kafkaTopic,largeMem,limitMem,name,nameId,openId,optType,page,pkg,pluginPkg,pluginVercode," +
      "pos,prePage,prevue,province,rectime,screen,serial,session,site,specId,subName,time,topic," +
      "topicCid,topicId,topicType,touch,uid,url,verCode,verName,wlan0Mac,x,y,adType from cibn").as[toJson].rdd


//    val hotby = spark.sql("select uuid,androidId,duration,time,endTime,blockId,country from cibn").rdd.map(x =>{
//      (x.get(0),x.get(1),x.get(2),x.get(3),x.get(4),x.get(5),x.get(6))
//    })

    /**
      * 观看时长统计，Top10
      */
    ans.map{
      item =>(item.duration,(item.uuid,item.province,item.androidId,item.name))
    }.sortByKey(false).take(10)
      .foreach(println)


    /**
      * 原始数据入库
      */
        ans.map(item =>{
      val put = new Put(Bytes.toBytes(item.uuid+"_"+item.androidId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("uuid"),Bytes.toBytes(item.uuid))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("androidId"),Bytes.toBytes(item.androidId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("bdCpu"),Bytes.toBytes(item.bdCpu))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("bdModel"),Bytes.toBytes(item.bdModel))
//      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("blockId"),Bytes.toBytes(item.blockId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("buildBoard"),Bytes.toBytes(item.buildBoard))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("channel"),Bytes.toBytes(item.channel))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("analyticId"),Bytes.toBytes(item.analyticId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cid"),Bytes.toBytes(item.cid))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cityCode"),Bytes.toBytes(item.cityCode))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("country"),Bytes.toBytes(item.country))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cpuCnt"),Bytes.toBytes(item.cpuCnt))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("cpuName"),Bytes.toBytes(item.cpuName))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("definition"),Bytes.toBytes(item.definition))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("deviceId"),Bytes.toBytes(item.deviceId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("deviceName"),Bytes.toBytes(item.deviceName))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("dpi"),Bytes.toBytes(item.dpi))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("duration"),Bytes.toBytes(item.duration))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("endTime"),Bytes.toBytes(item.endTime))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("entry1"),Bytes.toBytes(item.entry1))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("entry1Id"),Bytes.toBytes(item.entry1Id))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eth0Mac"),Bytes.toBytes(item.eth0Mac))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventKey"),Bytes.toBytes(item.eventKey))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventName"),Bytes.toBytes(item.eventName))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventType"),Bytes.toBytes(item.eventType))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("eventValue"),Bytes.toBytes(item.eventValue))
//      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("index"),Bytes.toBytes(item.index))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ip"),Bytes.toBytes(item.ip))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("ipaddr"),Bytes.toBytes(item.ipaddr))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("isVip"),Bytes.toBytes(item.isVip))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("isp"),Bytes.toBytes(item.isp))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("kafkaTopic"),Bytes.toBytes(item.kafkaTopic))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("largeMem"),Bytes.toBytes(item.largeMem))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("limitMem"),Bytes.toBytes(item.limitMem))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("openId"),Bytes.toBytes(item.openId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("optType"),Bytes.toBytes(item.optType))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("page"),Bytes.toBytes(item.page))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pkg"),Bytes.toBytes(item.pkg))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pluginPkg"),Bytes.toBytes(item.pluginPkg))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pluginVercode"),Bytes.toBytes(item.pluginVercode))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("pos"),Bytes.toBytes(item.pos))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("prePage"),Bytes.toBytes(item.prePage))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("prevue"),Bytes.toBytes(item.prevue))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("province"),Bytes.toBytes(item.province))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("rectime"),Bytes.toBytes(item.rectime))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("screen"),Bytes.toBytes(item.screen))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("serial"),Bytes.toBytes(item.serial))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("session"),Bytes.toBytes(item.session))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("site"),Bytes.toBytes(item.site))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("specId"),Bytes.toBytes(item.specId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("subName"),Bytes.toBytes(item.subName))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("time"),Bytes.toBytes(item.time))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topic"),Bytes.toBytes(item.topic))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topicCid"),Bytes.toBytes(item.topicCid))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topicId"),Bytes.toBytes(item.topicId))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("topicType"),Bytes.toBytes(item.topicType))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("touch"),Bytes.toBytes(item.touch))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("verCode"),Bytes.toBytes(item.verCode))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("verName"),Bytes.toBytes(item.verName))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("wlan0Mac"),Bytes.toBytes(item.wlan0Mac))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("x"),Bytes.toBytes(item.x))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("y"),Bytes.toBytes(item.y))
      put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("adType"),Bytes.toBytes(item.adType))
      (new ImmutableBytesWritable,put)
    }).saveAsHadoopDataset(jobConf)

    spark.close()
  }
}
