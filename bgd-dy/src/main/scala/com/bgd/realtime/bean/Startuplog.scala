package com.bgd.realtime.bean

/**
  * @author dengyu
  * @data 2019/11/27 - 11:03
  */
case class Startuplog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      logType:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var logHourMinute:String,
                      var ts:Long
                     ) {
}
