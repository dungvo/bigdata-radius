package com.ftel.bigdata.radius.streaming

import play.api.libs.json.JsString
import play.api.libs.json.JsValue
import play.api.libs.json.JsNumber
import play.api.libs.json.JsNull
import play.api.libs.json.Json
import com.ftel.bigdata.utils.NumberUtil





/**
 * Json Object for mapping Json String.
 */
class JsonObject(jsonString: String) {

  require(jsonString != null && !jsonString.isEmpty(), "Json String must be not empty")

  private val _value: JsValue = Json.parse(jsonString)

  /**
   * Get value as String from field specify. If field don't exist in json, It will return valueDefault
   */
  def getValueAsString(fieldName: String, valueDefault: String): String = {
    val res = _value.\(fieldName).getOrElse(JsString(valueDefault))
    if (res.isInstanceOf[JsString]) res.as[JsString].value else res.toString()
  }

  /**
   * Get value as String from field specify. If field don't exist in json, It will return empty String
   */
  def getValueAsString(fieldName: String): String = {
    val res = _value.\(fieldName).getOrElse(JsString(""))
    if (res.isInstanceOf[JsString]) res.as[JsString].value else res.toString()
  }

  /**
   * Get value as Number from field specify. If field don't exist in json, It will return valueDefault
   */
  def getValueAsNumber(fieldName: String, valueDefault: Double): BigDecimal = {
    val res = _value.\(fieldName).getOrElse(JsNumber(valueDefault))
    if (res.isInstanceOf[JsNumber]) res.as[JsNumber].value
    else if (res.isInstanceOf[JsString]) res.as[JsString].value.toDouble
    else NumberUtil.toLong(res.toString())
  }

  /**
   * Get value as Number from field specify. If field don't exist in json, It will return 0
   */
  def getValueAsNumber(fieldName: String): BigDecimal = {
    val res = _value.\(fieldName).getOrElse(JsNumber(0))
    if (res.isInstanceOf[JsNumber]) res.as[JsNumber].value
    else if (res.isInstanceOf[JsString]) res.as[JsString].value.toDouble
    else NumberUtil.toLong(res.toString())
  }
  
  /**
   * Get value as JsValue from field specify. If don't exist return JsNull
   */
  def getValue(fieldName: String): JsValue = {
    _value.\(fieldName).getOrElse(JsNull)
  }
  
}