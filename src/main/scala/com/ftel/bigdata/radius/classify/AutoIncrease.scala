package com.ftel.bigdata.radius.classify

class AutoIncrease(var init: Int) {
  def get: Int = {
    init = init + 1
    init
  }
}