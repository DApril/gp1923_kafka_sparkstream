package util

import com.alibaba.fastjson.{JSON, JSONObject}

object JsonUtils {
    def getText(json:String):Integer={
      val jsonObj: JSONObject = JSON.parseObject(json)
      jsonObj.getInteger("sysId")

    }
}
