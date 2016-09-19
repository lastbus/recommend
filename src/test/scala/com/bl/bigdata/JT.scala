package com.bl.bigdata

import org.json.JSONObject
import org.junit.Test

/**
  * Created by MK33 on 2016/7/19.
  */
@Test
class JT {

  @Test
  def t = {
    val s = "{\"eventDate\":\"1468920534526\",\"memberId\":\"100000000188659\",\"channel\":\"H5\",\"recResult\":{\"api\":\"gyl\",\"elapsedTime\":1,\"goodsList\":[{\"category_id\":\"102818\",\"category_name\":\"投影仪\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"极米(XGIMI) H1 超高清1080P 3D智能 家用投影机\",\"pro_sid\":\"467068\",\"sale_price\":\"5180.0\",\"sid\":\"305330\",\"url\":\"http://k12.iblimg.com/goods/302001522670/SP_3020_302001522670_01_10006.jpg\"},{\"category_id\":\"101744\",\"category_name\":\"手机\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"华为（HUAWEI）麦芒4（RIO-AL00）玫瑰金 3+32GB内存 全网通版4G手机 双卡双待\",\"pro_sid\":\"438272\",\"sale_price\":\"1888.0\",\"sid\":\"241463\",\"url\":\"http://k11.iblimg.com/goods/302001470001/SP_3020_302001470001_01_10006.jpg\"},{\"category_id\":\"102410\",\"category_name\":\"床品套件\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"小绵羊家纺 韩式斜纹四件套C2-1566 200*230cm\",\"pro_sid\":\"436663\",\"sale_price\":\"169.0\",\"sid\":\"244871\",\"url\":\"http://k15.iblimg.com/goods/302001475229/SP_3020_302001475229_01_10006.jpg\"},{\"category_id\":\"102203\",\"category_name\":\"面部精华\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"欧舒丹 蜡菊赋颜御龄精华霜 50ml\",\"pro_sid\":\"390642\",\"sale_price\":\"833.0\",\"sid\":\"121683\",\"url\":\"http://k12.iblimg.com/goods/100027000393/SP_1000_100027000393_01_10006.jpg\"},{\"category_id\":\"103073\",\"category_name\":\"其它膨化食品\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"张君雅 小妹妹拉面条饼（番茄味）65g\",\"pro_sid\":\"460770\",\"sale_price\":\"7.5\",\"sid\":\"291466\",\"url\":\"http://k12.iblimg.com/goods/100031989395/SP_1000_100031989395_01_10006.jpg\"},{\"category_id\":\"102976\",\"category_name\":\"其他工艺藏品\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"皇家雪兰莪五行锡镴双盖茶叶罐大号\",\"pro_sid\":\"451383\",\"sale_price\":\"3180.0\",\"sid\":\"267949\",\"url\":\"http://k21.iblimg.com/mp/mp/goods/7282/38216/38216_@0_@200X200.jpg\"},{\"category_id\":\"103045\",\"category_name\":\"刀具剪刀\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"双立人（Zwilling）TWIN Point鸿运当头刀具8件套\",\"pro_sid\":\"439174\",\"sale_price\":\"888.0\",\"sid\":\"249274\",\"url\":\"http://k15.iblimg.com/goods/303001458193/SP_3030_303001458193_01_10006.jpg\"},{\"category_id\":\"102410\",\"category_name\":\"床品套件\",\"from\":\"RCMD_ALS\",\"goods_sales_name\":\"小绵羊家纺 韩式斜纹四件套C2-1586 200*230cm\",\"pro_sid\":\"436673\",\"sale_price\":\"169.0\",\"sid\":\"244863\",\"url\":\"http://k13.iblimg.com/goods/302001475774/SP_3020_302001475774_01_10006.jpg\"}],\"message\":\"正常\",\"method\":\"gyl_als\",\"rcmdAmt\":8,\"statusCode\":200,\"totalPages\":8},\"actType\":\"gyl\"}"

    val json = new JSONObject(s)

    println(json.getString("eventDate"))

    val o1 = json.getJSONObject("recResult")
    println(o1.getString("api"))
    val a1 = o1.getJSONArray("goodsList")
    println(a1)


  }
}
