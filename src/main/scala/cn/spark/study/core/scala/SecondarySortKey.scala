package cn.spark.study.core.scala

/**
  * Created by Administrator on 2017/4/8 0008.
  */
class SecondarySortKey(val first:Int,val second:Int) extends Ordered[SecondarySortKey] with Serializable{

  override def compare(that: SecondarySortKey): Int = {
    if(this.first - that.first!=0){
      this.first-that.first
    }
    else{
      this.second-that.second
    }
  }
}
