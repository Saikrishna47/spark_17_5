object matching 
{
  def main(args:Array[string])
  {
    val spark = sparksession.builder().appName("matching").master("local[*]").getorCreate()
    
    val dataSchema = structType(Array( structField("OrderID",String),
                                      structField("UserName",String),
                                      structField("OrderTime",String),
                                      structField("OrderType",String),
                                      structField("Quantity",String),
                                      structField("Price",String)
                                      ))
                                   
                                      
    val dataDF= spark.read.format("com.databricks.spark.csv").schema("dataSchema")
    
    val selfjoinDF = dataDF.as("df1").join(dataDF.as("df1"), col("df1.OrderType")===col("df2.OrderType") && col("df1.Quantity")===col("df2.Quantity") )
    .select("df1.OrderID","df1.OrderTime","df1.OrderType","df1.Quantity","df1.Price")
    
    val rankDF = selfjoinDF.withcolumn("rank", row_number().over(window.partitionBy("OrderID").orderBy("Quantity"))).select("*")
    
    var orderDF= rankDF.groupby("OrderID","OrderType").agg(count("rank")).withcolumn("orderbook", where(count>1))
    .select("OrderID","rank","OrderTime","Quantity","Price","orderbook","OrderType")
    
    orderDF=orderDF.withcolumn("orderbook",where(col("OrderType")!="1",lit("open")))
    
    var finalPriceDF= orderDF.when(col("OrderType")=="BUY").groupby("OrderID","OrderType").min("price").withcolumnrenamed("min","finalPrice").select("*")
   
    finalPriceDF.when(col("OrderType")=="SELL").groupby("OrderID","OrderType").max("price").withcolumnrenamed("max","finalPrice").select("*")
    
    val ouputDF = finalPriceDF.withcolumn("rank", row_number().over(window.partitionBy("OrderID").orderBy("OrderType"))).distinct("OrderTime")
    .select("OrderID","rank","OrderTime","Quantity","finalPrice")

  }
}
