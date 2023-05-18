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
                                   
    //reading the file and storing into dataframe                                  
    val dataDF= spark.read.format("com.databricks.spark.csv").schema("dataSchema")
    
    //get only records with both buy and sell orders 
    val selfjoinDF = dataDF.as("df1").join(dataDF.as("df1"), col("df1.Quantity")===col("df2.Quantity") )
    .select("df1.OrderID","df1.OrderTime","df1.OrderType","df1.Quantity","df1.Price").distinct()
    
    //to check count in next steps for orders, ranking over orderid
    val rankDF = selfjoinDF.withcolumn("rank", row_number().over(window.partitionBy("OrderID").orderBy("Quantity"))).select("*")
    
    //grouping to get the status of order 
    var orderDF= rankDF.groupby("rank","Quantity").agg(count("rank")).withcolumn("orderbook", when(col("agg")=2),lit("Closed")).otherwise(lit("Open")) )
    .select("OrderID","rank","OrderTime","Quantity","Price","orderbook","OrderType")
    
    //orderDF=orderDF.withcolumn("orderbook",where(col("OrderType")!="1",lit("open")))
    
    //get final prices of orders placed
    var finalPriceDF= orderDF.filter(col("OrderType")=="BUY").groupby("OrderID","OrderType").min("price").withcolumnrenamed("min","finalPrice").select("*")
   
    finalPriceDF.filter(col("OrderType")=="SELL").groupby("OrderID","OrderType").max("price").withcolumnrenamed("max","finalPrice").select("*")
    
    val ouputDF = finalPriceDF.withcolumn("rank", row_number().over(window.partitionBy("OrderID").orderBy("OrderType"))).distinct("OrderTime")
    .select("OrderID","rank","OrderTime","Quantity","finalPrice")

  }
}
