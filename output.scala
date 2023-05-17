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
    
    
  }
}
