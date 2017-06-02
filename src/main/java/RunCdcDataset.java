import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.RollingLogs.LineIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import scala.Function;
import scala.tools.nsc.typechecker.PatternMatching.DPLLSolver.Lit;

public class RunCdcDataset {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		ConfigReader.loadProperties();
		SparkSession spark = SparkSession
				  .builder()
				  .appName(ConfigReader.getProperty(Constants.APPNAME))
	              .master(ConfigReader.getProperty(Constants.SPARKMASTER))
				  .config("spark.scheduler.mode", ConfigReader.getProperty(Constants.SPARKSCHEDULERMODE))
				  .getOrCreate();
		
		
		//Start timer
	    Long startTime = System.currentTimeMillis();
	    
	    
	    //Get these values from the MetaData table
	    /*String fullDataPath = "C:\\\\Users\\\\maharr1\\\\Desktop\\\\cdc-million\\\\fullData.csv";
	    String newDataPath = "C:\\\\Users\\\\maharr1\\\\Desktop\\\\cdc-million\\\\newData.csv";*/
	    
	    /*String fullDataPath = "C:\\\\Users\\\\maharr1\\\\Desktop\\\\cdc-test\\\\fullData.csv";
	    String newDataPath = "C:\\\\Users\\\\maharr1\\\\Desktop\\\\cdc-test\\\\newData.csv";*/
	    
	    
	    String fullDataPath = ConfigReader.getProperty(Constants.FULLDATAPATH);
	    String newDataPath = ConfigReader.getProperty(Constants.NEWDATAPATH);;
	    
   
	    Dataset dfFull = spark.read().format("com.databricks.spark.csv")
	    		.option("header", "true").option("inferSchema","true").load(fullDataPath).persist(StorageLevel.MEMORY_AND_DISK_SER());
	    
	    Dataset dfNew = spark.read().format("com.databricks.spark.csv")
	    		.option("header", "true").option("inferSchema","true").load(newDataPath).persist(StorageLevel.MEMORY_AND_DISK_SER());
	   
	    
	    //readMetadata();
	    computeCDC(spark,dfFull,dfNew);
	    
	    Long stopTime = System.currentTimeMillis();
	    
	    Long elapsedTime = (stopTime - startTime);
	    
	    System.out.println("Elapsed Time : "+ elapsedTime + " milliseconds");
	    
	    spark.stop();
	    
	}
	
	
	
	/*
	 * Method to compute Inserts/Updates/Deletes and generate CDC file.
	 */
	
	public static Dataset computeCDC(SparkSession spark, Dataset dfFull, Dataset dfNew)
	{	
		
		Map<String, Integer> colMap= new HashMap();
	    final String[] cols = dfFull.columns();
	    final Integer columnLength = cols.length;
	    
	    for (int i = 0; i < cols.length; i++) {
	    	
	    //System.out.println(i+" || "+cols[i]);
	      colMap.put(cols[i], i);
	    }
		
	    //Get this from the MetaData table
	    //String keyColumns = "CustomerID";
	    
	    String[] keyColumnsArray = ConfigReader.getProperty("keyColumns").split(",");
	    
	    
	    String joinQueryString = "";
	    String filterQueryStringFull = getFilterString("full", keyColumnsArray);
	    String filterQueryStringNew = getFilterString("new", keyColumnsArray);
	    
	    for (int i = 0; i < keyColumnsArray.length; i++) {
			
	    	if(i==keyColumnsArray.length-1)
	    		joinQueryString = joinQueryString+" new."+keyColumnsArray[i]+"= full."+keyColumnsArray[i];
	    	
	    	else
	    		joinQueryString = joinQueryString+" new."+keyColumnsArray[i]+" = full."+keyColumnsArray[i]+" and";	
	    	
		}
	    
	    
	    
	    /*System.out.println("JOIN QUERY STRING :"+ joinQueryString);
	    System.out.println("FILTER QUERY STRING FULL:"+ filterQueryStringFull);
	    System.out.println("FILTER QUERY STRING NEW :"+ filterQueryStringNew);*/
	    
	    /**************************************
	     * Deletes *
	     **************************************/
	    dfNew.createOrReplaceTempView("new");
	    dfFull.createOrReplaceTempView("full");
	   
	    
	    // To pick 'select' columns from the DF
	    Column[] deleteColumnArray = new Column[columnLength];
		   
		   for (int i = 0; i < deleteColumnArray.length; i++) {
			
			   deleteColumnArray[i] =  new Column("full."+cols[i]);
		   }

	/*   Dataset deletes = spark.sql("Select * from new RIGHT OUTER JOIN full on new.CustomerID = full.CustomerID").filter("new.CustomerID is null").select(deleteColumnArray);
	   
	   deletes.show();
*/
	   
	   
	    System.out.println("*****************DELETED*****************");
	   // JavaRDD<Row> deleteRDD = spark.sql("Select * from new RIGHT OUTER JOIN full on new.CustomerID = full.CustomerID").filter("new.CustomerID is null").select(deleteColumnArray)
	    
	    JavaRDD<Row> deleteRDD = spark.sql("Select * from new RIGHT OUTER JOIN full on "+joinQueryString).filter(filterQueryStringNew).select(deleteColumnArray)
	    					.javaRDD().map(new org.apache.spark.api.java.function.Function<Row, Row>() {

	    						public Row call(Row row) throws Exception {

	    								//Creating a row here
	    								Object[] rowElementsArray = new Object[cols.length+1];
	    								for (int j = 0; j < cols.length; j++) {
	    									
	    									rowElementsArray[j] = row.get(j);
	    								}
	    								rowElementsArray[rowElementsArray.length-1] = "D"; 
	    								
	    								Row deletedRow = RowFactory.create(rowElementsArray);
	    								return deletedRow;
	    							
	    						}
	    					});
	    
	    
	    
	    
	    /************************************** 
	     * Inserts *
	     **************************************/
	    
	    
	    Column[] insertColumnArray = new Column[columnLength];
		   
		   for (int i = 0; i < insertColumnArray.length; i++) {
			
			   insertColumnArray[i] =  new Column("new."+cols[i]);
		   }
	    
	    
	     
	    System.out.println("*****************INSERTED*****************");
	   // JavaRDD<Row> InsertedRDD = spark.sql("Select * from new LEFT OUTER JOIN full on new.CustomerID = full.CustomerID").filter("full.CustomerID is null").select(insertColumnArray)
	    	
	    JavaRDD<Row> InsertedRDD = spark.sql("Select * from new LEFT OUTER JOIN full on "+joinQueryString).filter(filterQueryStringFull).select(insertColumnArray)
	    		.javaRDD().map(new org.apache.spark.api.java.function.Function<Row, Row>() {

	    	    						public Row call(Row row) throws Exception {

	    	    								//Creating a row here
	    	    								Object[] rowElementsArray = new Object[cols.length+1];
	    	    								for (int j = 0; j < cols.length; j++) {
	    	    									
	    	    									rowElementsArray[j] = row.get(j);
	    	    								}
	    	    								rowElementsArray[rowElementsArray.length-1] = "I"; 
	    	    								
	    	    								Row InsertedRow = RowFactory.create(rowElementsArray);
	    	    								return InsertedRow;
	    	    							
	    	    						}
	    	    					});
	    
	    //dfInserted.show();
	    
	    
	    
	     /* No Change */
	     
	   /* System.out.println("*****************NO CHANGE*****************");
	    Dataset dfNoChange = dfNew.alias("new").join(dfFull.alias("old"), dfNew.col("CustomerID").equalTo(dfFull.col("CustomerID")), "left_outer").filter("old.CustomerID = new.CustomerID").filter("old.Name = new.Name and old.Address= new.Address");
	    dfNoChange.show();
	    
	    */
	    
	   /***************************************
	    * Updates *
	    ***************************************/

	    System.out.println("*****************UPDATED*****************");
	    
	    Dataset joinedDF = spark.sql("Select * from new LEFT OUTER JOIN full on "+joinQueryString);
		 
	    //joinedDF.show();
	    
	    //JavaRDD<Row> updatedRDD = joinedDF.filter("full.CustomerID = new.CustomerID")
	    
	    JavaRDD<Row> updatedRDD = joinedDF.filter(joinQueryString)
	    						.javaRDD().map(new org.apache.spark.api.java.function.Function<Row, Row>() {

			public Row call(Row row) throws Exception {
				
				Integer flag = 0;// true
				for (int i = 0; i < cols.length; i++) {
					//System.out.println("New: "+row.get(i).toString().trim()+ "|| Old: "+row.get(i+columnLength).toString().trim());
					if(!(row.get(i).toString().trim().equals(row.get(i+columnLength).toString().trim())))
					{
						flag=1;
						break;
					}
				}
				
				if(flag==1)// i.e the updated records
				{
					//Creating a row here
					Object[] rowElementsArray = new Object[cols.length+1];
					for (int j = 0; j < cols.length; j++) {
						
						rowElementsArray[j] = row.get(j);
					}
					rowElementsArray[rowElementsArray.length-1] = "U"; 
					
					Row updatedRow = RowFactory.create(rowElementsArray);
					return updatedRow;
				}
				
				
				else return null;
			}
		}).filter(new org.apache.spark.api.java.function.Function<Row, Boolean>() {
			
			public Boolean call(Row row) throws Exception {
				if(row==null)
				return false;
				else return true;
			}
		});
	   
	    
	 
	    StructField cdcField = DataTypes.createStructField("cdc", DataTypes.StringType, true);
	    StructType schema = dfFull.schema().add(cdcField);
	    
	    
	    Dataset cdcDF = spark.createDataFrame(updatedRDD.union(deleteRDD).union(InsertedRDD), schema);
	    //cdcDF.show();
	    
	    
	  /* Dataset dfUpdated = sqlContext.createDataFrame(updated, schema);
	    dfUpdated.show();
	  */
	    
	    //Write CDC file to the output folder.
	    
	    cdcDF.repartition(1).write().format("com.databricks.spark.csv")
	    .option("header", "true")
	    //.save("cdc.csv");
	    .save(ConfigReader.getProperty(Constants.CDCOUTPUTPATH));
		
	    return cdcDF;

		
	}
	
	
	
	
	//Method to return the filter string
	
	public static String getFilterString(String type, String[] keyColumnsArray)
	{
		String filterQueryString = "";
		for (int i = 0; i < keyColumnsArray.length; i++) {
			
	    	if(i==keyColumnsArray.length-1)
	    		filterQueryString = filterQueryString+ type + "."+keyColumnsArray[i]+" is null ";
	    	
	    	else
	    		filterQueryString = filterQueryString+ type + "."+keyColumnsArray[i]+" is null and ";	
	    	
		}
		
		return filterQueryString;
		
	}
	
	
	
	// Redshift metadata utility
	
	public static void readMetadata()
	{
		
		Connection conn = null;
        Statement stmt = null;
      
        String dbURL = ConfigReader.getProperty(Constants.DBURL);
        String dbUsername = ConfigReader.getProperty(Constants.DBUSERNAME);
        String dbPassword = ConfigReader.getProperty(Constants.DBPASWORD);

		
        try{
        	
        	Class.forName("com.amazon.redshift.jdbc.Driver");

        
           //Open a connection and define properties.
           System.out.println("Connecting to database...");
           Properties props = new Properties();

           
           //Uncomment the following line if using a keystore.
           //props.setProperty("ssl", "true");  
           props.setProperty("user", dbUsername);
           props.setProperty("password", dbPassword);
           conn = DriverManager.getConnection(dbURL, props);

           System.out.println("Listing Table Metatable Information...");
           stmt = conn.createStatement();
           String sql;
           sql = "SELECT * FROM tbl_metadata WHERE schema_system_name='COMMON' and object_name='EMPLOYEE' and active_flag = true order by object_fields_order;";
           ResultSet rs = stmt.executeQuery(sql);
           
           
           List<RedshiftObject> columnList = new ArrayList<RedshiftObject>();
           
           //Get the data from the result set.
           while(rs.next()){
              //Retrieve columns.
        	   
        	   RedshiftObject rsObject = new RedshiftObject();
        	   
        	   rsObject.setSourceType(rs.getString("source_type"));
        	   rsObject.setSchemaSystemName(rs.getString("schema_system_name"));
        	   rsObject.setObjectId(rs.getLong("object_id"));
        	   rsObject.setObjectName(rs.getString("object_name"));
        	   rsObject.setObjectFields(rs.getString("object_fields"));
        	   rsObject.setObjectDataType("object_data_type");
        	   rsObject.setIsPk(rs.getBoolean("is_pk"));
        	   rsObject.setIsCdc(rs.getBoolean("is_cdc"));
        	   rsObject.setIsMandatory(rs.getBoolean("is_mandatory"));
        	   rsObject.setFileDelimiter(rs.getString("file_delimiter"));
        	   rsObject.setIsFileHeader(rs.getBoolean("is_file_header"));
        	   rsObject.setActiveFlag(rs.getBoolean("active_flag"));
        	   rsObject.setStartDate(rs.getDate("start_date"));
        	   rsObject.setEndDate(rs.getDate("end_date"));
        	   rsObject.setComments(rs.getString("comments"));
        	   rsObject.setObjectFieldsOrder(rs.getInt("object_fields_order"));
        	   rsObject.setFilePath(rs.getString("file_path"));
        	   
        	   
        	   System.out.println(rsObject.toString());
   
        	   columnList.add(rsObject);

           }
         
           
           rs.close();
           stmt.close();
           conn.close();
        }catch(Exception ex){
           //For convenience, handle all errors here.
           ex.printStackTrace();
        }finally{
           //Finally block to close resources.
           try{
              if(stmt!=null)
                 stmt.close();
           }catch(Exception ex){
           }// nothing we can do
           try{
              if(conn!=null)
                 conn.close();
           }catch(Exception ex){
              ex.printStackTrace();
           }
        }
        System.out.println("Finished reading the Table Metadata");
     }

        
	}
