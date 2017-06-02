

public interface Constants {

  public static final String SPARKMASTER = "spark.master.host";

  public static final String SPARKDRIVERHOST = "spark.driver.host";

  public static final String SPARKCORE = "spark.cores.max";
  
  public static final String SPARKEXECUTORMEMORY = "spark.executor.memory";

  public static final String RECONSTRUCTSPARKCORE = "reconstruct.spark.cores.max";

  public static final String APPNAME = "spark.appname";

  public static final String OUTPUTFOLDER = "output.folder";

  public static final String SPARKLIB="spark.lib";
  
  public static final String SPARKSCHEDULERMODE="spark.scheduler.mode";
  
  public static final String CDCOUTPUTPATH= "cdcOutputPath";
  
  
  /*
   * CDC dump files
   */
  
  public static final String FULLDATAPATH = "fullDataPath";
  public static final String NEWDATAPATH = "newDataPath";
  
  /*
   * Redshift
   */
  
  public static final String DBURL = "db.url";
  public static final String DBUSERNAME = "db.username";
  public static final String DBPASWORD = "db.password";
  
  /*
   * tbl_metadata columns
   */
  
  public static final String SOURCETYPE = "source_type";
  public static final String SCHEMASYSTEMNAME = "schema_system_name";
  public static final String OBJECTID = "object_id";
  public static final String OBJECTNAME = "object_name";
  public static final String OBJECTFIELDS = "object_fields";
  public static final String OBJECTDATATYPE = "object_data_type";
  public static final String ISPK = "is_pk";
  public static final String ISCDC = "is_cdc";
  public static final String ISMANDATORY = "is_mandatory";
  public static final String FILEDELIMITER = "file_delimiter";
  public static final String ISFILEHEADER = "is_file_header";
  public static final String ACTIVEFLAG = "active_flag";
  public static final String STARTDATE = "start_date";
  public static final String ENDDATE = "end_date";
  public static final String COMMENTS = "comments";
  
}
