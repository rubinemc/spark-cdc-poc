

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigReader {

  public static Properties props;
  public static Properties propsLog;


  public static void loadProperties() throws FileNotFoundException, IOException {
    
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream input = classLoader.getResourceAsStream("cdc.properties");
    
    props = new Properties();
    props.load(input);
   // props.load(new FileInputStream(configPath));
  }
  
 
public static void loadLogProperties() throws FileNotFoundException, IOException {
    
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream input = classLoader.getResourceAsStream("cdc-log4j.properties");
    
    propsLog = new Properties();
    propsLog.load(input);
  }
  
  public static String getProperty(String propertyName) {
    return props.get(propertyName).toString();
  }

  public static Properties getLogProperty() {
    return propsLog;
  }

}
