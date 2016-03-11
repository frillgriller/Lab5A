

package CityLoc;
import java.sql.*;
import java.util.*;
import java.io.*;
/**
 *
 * @author dkhalid.bscs13seecs
 */
public class CityLoc {

   
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost/Geo";
   static final String USER = "root";
   static final String PASS = "root";
   
   //Accessing Database
   
   
   
   public static void main(String[] args) throws SQLException{
   Connection conn = null;
   Statement stmt = null;
   try{
     
      Class.forName("com.mysql.jdbc.Driver");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);
      
      stmt = conn.createStatement();
      String pop; 

       String csvFile = "C:\\Geo.csv";
	BufferedReader br = null;
	String line = "";
	String cvsSplitBy = ",";
        int rows=0,count = 0;
	try {
		br = new BufferedReader(new FileReader(csvFile));
		while ((line = br.readLine()) != null) {
                        
			String[] ln = line.split(cvsSplitBy,-1);
                        for(int i =0; i<9;i++){
                            if(ln[i].length()==0){
                                ln[i] = "Error";
                            }
                            
                        }
			 pop = "INSERT INTO locations (locId, country, region,city,postalCode,latitude,longitude,metroCode,areaCode) values (?,?,?,?,?,?,?,?,?)";
                        
                         PreparedStatement statement = conn.prepareStatement(pop);
                           statement.setString(1,ln[0]);
                           statement.setString(2,ln[1]);
                           statement.setString(3,ln[2]);
                           statement.setString(4,ln[3]);
                           statement.setString(5,ln[4]);
                           statement.setString(6,ln[5]);
                           statement.setString(7,ln[6]);
                           statement.setString(8,ln[7]);
                           statement.setString(9,ln[8]);
                           rows = statement.executeUpdate();
                           if(rows>0){
                               count++;
                           }
                               if(count ==500){
                                   break;
                               }
		}

	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	} finally {
		if (br != null) {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
      conn.close();
   }catch(SQLException se){
      se.printStackTrace();
   }catch(Exception e){
     
      e.printStackTrace();
   }finally{
    
      try{
         if(stmt!=null)
            stmt.close();
      }catch(SQLException se2){
      }
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }
   }
   System.out.println("Program end");
}
    
}
