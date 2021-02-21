package databasetest;

import java.sql.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class DatabaseTest {
    
    public static JSONArray getJSONData(){
        JSONArray jsonArray = new JSONArray();
        
        //INSERT YOUR CODE HERE
        Connection conn = null;
        PreparedStatement pstSelect = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        String query, key, value;
        boolean hasresults;
        int resultCount, columnCount, updateCount = 0;
        
        try{
            // Identify Server
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS488";
            //System.out.println("Connecting to " + server + "...");
            
            // Load MySQL JDBC Driver
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            // Open Connection
            conn = DriverManager.getConnection(server, username, password);
            
            if(conn.isValid(0)){
                //System.out.println("Connected Successfully!");
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                //System.out.println("Submitting query...");
                
                hasresults = pstSelect.execute();
                
                //System.out.println("Getting results...");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ){
                    
                    if(hasresults){
                        // Get ResultSet MetaData
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        String[] columnNames = new String[columnCount];
                        // Get Column Names and Print as Table Header
                        for (int i = 2; i <= columnCount; i++) {
                            columnNames[i-2] = metadata.getColumnLabel(i);
                            
                            //key = metadata.getColumnLabel(i);
                            //System.out.format("%30s", key);
                        }
                        
                        // Get data and print as table rows
                        while(resultset.next()){
                            //System.out.println();
                            
                            //Loop through ResultSet Columns and Print Values
                            JSONObject jsonObject = new JSONObject();
                            
                            for (int i = 2; i <= columnCount; i++) {
                                value = resultset.getString(i);
                                
                                jsonObject.put(columnNames[i-2], value);
                                
                                
                                /*
                                if (resultset.wasNull()) {
                                    System.out.format("%30s", "NULL");
                                }
                                else {
                                    System.out.format("%30s", value);
                                }
                                */
                            }
                            
                            jsonArray.add(jsonObject);
                            
                            
                        }
                    }
                    else {
                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }
                    }
                    
                    // Check for more data
                    hasresults = pstSelect.getMoreResults();
                }
            }
            
            //System.out.println();
            
            // Close the database connection
            conn.close();
        }
        catch(Exception e){
            System.err.println(e.toString());
        }
        finally { 
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            // if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} } 
        }
        
        //System.out.println();
        System.out.println(jsonArray.toString());
        
        return jsonArray;
    }

    public static void main(String[] args) {
        getJSONData();
    }
    
}