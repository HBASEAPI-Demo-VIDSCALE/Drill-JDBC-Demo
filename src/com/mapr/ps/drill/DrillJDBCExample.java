
/**
 * Created by Venkata Gutta.
 */

package com.mapr.ps.drill;
import java.sql.*;

public class DrillJDBCExample {
    static final String JDBC_DRIVER = "org.apache.drill.jdbc.Driver";
    static final String DB_URL = "jdbc:drill:zk=mapr-01.fra.cdx-dev.unifieddeliverynetwork.net,mapr-02.fra.cdx-dev.unifieddeliverynetwork.net:5181,mapr-03.fra.cdx-dev.unifieddeliverynetwork.net:5181/drill/vidscale-poc-drillbits";
    //static final String DB_URL = "jdbc:drill:drillbit=mapr-05.fra.cdx-dev.unifieddeliverynetwork.net";
  //static final String DB_URL = "jdbc:drill:drillbit=mapr-05.fra.cdx-dev.unifieddeliverynetwork.net";

    static final String USER = "mapr";
    static final String PASS = "mapr-admin";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName(JDBC_DRIVER);
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            stmt = conn.createStatement();
            /* Perform a select on data in the classpath storage plugin. */
            String sql = "SELECT CONVERT_FROM(row_key, 'UTF8') AS row_key, CONVERT_FROM(demotable.cf1.col1, 'UTF8') AS column1, CONVERT_FROM(demotable.cf1.col2, 'UTF8') AS column2 FROM maprdb.demotable";
            ResultSet rs = stmt.executeQuery(sql);

            while(rs.next()) {
                String id  = rs.getString("row_key");
                String name = rs.getString("column1");
                String last = rs.getString("column2");

                System.out.print("Key: " + id);
                System.out.print(", Column1: " + name);
                //System.out.println(", Column2: " + last);
            }

            rs.close();
            stmt.close();
            conn.close();
        } catch(SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch(Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            try{
                if(stmt!=null)
                    stmt.close();
            } catch(SQLException se2) {
            }
            try {
                if(conn!=null)
                    conn.close();
            } catch(SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
