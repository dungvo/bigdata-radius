package test;

import java.sql.*;

/**
 * Created by hungdv on 29/07/2017.
 */
public class JDBC_postgresTest {

    private static final String DB_DRIVER = "org.postgresql.Driver";
    private static final String DB_CONNECTION = "jdbc:postgresql://172.27.11.153:5432/dwh_noc?user=dwh_noc&password=bigdata";
    //private static final String DB_CONNECTION = "jdbc:postgresql://localhost:5432/dwh_noc";
    private static final String DB_USER = "noc_realtime";
    private static final String DB_PWD = "bigdata";

    public static void   main(String[] args) {
        System.out.println("Hello world");
        try{
            Class.forName(DB_DRIVER).newInstance();
            Connection conn = DriverManager.getConnection(DB_CONNECTION);
            //Connection conn = DriverManager.getConnection(DB_CONNECTION,DB_USER,DB_PWD);

            System.out.println("conntion : " + conn.toString());
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * from brashostmapping");
            while(rs.next()){
                System.out.println("Result : " + rs.toString());
            }
            boolean result = stmt.execute("insert into inserttest(bras_id,olt,portpon,time) " +
                    " select b.bras_id, bh.olt, bh.portpon,b.time " +
                    " from bras_count b join brashostmapping bh on b.bras_id = bh.bras_id ");
            System.out.println(result);


        }
        catch (ClassNotFoundException ex) {System.err.println("Class not found " + ex.getMessage());}
        catch (IllegalAccessException ex) {System.err.println(ex.getMessage());}
        catch (InstantiationException ex) {System.err.println(ex.getMessage());}
        catch (SQLException ex)           {System.err.println("SQL exception " + ex.getMessage());}


    }

}
