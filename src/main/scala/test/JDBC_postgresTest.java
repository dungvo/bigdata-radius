package test;

import java.sql.*;

/**
 * Created by hungdv on 29/07/2017.
 */
public class JDBC_postgresTest {

    private static final String DB_DRIVER = "org.postgresql.Driver";
    private static final String DB_CONNECTION = "jdbc:postgresql://localhost:5432/dwh_noc";
    private static final String DB_USER = "postgres";
    private static final String DB_PWD = "hung";

    public static void   main(String[] args) {
        try{
            Class.forName(DB_DRIVER).newInstance();
            Connection conn = DriverManager.getConnection(DB_CONNECTION,DB_USER,DB_PWD);
            Statement stmt = conn.createStatement();
            boolean result = stmt.execute("insert into inserttest(bras_id,olt,portpon,time) " +
                    " select b.bras_id, bh.olt, bh.portpon,b.time " +
                    " from bras_count b join brashostmapping bh on b.bras_id = bh.bras_id ");
            System.out.println(result);

            /*ResultSet rs = stmt.executeQuery("SELECT * from brashostmapping");
            while(rs.next()){
                System.out.println(rs.toString());
            }*/
        }
        catch (ClassNotFoundException ex) {System.err.println(ex.getMessage());}
        catch (IllegalAccessException ex) {System.err.println(ex.getMessage());}
        catch (InstantiationException ex) {System.err.println(ex.getMessage());}
        catch (SQLException ex)           {System.err.println(ex.getMessage());}


    }

}
