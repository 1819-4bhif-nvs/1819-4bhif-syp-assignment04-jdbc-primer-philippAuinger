package at.htl.supermarket;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SupermarketTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJdbc(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbinung zur Datenbank nicht möglich!\n" + e.getMessage() + "\n");
            System.exit(1);
        }

        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE supermarket(" +
                    "ID INT CONSTRAINT MARKET_PK PRIMARY KEY," +
                    "NAME VARCHAR(255) NOT NULL," +
                    "LOCATION VARCHAR(255)" +
                    ")";

            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        try {
            Statement stmt = conn.createStatement();
            String sql = "CREATE TABLE product(" +
                    "ID INT CONSTRAINT PRODUCT_PK PRIMARY KEY," +
                    "MARKET INT," +
                    "NAME VARCHAR(255) NOT NULL," +
                    "PRICE FLOAT," +
                    "QUANTITY INT NOT NULL," +
                    "FOREIGN KEY(MARKET) REFERENCES SUPERMARKET(ID)" +
                    ")";

            stmt.execute(sql);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
        System.out.println("Tabelle SUPERMARKET erstellt.");
        System.out.println("Tabelle PRODUCT erstellt.");
    }

    @Test
    public void t01_metadataTest(){
        try {
            DatabaseMetaData metadata = conn.getMetaData();

            //get your tables
            ResultSet rs = metadata.getTables(null, null, null, new String[]{"TABLE"});
            rs.next();
            assertThat(rs.getString("TABLE_NAME"),is("PRODUCT"));
            rs.next();
            assertThat(rs.getString("TABLE_NAME"),is("SUPERMARKET"));

            //get your PKs
            rs = metadata.getPrimaryKeys(null,null, "PRODUCT");
            rs.next();
            assertThat(rs.getString("PK_NAME"),is("PRODUCT_PK"));

            rs = metadata.getPrimaryKeys(null,null, "SUPERMARKET");
            rs.next();
            assertThat(rs.getString("PK_NAME"),is("MARKET_PK"));

            //get your FKs
            rs = metadata.getImportedKeys(null,null, "PRODUCT");
            rs.next();
            assertThat(rs.getString("FKTABLE_NAME"),is("PRODUCT"));
            assertThat(rs.getString("PKTABLE_NAME"),is("SUPERMARKET"));
            assertThat(rs.getString("FKCOLUMN_NAME"),is("MARKET"));

            //Datatypes
            rs = metadata.getColumns(null,null,"PRODUCT",null);
            rs.next();
            assertThat(rs.getString("DATA_TYPE"),is("4")); //4 = INTEGER
            rs.next();
            rs.next();
            assertThat(rs.getString("DATA_TYPE"),is("12")); //12 = VARCHAR



        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void t02_insertion(){
        int countInserts = 0;
        try{
            //region INSERTS
            //SUPERMARKET
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO SUPERMARKET (ID, NAME, LOCATION) VALUES(?,?,?)");
            stmt.setInt(1,1);
            stmt.setString(2,"ADEG");
            stmt.setString(3,"Grünau im Almtal");
            countInserts += stmt.executeUpdate();

            stmt.setInt(1,2);
            stmt.setString(2,"SPAR");
            stmt.setString(3,"Linz Bahnhof");
            countInserts += stmt.executeUpdate();

            stmt.setInt(1,3);
            stmt.setString(2,"BILLA");
            stmt.setString(3,"Leonding Meixnerkreuzung");
            countInserts += stmt.executeUpdate();

            //PRODUCT
            stmt = conn.prepareStatement("INSERT INTO PRODUCT (ID, MARKET, NAME, PRICE, QUANTITY) VALUES(?,?,?,?,?)");
            stmt.setInt(1,1);
            stmt.setInt(2,1);
            stmt.setString(3,"SMOOTHIE");
            stmt.setDouble(4, 2.20);
            stmt.setInt(5, 90);
            countInserts += stmt.executeUpdate();

            stmt.setInt(1,2);
            stmt.setInt(2,1);
            stmt.setString(3,"Banane");
            stmt.setDouble(4, 1.00);
            stmt.setInt(5, 14);
            countInserts += stmt.executeUpdate();

            stmt.setInt(1,3);
            stmt.setInt(2,2);
            stmt.setString(3,"Apfel");
            stmt.setDouble(4, 0.90);
            stmt.setInt(5, 120);
            countInserts += stmt.executeUpdate();

            stmt.setInt(1,4);
            stmt.setInt(2,3);
            stmt.setString(3,"Wasser");
            stmt.setDouble(4, 0.50);
            stmt.setInt(5, 102);
            countInserts += stmt.executeUpdate();

            stmt.setInt(1,5);
            stmt.setInt(2,2);
            stmt.setString(3,"SBudget - Käse");
            stmt.setDouble(4, 5.31);
            stmt.setInt(5, 39);
            countInserts += stmt.executeUpdate();
            //endregion

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        assertThat(countInserts, is(8));
    }

    @Test
    public void t03_insertionTest(){
        try{
            PreparedStatement pstmt = conn.prepareStatement("SELECT NAME, LOCATION FROM SUPERMARKET");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("NAME"),is("ADEG"));
            assertThat(rs.getString("LOCATION"),is("Grünau im Almtal"));
            rs.next();
            assertThat(rs.getString("NAME"),is("SPAR"));
            assertThat(rs.getString("LOCATION"),is("Linz Bahnhof"));
            rs.next();
            assertThat(rs.getString("NAME"),is("BILLA"));
            assertThat(rs.getString("LOCATION"),is("Leonding Meixnerkreuzung"));

            pstmt = conn.prepareStatement("SELECT PRODUCT.name, M.name FROM PRODUCT JOIN SUPERMARKET M ON PRODUCT.MARKET = M.ID");
            rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString(1),is("SMOOTHIE"));
            assertThat(rs.getString(2),is("ADEG"));
            rs.next();
            rs.next();
            assertThat(rs.getString(1),is("Apfel"));
            assertThat(rs.getString(2),is("SPAR"));
        } catch(SQLException e){
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJdbc(){
        try{
            conn.createStatement().execute("DROP TABLE PRODUCT");
            conn.createStatement().execute("DROP TABLE SUPERMARKET");
            System.out.println("Tabelle PRODUCT gelöscht.");
            System.out.println("Tabelle SUPERMARKET gelöscht.");
        }catch(SQLException e){
            System.out.println("Tabelle konnte nicht gelöscht werden:\n"
                    + e.getMessage());
        }
        try {
            if(conn != null || !conn.isClosed()){
                conn.close();
                System.out.println("Goodbye!");

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
