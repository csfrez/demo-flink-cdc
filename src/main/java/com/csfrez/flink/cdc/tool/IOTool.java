package com.csfrez.flink.cdc.tool;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class IOTool {


    public static void close(ResultSet rs){
        try{
            if(rs != null)
                rs.close();
        } catch (Exception e){
        }
    }

    public static void close(Statement stmt){
        try{
            if(stmt != null)
                stmt.close();
        } catch (Exception e){
        }
    }

    public static void close(PreparedStatement pstmt){
        try{
            if(pstmt != null)
                pstmt.close();
        } catch (Exception e){
        }
    }

    public static void close(Connection conn){
        try{
            if(conn != null)
                conn.close();
        } catch (Exception e){
        }
    }

    public static void close(InputStream inputStream){
        try{
            if(inputStream != null)
                inputStream.close();
        } catch (Exception e){
        }
    }

}
