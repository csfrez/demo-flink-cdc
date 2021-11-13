package com.csfrez.flink.cdc.tool;

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

}
