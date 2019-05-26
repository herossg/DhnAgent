package com.dhn.MartAAgent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

@Repository
public class UserMsgUpdate {
	@Autowired
	@Qualifier("dhnds")
    DataSource dhnSource ;

	
	@Autowired
	@Qualifier("userds")
    DataSource userSource ;
	
	static boolean isRunning;
	
	public void MsgUpdate(String _monthStr) {
		Logger log = LoggerFactory.getLogger(getClass());
		
		String monthStr = _monthStr; 
		int totalud = 0;
		
		if(!isRunning) {
			isRunning = true;
			String userTblName = DbInfo.BROADCAST_TABLE + monthStr;
			Connection userCon = null;
			
			try {
				userCon = userSource.getConnection();

				String userQuery = "select * from " + DbInfo.MSG_TABLE + " dm where dm.MSG_CNT = (select count(1) from " + userTblName + " db where db.MSG_ID = dm.DHN_MSG_ID)";
				Statement stm = userCon.createStatement();
				ResultSet rs = stm.executeQuery(userQuery);
				
				
				String updateQuery = "update " + DbInfo.MSG_TABLE + " set msg_st = 3 where msg_id = ?";
				
				while(rs.next()) {
					PreparedStatement userUpdate = userCon.prepareStatement(updateQuery);
					userUpdate.setString(1, rs.getString("MSG_ID"));
					userUpdate.executeUpdate();
					userUpdate.close();
					totalud++;
				}
				
			} catch (SQLException e) {
				log.error(e.toString());
			}
			
			
			try {
				if(userCon != null)
					userCon.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			isRunning = false;
		}
	}
}
