package com.dhn.MartAAgent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

@Repository
public class UserMsgSent {
	
	@Autowired
	@Qualifier("dhnds")
    DataSource dhnSource ;

	
	@Autowired
	@Qualifier("userds")
    DataSource userSource ;
	
	static boolean isRunning;
	
	public void SentMsg(String _monthStr) {
		
		Logger log = LoggerFactory.getLogger(getClass());

		if(!UserMsgSent.isRunning) {
			//log.info("Message Sent Start!!");
			UserMsgSent.isRunning = true;
			int totalsent = 0;
			Connection userCon = null;
			Connection dhnCon = null;
			try {
				userCon = userSource.getConnection();
				dhnCon = dhnSource.getConnection();
				
				String userQuery = "";
				
				if(DbInfo.DBMS.toUpperCase().equals("MYSQL") || DbInfo.DBMS.toUpperCase().equals("MARIADB")) {
					userQuery = "select * from " + DbInfo.MSG_TABLE + " where msg_st = 0 and dhn_msg_id is null limit 0, 50";
				}
				
				if(DbInfo.DBMS.toUpperCase().equals("ORACLE")) {
					userQuery = "select * from " + DbInfo.MSG_TABLE + " where msg_st = 0 and dhn_msg_id is null and rownum <= 50";
				}
				
				//log.info("Message Sent Query : " + userQuery + " ( " + DbInfo.DBMS.toUpperCase() + " )");
				
				Statement stm = userCon.createStatement();
				ResultSet rs = stm.executeQuery(userQuery);

				String insertQuery = "INSERT INTO cb_grs_msg  ( MSG_GB" + 
															" , MSG_ST" + 
															" , MSG_INS_DTTM" + 
															" , MSG_REQ_DTTM" + 
															" , MSG_SND_PHN" + 
															" , MSG_RCV_PHN" + 
															" , SUBJECT" + 
															" , TEXT" + 
															" , FILE_PATH1" + 
															" , FILE_PATH2" + 
															" , FILE_PATH3) " + 
															" VALUES " + 
															" ( ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?" + 
															" , ?)";
				
				String updateQuery = "update " + DbInfo.MSG_TABLE + " set dhn_msg_id = ?, msg_st = 2, msg_cnt = ? where msg_id = ?";
				//rs.first();
				//rs.next();
				//log.info("MsG TEXT", rs.getString("TEXT"));

				while(rs.next()) {
					//log.info("MsG TEXT", rs.getString("TEXT"));
					
					PreparedStatement dhnIns = dhnCon.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);
					dhnIns.setString(1, rs.getString("MSG_GB"));
					dhnIns.setString(2, rs.getString("MSG_ST"));
					dhnIns.setString(3, rs.getString("MSG_INS_DTTM"));
					dhnIns.setString(4, rs.getString("MSG_REQ_DTTM"));
					dhnIns.setString(5, rs.getString("MSG_SND_PHN"));
					dhnIns.setString(6, rs.getString("MSG_RCV_PHN"));
					dhnIns.setString(7, rs.getString("SUBJECT"));
					dhnIns.setString(8, rs.getString("TEXT"));
					dhnIns.setString(9, rs.getString("FILE_PATH1"));
					dhnIns.setString(10, rs.getString("FILE_PATH2"));
					dhnIns.setString(11, rs.getString("FILE_PATH3"));
					int rowAffected = dhnIns.executeUpdate();
					
		            if(rowAffected == 1)
		            {
		                // get candidate id
		            	ResultSet rstemp = null;
		            	rstemp = dhnIns.getGeneratedKeys();
		                if(rstemp.next()) {
		                	PreparedStatement userUpdate = userCon.prepareStatement(updateQuery);
		                	userUpdate.setInt(1, rstemp.getInt(1));
		                	userUpdate.setInt(2, rs.getString("MSG_RCV_PHN").split(",").length);
		                	userUpdate.setInt(3, rs.getInt("msg_id"));
		                	userUpdate.executeUpdate();
		                	userUpdate.close();
		                	
		                	String dhn_mapper = "insert into cb_grs_msg_user_map(grs_msg_id, grs_mem_id, grs_yyyymm) values(?,?,?)";
		                	PreparedStatement dhnmapins = dhnCon.prepareStatement(dhn_mapper);
		                	dhnmapins.setInt(1, rstemp.getInt(1));
		                	dhnmapins.setInt(2, CronTable.userInfo.user_id);
		                	dhnmapins.setString(3, _monthStr);
		                	dhnmapins.executeUpdate();
		                	dhnmapins.close();
		                	
		                }
		            } else {
		            	
		            }
		            dhnIns.close();
		            totalsent++;
				}

				//log.info("RS SIZE : " + rs.getr);
				
			} catch (SQLException e) {
				log.error(e.toString());
			}

			if(totalsent > 0)
				log.info("" + totalsent + " Message Sent !!");
			
			try {
				if(userCon != null)
					userCon.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				if(dhnCon != null)
					dhnCon.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			UserMsgSent.isRunning = false;
		}
	}
	
}
