package com.dhn.DhnAgent;

import java.sql.*;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import java.util.Date;

import javax.sql.DataSource;

@Repository
public class OracleCheckLogTable {
	
	@Autowired
	@Qualifier("userds")
    DataSource userSource ;
	
	Logger log = LoggerFactory.getLogger(getClass());
	
	public void OracleCreateLogTable() {
		
		Connection userCon = null;

		Logger log = LoggerFactory.getLogger(getClass());
		log.info("oracle log table checked ");

		Date month = new Date();
		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month);
		String[] types = {"TABLE"};
		
		try {
			userCon = userSource.getConnection(); 
			
			DatabaseMetaData md = userCon.getMetaData();
			
			ResultSet rs = md.getTables(null, DbInfo.SID, "cb_dhn_broadcast_"+ monthStr, types);
			if(!rs.next()) {
				Statement CRTTable = userCon.createStatement();
				
				String CreateSTR =  "create table cb_dhn_broadcast_"+ monthStr +   
												"( msg_id       number," + 
												"  msg_gb       varchar2(5)," + 
												"  bc_msg_id    number," + 
												"  bc_snd_st    varchar2(1)," + 
												"  bc_snd_phn   varchar2(20)," + 
												"  bc_rcv_phn   varchar2(20)," + 
												"  bc_rslt_no   varchar2(5)," + 
												"  bc_rslt_text varchar2(255)," + 
												"  bc_snd_dttm  date," + 
												"  bc_rcv_dttm  date" + 
												") ";
								
				CRTTable.executeUpdate(CreateSTR);
				CRTTable.close();
				
				Statement CRT_IDX1 = userCon.createStatement();
				CreateSTR =  "create index msg_id_"+ monthStr + " on cb_dhn_broadcast_"+ monthStr + "(msg_id)";
				CRT_IDX1.executeUpdate(CreateSTR);
				CRT_IDX1.close();

				Statement CRT_IDX2 = userCon.createStatement();
				CreateSTR =  "create index bc_snd_st_"+ monthStr + " on cb_dhn_broadcast_"+ monthStr + "(bc_snd_st, bc_snd_dttm)";
				CRT_IDX2.executeUpdate(CreateSTR);
				CRT_IDX2.close();

				Statement CRT_IDX3 = userCon.createStatement();
				CreateSTR =  "ALTER TABLE cb_dhn_broadcast_"+ monthStr + " add constraint bc_msg_id_"+ monthStr + " primary key (BC_MSG_ID)";
				CRT_IDX3.executeUpdate(CreateSTR);
				CRT_IDX3.close();

			}
			rs.close();	

			ResultSet rs2 = md.getTables(null, DbInfo.SID,  "cb_dhn_msg", types);
			
			if(!rs2.next()) {
				Statement CRTTable2 = userCon.createStatement();
 				
				String CreateSTR =  "create table cb_dhn_msg" + 
												"(" + 
												"  msg_id       number," + 
												"  msg_gb       varchar2(3)," + 
												"  msg_st       varchar2(1)," + 
												"  msg_ins_dttm date," + 
												"  msg_req_dttm date," + 
												"  msg_snd_phn  varchar2(20)," + 
												"  msg_rcv_phn  varchar2(20)," + 
												"  subject      varchar2(50)," + 
												"  text         varchar2(3000)," + 
												"  file_path1   varchar2(255)," + 
												"  file_path2   varchar2(255)," + 
												"  file_path3   varchar2(255)," + 
												"  cb_msg_id    varchar2(20)," + 
												"  dhn_msg_id   number," + 
												"  msg_cnt      number" + 
												")";
				CRTTable2.executeUpdate(CreateSTR);
				CRTTable2.close();

				Statement CRT_IDX1 = userCon.createStatement();
				CreateSTR =  "create index msg_st on cb_dhn_msg(msg_st, dhn_msg_id)";
				CRT_IDX1.executeUpdate(CreateSTR);
				CRT_IDX1.close();

				Statement CRT_IDX2 = userCon.createStatement();
				CreateSTR =  "alter table cb_dhn_msg  add constraint msg_id primary key (MSG_ID)";
				CRT_IDX2.executeUpdate(CreateSTR);
				CRT_IDX2.close();

			}
			rs2.close();	
			
		} catch(Exception ex) {}
			
		try {
			if(userCon != null)
				userCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
} 


