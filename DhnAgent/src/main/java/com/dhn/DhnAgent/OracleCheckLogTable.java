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
	
	public void CreateLogTable() {
		
		Connection userCon = null;
		
		Date month = new Date();
		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month);
		String[] types = {"TABLE"};
		
		try {
			userCon = userSource.getConnection(); 
			
			DatabaseMetaData md = userCon.getMetaData();
			
			ResultSet rs = md.getTables(null, "dhn", "cb_dhn_broadcast_"+ monthStr, types);
			
			if(!rs.next()) {
				Statement CRTTable = userCon.createStatement();

create table cb_dhn_broadcast_201905
(
  msg_id       number,
  msg_gb       varchar2(5),
  bc_msg_id    number,
  bc_snd_st    varchar2(1),
  bc_snd_phn   varchar2(20),
  bc_rcv_phn   varchar2(20),
  bc_rslt_no   varchar2(5),
  bc_rslt_text varchar2(255),
  bc_snd_dttm  date,
  bc_rcv_dttm  date
)
;
-- Create/Recreate indexes 
create index msg_id_2019 on cb_dhn_broadcast_201905 (msg_id);
create index bc_snd_st_2019 on cb_dhn_broadcast_201905 (bc_snd_st, bc_snd_dttm);
alter table cb_dhn_broadcast_201905
  add constraint bc_msg_id primary key (BC_MSG_ID);
				
				String CreateSTR =  "CREATE TABLE `cb_dhn_broadcast_"+ monthStr +"` (" + 
									"		`MSG_ID` INT(11) NOT NULL," + 
									"		`MSG_GB` VARCHAR(5) NOT NULL," + 
									"		`BC_MSG_ID` BIGINT(20) UNSIGNED NOT NULL," + 
									"		`BC_SND_ST` CHAR(1) NULL DEFAULT NULL," + 
									"		`BC_SND_PHN` VARCHAR(20) NOT NULL," + 
									"		`BC_RCV_PHN` VARCHAR(20) NOT NULL," + 
									"		`BC_RSLT_NO` VARCHAR(5) NULL DEFAULT NULL," + 
									"		`BC_RSLT_TEXT` VARCHAR(255) NULL DEFAULT NULL," + 
									"		`BC_SND_DTTM` DATETIME NULL DEFAULT NULL," + 
									"		`BC_RCV_DTTM` DATETIME NULL DEFAULT NULL," + 
									"		PRIMARY KEY (`BC_MSG_ID`)," + 
									"		INDEX `idx_cb_dhn_broadcast_" + monthStr + "_MSG_ID` (`MSG_ID`)," + 
									"		INDEX `BC_SND_ST_BC_SND_DTTM` (`BC_SND_ST`, `BC_SND_DTTM`)" + 
									"	)" + 
									"	COLLATE='utf8_general_ci'" + 
									"	ENGINE=InnoDB";
				CRTTable.executeUpdate(CreateSTR);
				CRTTable.close();
			}
			rs.close();	

			ResultSet rs2 = md.getTables(null, "dhn", "cb_dhn_msg", types);
			
			if(!rs2.next()) {
				Statement CRTTable2 = userCon.createStatement();
				
create table cb_dhn_msg
(
  msg_id       number,
  msg_gb       varchar2(3),
  msg_st       varchar2(1),
  msg_ins_dttm date,
  msg_req_dttm date,
  msg_snd_phn  varchar2(20),
  msg_rcv_phn  varchar2(20),
  subject      varchar2(50),
  text         varchar2(3000),
  file_path1   varchar2(255),
  file_path2   varchar2(255),
  file_path3   varchar2(255),
  cb_msg_id    varchar2(20),
  dhn_msg_id   number,
  msg_cnt      number
)
;
create index msg_st on XXPO.cb_dhn_msg (msg_st, dhn_msg_id);
alter table cb_dhn_msg
  add constraint msg_id primary key (MSG_ID);
				
				String CreateSTR =  "			CREATE TABLE `cb_dhn_msg` (" + 
									"					`MSG_ID` INT(11) NOT NULL AUTO_INCREMENT," + 
									"					`MSG_GB` CHAR(3) NOT NULL," + 
									"					`MSG_ST` CHAR(1) NOT NULL DEFAULT '0'," + 
									"					`MSG_INS_DTTM` DATETIME NOT NULL," + 
									"					`MSG_REQ_DTTM` DATETIME NOT NULL," + 
									"					`MSG_SND_PHN` VARCHAR(20) NOT NULL," + 
									"					`MSG_RCV_PHN` VARCHAR(2000) NOT NULL," + 
									"					`SUBJECT` VARCHAR(50) NULL DEFAULT NULL," + 
									"					`TEXT` VARCHAR(2255) NOT NULL," + 
									"					`FILE_PATH1` VARCHAR(255) NULL DEFAULT NULL," + 
									"					`FILE_PATH2` VARCHAR(255) NULL DEFAULT NULL," + 
									"					`FILE_PATH3` VARCHAR(255) NULL DEFAULT NULL," + 
									"					`CB_MSG_ID` VARCHAR(20) NULL DEFAULT NULL," + 
									"					`DHN_MSG_ID` INT(11) NULL DEFAULT NULL," + 
									"					`MSG_CNT` INT(11) NULL DEFAULT NULL," + 
									"					PRIMARY KEY (`MSG_ID`)," + 
									"					INDEX `MSG_ST` (`MSG_ST`, `DHN_MSG_ID`)" + 
									"				)" + 
									"				COLLATE='utf8_general_ci'" + 
									"				ENGINE=InnoDB" + 
									"				AUTO_INCREMENT=1";
				CRTTable2.executeUpdate(CreateSTR);
				CRTTable2.close();
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


