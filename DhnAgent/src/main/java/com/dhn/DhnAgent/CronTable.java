package com.dhn.DhnAgent;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import javax.print.attribute.standard.DateTimeAtCompleted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Service
public class CronTable {
	
//	@Autowired
//	PriceDao priceDao;
	
	@Autowired
	UserMsgSent userMsgSent;
	
	@Autowired
	UserMsgReceive userMsgReceive;
	
	@Autowired
	UserMsgUpdate userMsgUpdate;
	
	@Autowired
	CheckLogTable checkLogTagle;
	
	boolean isRunning = false;
	static UserInfo userInfo = new UserInfo();
	Logger log = LoggerFactory.getLogger(getClass());
	
	@Scheduled(initialDelay=1000, fixedDelay = 5000)
	public void MsgSentJob() {
		Date month = new Date();
		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month);
		
		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart)
				userMsgSent.SentMsg(monthStr);
		}
	}

	@Scheduled(initialDelay=1000, fixedDelay = 3000)
	public void MsgReceiveJob() {
		Date month = new Date();
		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month);
		
		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart)
				userMsgReceive.ReceiveMsg(monthStr);
		}
	}

	@Scheduled(initialDelay=1000, fixedDelay = 5000)
	public void MsgUpdateJob() {
		Date month = new Date();
		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month);

		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart)
				userMsgUpdate.MsgUpdate(monthStr);
		}
	}

	@Scheduled(cron="0 5 0 1 * *")
	public void CheckLogTable() {
		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart)
				checkLogTagle.CreateLogTable();
		}
	}
	
	
	@Scheduled(initialDelay=1000, fixedDelay = 3600000)
	public void StartCheckLogTable() {
		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart) {
				if(!isRunning) {
					Logger log = LoggerFactory.getLogger(getClass());
					log.info("시작 시 Log Table 확인 ");
					
					checkLogTagle.CreateLogTable();
					isRunning = true;
				}
			}
		}
	}
	

	@Scheduled(cron="1 * * 1 * *")
	public void preMsgReceiveJob() {
		Calendar month = Calendar.getInstance();
		month.add(month.MONTH,-1);

		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month.getTime());
		
		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart)
				userMsgReceive.ReceiveMsg(monthStr);
		}
	}

	@Scheduled(cron="1 * * 1 * *")
	public void preMsgUpdateJob() {
		Calendar month = Calendar.getInstance();
		month.add(month.MONTH,-1);

		SimpleDateFormat transFormat = new SimpleDateFormat("yyyyMM");
		String monthStr = transFormat.format(month.getTime());

		if(userInfo.user_name.isEmpty()) 
		{
			log.error("사용자 정보가 존재 하지 않습니다.");
		} else {
			if(DataSourceProperties.isStart)
				userMsgUpdate.MsgUpdate(monthStr);
		}
	}

	
}
