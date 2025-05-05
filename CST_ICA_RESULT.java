/*
 *     Date     Ver.        Programmer 		Note
 *  ==========  =========	==============	=====================================================
 *  2025/04/10	V3.0.11		Emily Lai		Initial version.
 */

package com.cjkk.ccms.rule.ilcp;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Hashtable;

import com.cjkk.ccms.cassette.CassetteCurrInfoBean;
import com.cjkk.ccms.cassette.CassetteCurrInfoDB;
import com.cjkk.ccms.cassette.CassetteTransactionDB;
import com.cjkk.ccms.cfg.SystemCodeBean;
import com.cjkk.ccms.cfg.SystemCodeDB;
import com.cjkk.ccms.core.Alarm;
import com.cjkk.ccms.core.CCMSReturnCode;
import com.cjkk.ccms.core.COREHandler;
import com.cjkk.ccms.core.CommunicationDB;
import com.cjkk.ccms.core.SystemStatusBean;
import com.cjkk.ccms.db.DBConnection;
import com.cjkk.ccms.mcs.MoveRequest;
import com.cjkk.ccms.mcs.MoveRequestInfo;
import com.cjkk.ccms.mes.ReceiveNewCassette;
import com.cjkk.ccms.mes.ReturnEmptyCassette;
import com.cjkk.ccms.opi.InqShipRecvBean;
import com.cjkk.ccms.opi.InqShipRecvDB;
import com.cjkk.ccms.port.BasePort;
import com.cjkk.ccms.port.PortProcess;
import com.cjkk.ccms.rule.RuleSchema;
import com.cjkk.ccms.rule.ilcp.CST_ICA_RESULT;
import com.cjkk.ccms.tx.AMQTxFacade;
import com.cjkk.ccms.util.AlarmException;
import com.cjkk.ccms.util.CCMSConst;
import com.cjkk.ccms.util.CITLogger;
import com.cjkk.ccms.util.CustomLevel;
import com.cjkk.ccms.util.StringUtil;
import com.cjkk.ccms.util.csHashtable;
import com.cjkk.ccms.util.CCMSConst.SystemName;
import com.cjkk.ccms.util.CCMSConst.SystemType;

/**
 * <p>Title: CST_ICA_RESULT</p>
 * <p>Description: iLCP to CCMS "CST_ICA_RESULT" transaction</p>
 * 	  1.Check ICA_AUTO status, if status = manual then return error code.
 * 	  2.Check CST should at ICA port, if not at ICA port then return error code.
 *    3.Check CST ID should exist in DB, if not exist then return error code.
 *    4.If CST REG_STATUS = unregistered, do RegNewCst.
 *    5.If ICA_RESULT = NG, do nothing and force move to stocker.
 *    6.If ICA_RESUT = OK, do CST Empty Event to MES and auto move to clean port.
 *    
 * @author Emily Lai
 */

public class CST_ICA_RESULT extends RuleSchema {

	private static CITLogger log = CITLogger.getInstance(CST_ICA_RESULT.class);
	public String returnCode = "0";
	private String cstId;
	private String ICAResult;	
	
	public CST_ICA_RESULT() { }
	
	public CST_ICA_RESULT(AMQTxFacade tx, Connection conn, Hashtable htmsg) 
	{
        super(tx, conn, htmsg);
        this.systemName = CCMSConst.SystemName.ILCP;
    }
	
	//send replayData first,
    public void doStart() throws Exception
    {    
    	log.ILCP("CST_ICA_RESULT:start");
        checkValue();
        initProcess(true);
        makeReplyData();
        sendResponseData();         
        doProcess();     
        log.ILCP("CST_ICA_RESULT:end");
    }
    
    public boolean checkValue() throws Exception
    {    	       
        this.cstId = getStringValue(request, "cst_id");
        this.ICAResult = getStringValue(request, "ica_result");        
        this.action = CCMSConst.TRX_ACTION.CST_ICA_RESULT;
        boolean flag = super.checkValue();
        
        try {
        	//Check ICA_AUTO status
            if (!isAutoMode()) {
                this.procFlag = false;
                Alarm alarm = new Alarm();
                alarm.alarmOn(this.conn, this.portId, CCMSConst.ALARM_ACTION.ICA_MODE_IS_MANUAL, this.cstId,
                              this.userId, "CCMS", this.returnMsg);
                return false;
            }
        } catch (AlarmException ex) {
            log.error(ex);
        } catch (Exception ex) {
            log.error(ex);
        }

        return flag;
    };
    
    private void initProcess(boolean isOnline) 
    {
    	if (!this.procFlag) {
            return;
        }
    	
		try {
			CassetteCurrInfoBean cstBean = null;
			CassetteCurrInfoDB cstDb = new CassetteCurrInfoDB();		
			cstBean = cstDb.selectCassetteById(conn, cstId);			
			String result = CCMSConst.TRX_RESULT.SUCCESS;
			
			if (cstBean == null) {
				this.returnCode = CCMSConst.ILCP_RTCODE.CST_NOT_EXIST;
				this.returnMsg = CCMSConst.ILCP_RTMSG.CST_NOT_EXIST;
				log.error(returnMsg + ":" + cstId);

				this.procFlag = false;
				Alarm alarm = new Alarm();
				alarm.alarmOn(conn, portId, CCMSConst.ALARM_ACTION.CST_ID_NOT_EXIST, this.cstId, userId, CCMSConst.SYS_NAME.CCMS,
						returnMsg+" [CST ID:"+ this.cstId +"]");
				this.procFlag = false;
				return;
			}

			if (!CCMSConst.PortName.ICA.equalsIgnoreCase(cstBean.getCurrentLocationPhyPortName())) {
				this.returnCode = CCMSConst.ILCP_RTCODE.CST_NOT_AT_ICA;
				this.returnMsg = CCMSConst.ILCP_RTMSG.CST_NOT_AT_ICA;
				log.error(returnMsg + ":" + cstId);

				this.procFlag = false;
				Alarm alarm = new Alarm();
				alarm.alarmOn(conn, portId, CCMSConst.ALARM_ACTION.CST_NOT_AT_ICA_PORT, this.cstId, userId, CCMSConst.SYS_NAME.CCMS,
						returnMsg+" [CST ID:"+ this.cstId +"]");				
				return;
			}

			// If CST needs register 
			if (CCMSConst.CST_REG_STATUS.UNREG.equalsIgnoreCase(cstBean.getRegStatus())) {
				// Get dimension,capacity
				String CST_DIMENSION = "";
				String CST_CAPACITY = "";

				SystemCodeDB sysDb = new SystemCodeDB();
				SystemCodeBean cst_bean_d[] = sysDb.selectSystemCode(conn, "CST_DIMENSION");
				if (cst_bean_d.length > 0) {
					CST_DIMENSION = cst_bean_d[0].getItemCode();
				}

				SystemCodeDB sysDb_c = new SystemCodeDB();
				SystemCodeBean cst_bean_c[] = sysDb_c.selectSystemCode(conn, "CST_CAPACITY");
				if (cst_bean_c.length > 0) {
					CST_CAPACITY = cst_bean_c[0].getItemCode();
				}

				cstBean.setDimension(CST_DIMENSION);
				cstBean.setCapacity(CST_CAPACITY);
				cstBean.setUserId(userId);
				cstBean.setRegStatus(CCMSConst.CST_REG_STATUS.USED);
				cstBean.setReturnType(CCMSConst.RETURN_TYPE.NEW_CST);
				cstBean.setQuantityType(CCMSConst.QTY_TYPE.EMPTY);
				cstBean.save(conn);
				DBConnection.commit(conn);
				
				//send new cst to MES
				ReceiveNewCassette recNewCst = new ReceiveNewCassette();
                boolean flag = recNewCst.sendMsg(conn, cstBean.getCstId(), response, true, userId);
                
                Hashtable[] htArr = getSubHashTable(response, "Body"); 
                String MESRetCode = getArrStringValue(htArr, "Status");                 
                String MESRetMsg = getArrStringValue(htArr, "Description");
                
                if (StringUtil.convertReturnCode(MESRetCode) != 0) {
	                log.ILCP("Register new CST fail:" + MESRetMsg.toString());
	                result = CCMSConst.TRX_RESULT.FAIL;
                }            
                
                CassetteTransactionDB cstTblDB = new CassetteTransactionDB();
		        cstTblDB.saveCstTrx(cstBean, userId, "CCMSRegNewCst", "CST_REG", result,this.timeStamp);
			}

			// If ICAResult=OK, auto move to next station
			if (CCMSConst.REPLY_STATUS.OK.equalsIgnoreCase(ICAResult)) {
				cstBean.setIcaResult(CCMSConst.ICA_RESULT.OK);
				cstBean.setReturnType(CCMSConst.RETURN_TYPE.EMPTY_RET);
				cstBean.setQuantityType(CCMSConst.QTY_TYPE.EMPTY);
				cstBean.setCstDamageType(CCMSConst.CST_DAMAGE_TYPE.NO_DAMAGE);
				cstBean.setSheetQaHold(CCMSConst.QA_HOLD.NO_HOLD);
				cstBean.setIcaReq("N");
				cstBean.setCleanReq("Y");
				cstBean.save(conn);
				DBConnection.commit(conn);

				InqShipRecvDB srDb = new InqShipRecvDB();
				InqShipRecvBean recBean = srDb.selectLastInqShipRecv(conn, null, cstBean.getCstId(), cstBean.getCycleId(),
						CCMSConst.PT_INQ_SHIP_RECV.RECEIVE);
				
				if (recBean != null) {
					recBean.setReturnType(CCMSConst.RETURN_TYPE.EMPTY_RET);
					recBean.save(conn);
				}

				// Send Empty CST Event to MES
				ReturnEmptyCassette retEmptyCst = new ReturnEmptyCassette();
				retEmptyCst.sendMsg(conn, cstBean.getCstId(), response, true, userId);

				Hashtable[] htArr = getSubHashTable(response, "Body");
				String MESRetCode = getArrStringValue(htArr, "Status");
				String MESRetMsg = getArrStringValue(htArr, "Description");

				if (StringUtil.convertReturnCode(MESRetCode) != 0) {
					log.ILCP("Send Empty Event to MES Fail:" + MESRetMsg.toString());					
				}								
			}
			// If ICAResult=NG, force move to stock
			else if (CCMSConst.REPLY_STATUS.NG.equalsIgnoreCase(ICAResult)) {
				cstBean.setIcaResult(CCMSConst.ICA_RESULT.NG);
				cstBean.setIcaReq("Y");
				cstBean.save(conn);
				DBConnection.commit(conn);				
			}
			
			saveCommunication();
			saveCstTrx(isOnline);
		}
		catch (AlarmException ex) {                                
            log.error(ex);   
            }
		catch (Exception ex) {							
			log.error(ex);			
			DBConnection.rollback(conn);
			returnCode = Integer.toString(CCMSReturnCode.INIT_ERR);
	        returnMsg = CCMSReturnCode.getReturnMsg(CCMSReturnCode.INIT_ERR);
	        	        
			if (ex instanceof SQLException) {
				returnMsg = ex.toString();
			}			
			this.procFlag = false;
		}		
    }
    
    private boolean isAutoMode() {        
        try {
                        
            COREHandler ch = new COREHandler();
            SystemStatusBean[] sysBean = ch.selectSystemStatusBeanByType(
                    this.conn, SystemType.ICA_AUTO);

            for (SystemStatusBean s : sysBean) {
                if ("MANUAL".equalsIgnoreCase(s.getStatus().toUpperCase())) {
                    this.returnCode = CCMSConst.ILCP_RTCODE.ICA_MODE_MANUAL;
                    this.returnMsg = CCMSConst.ILCP_RTMSG.ICA_MODE_MANUAL;
                    return false;
                }
            }
        }catch (AlarmException ex) {                                
            log.error(ex);           
        }catch (Exception ex) {
            log.error(ex);
        }
        return true;
    }

	public void makeReplyData() throws Exception
	{
		String timeStamp = DBConnection.getTimestamp(conn);
        response.clear();
        response.put("tid", tid);
        response.put("action", action);
        response.put("return_code", returnCode);
        response.put("return_msg", returnMsg);
        response.put("time_stamp", timeStamp);
	}
	
	public void doProcess() throws Exception
	{												
		if (!this.procFlag) {
            return;
        }
		try {
			String result = CCMSConst.TRX_RESULT.SUCCESS;
			
			CassetteCurrInfoBean cstBean = null;
			CassetteCurrInfoDB cstDb = new CassetteCurrInfoDB();	
			cstBean = cstDb.selectCassetteById(conn, cstId);
			
			if (this.ICAResult.equalsIgnoreCase(CCMSConst.ICA_RESULT.OK))
			{
				// Auto Move to next destination
				portId = cstBean.getCurrentLocation();
				BasePort portObj = PortProcess.init(conn, portId);
				boolean flag = true;

				flag = portObj.sendAutoMoveRequest(conn, portId, cstBean, userId);

				if (flag) {
					DBConnection.commit(conn);
				} else {
					DBConnection.rollback(conn);
					cstBean.setUnloadRqst("MANUAL");
					cstBean.save(conn);
					DBConnection.commit(conn);
				}
			}
			else if (this.ICAResult.equalsIgnoreCase(CCMSConst.ICA_RESULT.NG)) 
			{			
				// Force move to stock
				Hashtable htRet = new csHashtable();        
				MoveRequest moveReq = new MoveRequest();
		        MoveRequestInfo info = new MoveRequestInfo();
		                                    
		        info.setAction(CCMSConst.MOVE_ACTION.MOVE);
		        info.setCstId(cstBean.getCstId());
			    info.setPriDest("STK-00");
			    info.setPriPriority(CCMSConst.MOVE_PRIORITY.CRITICAL);	    
			    info.setPriDetail("0000");
			    info.setUserId(CCMSConst.SYS_NAME.CCMS);
			    info.setOverWrite("N");
		       
			    boolean flag = true; 
		        
		        flag = moveReq.sendMoveRequest(this.conn, info, htRet, true);		        
		        
		        if (flag) {
					DBConnection.commit(conn);
				} else {
					DBConnection.rollback(conn);
					cstBean.setUnloadRqst("MANUAL");
					cstBean.save(conn);
					DBConnection.commit(conn);
				}    		      
			}				
		} catch (AlarmException ex) {
            DBConnection.rollback(conn);            
            cstBean.setUnloadRqst("MANUAL");
            cstBean.save(conn);
            DBConnection.commit(conn);
            log.error(ex);
        } catch (Exception ex) {
            log.error(ex);
            DBConnection.rollback(conn);            
        }		        		
	}
		
	protected Hashtable[] getSubHashTable(Hashtable ht, String str) {
		Hashtable[] htArr = null;
		Object obj = ht.get(str);
		if (obj != null && obj instanceof Hashtable[]) {
			htArr = (Hashtable[]) obj;
		}
		return htArr;
	}
    
    protected String getArrStringValue(Hashtable htArr[], String key) {
		String rtValue = "";
		if( htArr != null ) {
			for (int i = 0; i < htArr.length; i++) {
				Hashtable ht = htArr[i];
				Enumeration enumb = htArr[i].keys();
				String ht_key = enumb.nextElement().toString();
				if (key.equalsIgnoreCase(ht_key)) {
					rtValue = (String) ht.get(key);
					return rtValue;
				}
			}			
		}
		return rtValue;
	}
		
	private void saveCommunication()
	{
        CommunicationDB commDB = new CommunicationDB();
        commDB.saveCommunication(CCMSConst.SystemName.ILCP, this.msgName, this.tid, this.cstId,
                "IN", this.userId);
    }
	
	private void saveCstTrx(boolean isOnline) 
	{              
        CassetteTransactionDB cstTrxDB = new CassetteTransactionDB();

        CassetteCurrInfoDB cstDB = new CassetteCurrInfoDB();
        CassetteCurrInfoBean cstBean = null;

        try {
            cstBean = cstDB.selectCassetteById(conn, cstId);
            portId = cstBean.getCurrentLocation();
        } catch (Exception ex) {
            log.error(ex);
        }
        String result = CCMSConst.TRX_RESULT.FAIL;
        if (StringUtil.convertReturnCode(returnCode) == 0) {
            result = CCMSConst.TRX_RESULT.SUCCESS;
        }

        if (isOnline) {
            cstTrxDB.saveCstTrx(cstBean, userId, msgName, action, result, this.timeStamp);
        } else {
            cstTrxDB.saveOfflineCstTrx(cstBean, userId, msgName, action, result, this.timeStamp);
        }
    }
	
	

}