package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ReferenceColumnGroup相关业务处理类
 * 
 * @author lei.bin
 * 
 */
public class ReferenceColumnGroupProcess extends AbstractRepositoryService implements IReferenceColumnGroupProcess{
	// 记录日志
	static Logger logger = Logger.getLogger(ReferenceColumnGroupProcess.class);

	public ReferenceColumnGroupProcess(String token) {
		super(token);
	}

	public ReferenceColumnGroupProcess(IRelationalDataManager adapter) {
		super(adapter);
	}

	public ReferenceColumnGroupProcess(String token,
			IRelationalDataManager adapter) {
		super(token, adapter);
	}

	/**
	 * ReferenceColumnGroup更新 
	 * 根据前端传送的数据进行更新,将更新后的结果封装好返回给前端
	 * @return
	 */
	public List<InfoPacket> updateReferenceColumnGroup(String info,
			ReturnValue out) {
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		int update = 0;
		try {
			List<ReferenceColumn> columns = JsonUtil.toGsonList(info,
					ReferenceColumn.class);
			adapter.openSession();
			for (ReferenceColumn columnGroup : columns) {
				int groupId=columnGroup.getGroup_id();
				int relative_order=columnGroup.getGroup_order();
				//对columnGroup进行更新
				update = adapter.execute("update DS_REFERENCE_COLUMN_GROUP set relative_order="+relative_order+" where id="+groupId+"" );
				if (update>=0) {
					InfoPacket packet = new InfoPacket();
					packet.setCode(1);
					packet.setId(columnGroup.getId());
					packet.setKey(columnGroup.getKey());
					packet.setType(DSObjectType.COLUMNGROUP);
					infoPackets.add(packet);
				} else {
					//更新失败，终止循环
					out.setMessageCode(MessageCode.ERR_UPDATE_REFERENCECOLUMNGROUP);
					break;
				}
			}
		} catch (Exception e) {
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("[updateReferenceColumnGroup rollback is error]"
						+ e1);
			}
			logger.error("[updateReferenceColumnGroup is error]" + e);
		} finally {
			adapter.closeSession();
		}

		return infoPackets;
	}
}
