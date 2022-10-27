package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.entity.SquidFlowStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalcSquidFlowStatus {
	/**
	 * 统计squidflow的状态
	 * 
	 * @param statusList
	 * @return
	 */
	public static Map<String, Object> calcStatus(List<SquidFlowStatus> statusList) {
		int schedule = 0;// 调度
		int normal = 0;// 正常
		int checkout = 0;// 签出
		Map<String, Object> map = new HashMap<String, Object>();
		if (statusList!=null&&statusList.size()>0){
			for (SquidFlowStatus squidFlowStatus : statusList) {
				if (squidFlowStatus.getSquid_flow_status() == 0) {
					normal++;
				} else if (squidFlowStatus.getSquid_flow_status() == 1) {
					checkout++;
				} else if (squidFlowStatus.getSquid_flow_status() == 2) {
					schedule++;
				}
			}
		}
		map.put("schedule", schedule);
		map.put("normal", normal);
		map.put("checkout", checkout);
		return map;
	}
}
