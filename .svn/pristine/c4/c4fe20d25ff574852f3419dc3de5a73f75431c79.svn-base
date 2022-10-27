package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SpecialSquidJoin;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 *  对join操作的业务处理类，在datashireService中调用此方法
 *  继承SupportPlug获取adapter连接池对象
 * @version 1.0
 * @author lei.bin
 * @created 2013-10-21
 */
public interface IJoinProcess{
/**
	 * 根据前端传送的信息创建一个SquidJoin对象，并且返回结果
	 * @param info 输入信息
	 * @param out 返回结果代码
	 * @return List<SpecialSquidJoin>
	 * @author lei.bin
	 */
	public List<SpecialSquidJoin> createSquidJoin(String info, ReturnValue out);
	/**
	 *  根据前端传送的信息更新SquidJoin对象
	 * @param info 输入信息
	 * @param out 返回结果代码
	 * @return List<SpecialSquidJoin>
	 * @author lei.bin
	 */
	public List<SpecialSquidJoin> updateSquidJoin(String info, ReturnValue out);
	/**
	 * 对squidjoin进行 新增或者更新操作时获取StageSquid返回对象
	 * @param join 
	 * @param out 
	 * @return StageSquid
	 * @author lei.bin
	 */
	public StageSquid getStageSquidData(SquidJoin join, ReturnValue out);
	
}