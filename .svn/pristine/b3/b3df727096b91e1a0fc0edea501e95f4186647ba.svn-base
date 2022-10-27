package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.Project;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

public interface IProjectService{
/**
	 * 创建Project对象集合
	 * 作用描述：
	 * 1、 根据传入的Project对象集合调用ProjectPlug的创createProjects创建Project对象集合
	 * 2、创建成功——根据Project集合对象里的Key查询相对应的ID封装成InfoPacket对象集合返回
	 * 验证：
	 * Project对象集合不能为空且必须有数据
	 * Project对象集合的每个Key必须有值
	 * 修改说明：
	 *@param projects project对象集合
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> createProject(List<Project> projects,ReturnValue out);
	/**
	 * 修改Project
	 * 作用描述：
	 * 1、 根据ＩＤ修改Ｐｒｏｊｅｃｔ信息
	 * 2、创建成功——根据Project集合对象里的Key查询相对应的ID封装成InfoPacket对象集合返回
	 * 验证：
	 * 调用方法前确保Project对象集合不为空，并且Project对象集合里的每个Project对象的Id不为空
	 * 修改说明：
	 *@param projects project对象集合
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateProjects(List<Project> projects,ReturnValue out);
	/**
	 * 获得Project
	 * 作用描述：
	 * 根据repository对象的信息获得相应数据库下的所有Project
	 * 业务描述：
	 * 	1、根据Repository信息切换到指定的Repository物理数据库
	 * 	2、查询所有的Project信息
	 * 验证：
	 * repository对象不为空
	 * 修改说明：
	 *@param repository repository对象
	 *@return
	 */
	public Project[] queryProjects(Repository repository,ReturnValue out);
	
}