package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IRepositoryDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.RepositoryDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.enc.KeyGen;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.service.UserService;
import com.eurlanda.datashire.server.utils.Constants;
import com.eurlanda.datashire.server.utils.SysUtil;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.DateTimeUtils;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.VersionUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserNewProcess {
	static Logger logger = Logger.getLogger(UserNewProcess.class);// 记录日志
	private String token;

	public UserNewProcess() {

	}

	public UserNewProcess(String token) {
		this.token = token;
	}

	/**
	 * 登陆
	 * @param info
	 * @param out
	 * @return
	 * @throws SQLException 

	public Map<String, Object> login(String info, ReturnValue out) throws SQLException
	{
		Map<String, Object> outputMap = new HashMap<String, Object>();
		List<Repository> repositories = null;
		IRelationalDataManager adapter=DataAdapterFactory.getDefaultDataManager();
		try {
			// 判断license
			// 查询出所有的repository
			adapter.openSession();
			IRepositoryDao iRepositoryDao=new RepositoryDaoImpl(adapter);
			String key = iRepositoryDao.getLicenseKey();
			//String key = "";
			if (StringUtils.isNotNull(key)){
				int squidCnt = SysUtil.decodeLicenseKey(key);
				CommonConsts.MaxSquidCount = squidCnt;
			} else {
				// 判断许可是否过期
				long time = DateTimeUtils.timeDiff(CommonConsts.LimitedTime,
						DateTimeUtils.currentDate(), "yyyy-MM-dd");
				if (time <= 0) {
					out.setMessageCode(MessageCode.ERR_LICENSE);
					outputMap.put("SquidCount", CommonConsts.MaxSquidCount);
					return outputMap;
				} else {
					int days = Integer.parseInt((time)/(1000*60*60*24)+"");
					System.out.println(days);
					outputMap.put("TryDays", days);
					out.setMessageCode(MessageCode.WARN_LICENSE);
				}
			}
			outputMap.put("SquidCount", CommonConsts.MaxSquidCount);
			User user = JsonUtil.object2HashMap(info, User.class);
			if(user!=null && user.getUser_name()!=null && user.getPassword()!=null){
				user.setUser_name(user.getUser_name().trim()); // 用户名可以有前后空格
//				boolean isSuperUser =  CommonConsts.SuperUserName.equalsIgnoreCase(user.getUser_name())
//						&& CommonConsts.SuperUserPwd.equals(user.getPassword());

				ILoginDao iLoginDao=new LoginDaoImpl(adapter);
				user =iLoginDao.getUser(user.getUser_name(), user.getPassword());
				if(null==user)
				{
					out.setMessageCode(MessageCode.ERR_USER_NOT_EXIST);//用户不存在
					logger.error(">>>>>>>>>>>>>>>>>>登陆的用户信息不存在<<<<<<<<<<<<<<");
					return null;
				}

				repositories = iRepositoryDao.getAllRepository();
				if (null == repositories) {
					out.setMessageCode(MessageCode.ERR_REPOSITORYLIST);
				} else if (null != repositories && repositories.size() == 0) {
					out.setMessageCode(MessageCode.ERR_REPOSITORYEMPTY);
				} else {
					outputMap.put("RepositoryList", repositories);
				}
				outputMap.put("User", user);
				logger.info(">>>>>>>>>>>>>>>>>>用户"+user.getUser_name()+"正在登陆中<<<<<<<<<<<<<<");
			}else
			{
				out.setMessageCode(MessageCode.ERR_USER);//用户不存在
				logger.error(">>>>>>>>>>>>>>>>>>用户"+user.getUser_name()+"信息错误<<<<<<<<<<<<<<");
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_USER_NOT_EXIST);
			logger.error("用户登录异常", e);
		}finally {
			if(adapter!=null) {
				adapter.closeSession();
			}
		}
		return outputMap;
	}  */
	/**
	 * 删除用户
	 * @param info
	 * @param out
	 */
	public void deleteUser(String info, ReturnValue out) {

        Map<String, Object> map = JsonUtil.toHashMap(info);
        int userid = Integer.parseInt(map.get("UserId").toString());
        UserService userService = Constants.context.getBean(UserService.class);
//        userService.deleteById(userid);
        int execult = userService.deleteById(userid);
        if (execult <= 0) {
            out.setMessageCode(MessageCode.ERR_DELETEUSER);
        }
//
//		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
//		try {
//			Map<String, Object> map = JsonUtil.toHashMap(info);
//			int userid=Integer.parseInt(map.get("UserId").toString());
//			// 实例化相关的数据库处理类
//			adapter.openSession();
//			IUserDao userDao=new UserDaoImpl(adapter);
//			int execult = userDao.delete(userid,User.class);
//			if (execult<=0) {
//				out.setMessageCode(MessageCode.ERR_DELETEUSER);
//			}
//		} catch (Exception e) {
//			// TODO: handle exception
//			logger.error("deleteUser is error", e);
//			out.setMessageCode(MessageCode.ERR_DELETEUSER);
//			try {
//				adapter.rollback();
//			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
//				logger.fatal("rollback err!", e1);
//			}
//		} finally {
//			adapter.closeSession();
//		}
	
	}
	/**
	 * 新增用户
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> createUser(String info, ReturnValue out) {


        User user = JsonUtil.object2HashMap(info, User.class);

		Map<String, Object> outputMap = new HashMap<String, Object>();

        UserService userService = Constants.context.getBean(UserService.class);
        int count = userService.save(user);
        if(count > 0) {
            outputMap.put("Id", user.getId());
        } else {
            out.setMessageCode(MessageCode.ERR_CREATEUSER);
        }
//		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
//		try {
//
//			adapter.openSession();
//			IUserDao userDao=new UserDaoImpl(adapter);
//			int execult = userDao.insert2(user);
//			if (execult < 0) {
//				out.setMessageCode(MessageCode.ERR_CREATEUSER);
//			} else {
//				outputMap.put("Id", execult);
//			}
//		} catch (Exception e) {
//			// TODO: handle exception
//			logger.error("createUser is error", e);
//			out.setMessageCode(MessageCode.ERR_CREATEUSER);
//			try {
//				adapter.rollback();
//			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
//				logger.fatal("rollback err!", e1);
//			}
//		} finally {
//			adapter.closeSession();
//		}
		return outputMap;
	}

	private void copyBean(Object source,Object target,Class clazz){
		if(clazz.getSuperclass()!=null){
			copyBean(source,target,clazz.getSuperclass());
		}
		for (Field field:clazz.getDeclaredFields()){
			field.setAccessible(true);
			try {
				Object o = field.get(source);
				Boolean isBool = Modifier.isFinal(field.getModifiers());
				Boolean isStatic = Modifier.isStatic(field.getModifiers());
				if(o !=null&&!"".equals(o)&&!isBool&&!isStatic) {
					field.set(target, o);
				}
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 更新用户
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object>  updateUserNew(String info, ReturnValue out) {

        Map<String, Object> outputMap = new HashMap<String, Object>();
        User newUser = JsonUtil.object2HashMap(info, User.class);
        Map<String, Object> map = JsonUtil.toHashMap(info);
        String oldPassword=map.get("CurrentPassword").toString();

        UserService userService = Constants.context.getBean(UserService.class);
        User oldUser = userService.findByUsernameAndPassword(newUser.getUser_name(), oldPassword);
        if(oldUser != null) {
            newUser.setId(oldUser.getId());
            int count = userService.update(newUser);
            if(count > 0) {
                outputMap.put("UpdatedUser",newUser);
                out.setMessageCode(MessageCode.SUCCESS);
            } else {
                out.setMessageCode(MessageCode.ERR_UPDATEUSER);
            }
        } else {
            out.setMessageCode(MessageCode.ERR_UPDATEUSER);
        }

        return outputMap;


//
//		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
//		try {
//			adapter.openSession();
//
//			IUserDao userDao=new UserDaoImpl(adapter);
//			User dataBaseUser = userDao.getUserByName(newUser.getUser_name());
//			if(dataBaseUser!=null&&dataBaseUser.getPassword().equals(oldPassword)) {
//				newUser.setId(dataBaseUser.getId());
//				copyBean(newUser,dataBaseUser,User.class);
//				boolean execult = userDao.update(dataBaseUser);
//				out.setMessageCode(MessageCode.SUCCESS);
//				if (!execult) {
//					out.setMessageCode(MessageCode.ERR_UPDATEUSER);
//				}else{
//					out.setMessageCode(MessageCode.SUCCESS);
//				}
//			}else{
//				out.setMessageCode(MessageCode.ERR_UPDATEUSER);
//			}
//			outputMap.put("UpdatedUser",dataBaseUser);
//			return outputMap;
//		} catch (Exception e) {
//			logger.error("updateUser is error", e);
//			out.setMessageCode(MessageCode.ERR_UPDATEUSER);
//			try {
//				adapter.rollback();
//			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
//				logger.fatal("rollback err!", e1);
//			}
//		} finally {
//			adapter.closeSession();
//		}
//		return null;
	}

	/**
	 * 修改用户密码
	 * @param info
	 * @param out
	 * @retutn
	 */
	public void updateUserPassword(String info,ReturnValue out){

        UserService userService = Constants.context.getBean(UserService.class);
        Map<String,Object> map=JsonUtil.toHashMap(info);
        Map<String,Object> paramMap=new HashMap<String,Object>();
        if(map.get("UserId")==null || map.get("NewPassword")==null){
            out.setMessageCode(MessageCode.ERR_USERNAME_OR_PASSWORD);
        } else {
            // 应该把老的密码带过来的
            User user = new User();
            user.setId(Integer.parseInt(map.get("UserId")+""));
            user.setPassword(map.get("NewPassword").toString());
            int count = userService.update(user);
            if(count > 0) {
                out.setMessageCode(MessageCode.SUCCESS);
            } else {
                out.setMessageCode(MessageCode.ERR_USERNAME_OR_PASSWORD);
            }
        }

//		IRelationalDataManager adapter =DataAdapterFactory.getDefaultDataManager();
//		try{
//
//			if(map.get("UserId")==null || map.get("NewPassword")==null){
//				out.setMessageCode(MessageCode.ERR_USERNAME_OR_PASSWORD);
//			} else {
//				//验证当前用户是否存在
//				paramMap.put("id",Integer.parseInt(map.get("UserId")+""));
//				paramMap.put("password",map.get("NewPassword")+"");
//				paramMap.put("tableName",User.class);
//				map.put("last_logon_date", "SYSDATE()");
//			}
//			adapter.openSession();
//			boolean execult=adapter.updateSomeColumn(paramMap);
//			if(!execult){
//				out.setMessageCode(MessageCode.ERR_USERNAME_OR_PASSWORD);
//			}  else {
//				out.setMessageCode(MessageCode.SUCCESS);
//			}
//		}catch(Exception e){
//			logger.error("updateUserPassWord is error", e);
//			out.setMessageCode(MessageCode.ERR_USERNAME_OR_PASSWORD);
//			try {
//				adapter.rollback();
//			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
//				logger.fatal("rollback err!", e1);
//			}
//		} finally {
//			adapter.closeSession();
//		}
	}
	/**
	 * 更新用户(此时的更新用户但不更新密码的)
	 * @param info
	 * @param out
	 * @return
	 */
	public void updateUser(String info, ReturnValue out) {
        UserService userService = Constants.context.getBean(UserService.class);
        User user = JsonUtil.object2HashMap(info, User.class);
        user.setPassword(null);
        int cnt = userService.update(user);
        if(cnt > 0) {
            out.setMessageCode(MessageCode.SUCCESS);
        } else {
            out.setMessageCode(MessageCode.ERR_UPDATEUSER);
        }


//		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
//		try {
//			User user = JsonUtil.object2HashMap(info, User.class);
//			Map<String,Object> map=new HashMap<String,Object>();
//			//添加需要更新的数据库字符和值，字段需要和数据库字段相对应
//			map.put("id",user.getId());
//			map.put("user_name",user.getUser_name().replaceAll("\\\\","\\\\\\\\"));
//			map.put("full_name",user.getFull_name().replaceAll("\\\\","\\\\\\\\"));
//			map.put("email_address",user.getEmail_address());
//			map.put("last_logon_date", "SYSDATE()");
//			map.put("tableName",User.class);
//			adapter.openSession();
//			//IUserDao userDao=new UserDaoImpl(adapter);
//			boolean execult = adapter.updateSomeColumn(map);
//			if (!execult) {
//				out.setMessageCode(MessageCode.ERR_UPDATEUSER);
//			}
//		} catch (Exception e) {
//			logger.error("updateUser is error", e);
//			out.setMessageCode(MessageCode.ERR_UPDATEUSER);
//			try {
//				adapter.rollback();
//			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
//				logger.fatal("rollback err!", e1);
//			}
//		} finally {
//			adapter.closeSession();
//		}
	}
	/**
	 * 新增用户
	 * @param out
	 * @return
	 */
	public Map<String, Object> getAllUser(ReturnValue out) {
		Map<String, Object> outputMap = new HashMap<String, Object>();
        UserService userService = Constants.context.getBean(UserService.class);
        List<User> users = userService.getAllUser();
        outputMap.put("Users", users);
        return outputMap;

//        IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
//		List<User> users = null;
//		try {
//			adapter.openSession();
//			IUserDao userDao=new UserDaoImpl(adapter);
//			users=userDao.getAllUser();
//			outputMap.put("Users", users);
//		} catch (Exception e) {
//			// TODO: handle exception
//			logger.error("getAllUser is error", e);
//			out.setMessageCode(MessageCode.ERR_GETALLUSER);
//			try {
//				adapter.rollback();
//			} catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
//				logger.fatal("rollback err!", e1);
//			}
//		} finally {
//			adapter.closeSession();
//		}
//		return outputMap;
	}
	
	/**
	 * 获取license的key值
	 * @param out
	 * @return
	 */
	public Map<String, Object> getLicenseKey(ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		try {
			String temp = "";
			String version = VersionUtils.getServerVersion();
			//String macAddress = MacAddressUtils.getMacAddress();
			//temp = macAddress + "+" + version + "+" + "eurlanda";
			//temp = DesUtils.encryptBasedDes(temp);
			System.out.println(temp);
			byte[] ss = KeyGen.gen(version.getBytes());
			String rt = new String(ss);
			System.out.println(rt);
			if (StringUtils.isNotNull(rt)){
				outputMap.put("LicenseKey", rt);
			}else{
				out.setMessageCode(MessageCode.ERR_LICENSE_NOT_EXISTS);
			}
		} catch (Exception e) {
			// TODO: handle exception
			logger.error("getLicenseKey is error", e);
			out.setMessageCode(MessageCode.ERR_LICENSE_NOT_EXISTS);
		}
		return outputMap;
	}
	
	/**
	 * 设置license
	 * @param out
	 * @return
	 */
	public Map<String, Object> setLicense(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
			String key = String.valueOf(paramsMap.get("LicenseKey"));
			int squidCnt = SysUtil.decodeLicenseKey(key);
			// -99 代表无限制squid 数量
			if (squidCnt>5 || squidCnt == -99){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IRepositoryDao rDao = new RepositoryDaoImpl(adapter);
				rDao.saveLicenseKey(key);
				CommonConsts.MaxSquidCount = squidCnt;
				outputMap.put("SquidCount", squidCnt);
			}else{
				out.setMessageCode(MessageCode.ERR_LICENSE_NOT_EXISTS);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			// TODO: handle exception
			logger.error("setLicense is error", e);
			out.setMessageCode(MessageCode.ERR_LICENSE_NOT_EXISTS);
		} finally{
			if (adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * 获取当前squid的总数量
	 * @param out
	 * @return
	 */
	public Map<String, Object> getSquidAllForCount(ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			ISquidDao squidDao = new SquidDaoImpl(adapter);
			List<Squid> list = squidDao.getObjectAll(Squid.class);
			outputMap.put("SquidCount", list!=null?list.size():0);
			return outputMap;
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			// TODO: handle exception
			logger.error("setLicense is error", e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		} finally{
			if (adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	public static void main(String[] args) {
		//UserNewProcess process = new UserNewProcess();
		/*Map<String, Object> map = process.getLicenseKey(new ReturnValue());
		System.out.println(map);*/
		
		long t = DateTimeUtils.timeDiff(CommonConsts.LimitedTime,
				DateTimeUtils.currentDate(), "yyyy-MM-dd");
		System.out.println((t)/(1000*60*60*24));
		int days = Integer.parseInt((t)/(1000*60*60*24)+"");
		System.out.println(days);
		/*String info = "{\"LicenseKey\":\"VU3tslwM0C6FVqqsI2XZBRGRzTmZLr80fgeCwSfCVesCJOt7crsguSOe6k6sZmADTowUaWJ5H8XnTPqwHSDdjA==\"}";
		Map<String, Object> map = process.setLicense(info, new ReturnValue());
		System.out.println(map);*/
		/*System.out.println(OsUtils.getMacs().get(0).getMac());*/
	}
}

