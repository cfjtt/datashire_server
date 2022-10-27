package util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 日期相关辅助类
 */
public class DateUtil {

	public static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	public static DateFormat dateFullTimeFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	public static DateFormat dateTimeFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm");

	public static DateFormat dateHourTimeFormat = new SimpleDateFormat(
			"yyyy-MM-dd HH");

	public static DateFormat simpleDotFormatter = new SimpleDateFormat(
			"yyyy.MM");

	/**
	 * 对给定的字符串，转换为日期类型
	 * 
	 * @param dateString
	 * @return Date (value/null)
	 */
	public static Date parseHM(String dateString) {
		Date date = null;
		try {
			date = dateTimeFormat.parse(dateString);
		} catch (ParseException e) {
		}
		return date;
	}
	

	/**
	 * 对给定的字符串，转换为日期类型
	 * 
	 * @param dateString
	 * @return Date (value/null)
	 */
	public static Date parseHMS(String dateString) {
		Date date = null;
		try {
			date = dateFullTimeFormat.parse(dateString);
		} catch (ParseException e) {
		}
		return date;
	}

	public static String getDateAddYearString(int years) {
		Date date = addYear(years);
		return format(date, "yyyy-MM-dd");
	}

	/**
	 * 计算两个时间相差多少年
	 * 
	 * @param nowDate
	 *            当前的时间（被减数）
	 * @param date
	 *            以前的时间（减数）
	 */
	public static int dateToDateYear(Date nowDate, Date date) {
		if (nowDate == null)
			return 0;
		if (date == null)
			return 0;
		long livetime = nowDate.getTime() - date.getTime();
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(livetime);
		@SuppressWarnings("unused")
		int liveyears = calendar.get(Calendar.YEAR) - 1970;
		return liveyears;
	}

	/**
	 * 在当前时间减多少年
	 */
	public static Date addYear(int years) {
		Date date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);

		calendar.add(Calendar.YEAR, 0 - years);
		return calendar.getTime();
	}

	/**
	 * <p>
	 * 根据{@code subType} 减去相应 {@code subNum }数
	 * </p>
	 * 
	 * @param date
	 *            时间
	 * @param subType
	 *            类型（年-yy、月-MM、星期-yy、天-dd）
	 * @param subNum
	 * @return
	 */
	public static Date subDate(Date date, int type, int subNum) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(type, subNum);
		return calendar.getTime();
	}

	/**
	 * 对给定的日期以模式串pattern格式化
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String format(Date date, String pattern) {
		if (date == null) {
			return "";
		}
		DateFormat df = new SimpleDateFormat(pattern);
		return df.format(date);
	}

	/**
	 * 对给定的日期以模式串pattern格式化
	 * 
	 * @param date
	 * @param pattern
	 * @return
	 */
	public static String format(Object obj, String pattern) {
		if (obj == null) {
			return "";
		}
		DateFormat df = new SimpleDateFormat(pattern);
		return df.format(obj);
	}

	/**
	 * 对给定的日期字符串以pattern格式解析
	 * 
	 * @param dateString
	 * @param pattern
	 * @return
	 */
	public static Date parse(String dateString, String pattern) {
		DateFormat df = new SimpleDateFormat(pattern);
		Date date = null;
		try {
			date = df.parse(dateString);
		} catch (Throwable t) {
			date = null;
		}
		return date;
	}

	/**
	 * 计算一天的起始时间和结束时间.
	 * 
	 * @param date
	 * @return
	 */
	public static Date[] getDayPeriod(Date date) {
		if (date == null) {
			return null;
		}
		Date[] dtary = new Date[2];
		dtary[0] = getDayMinTime(date);
		dtary[1] = getDayMaxTime(date);
		return dtary;
	}

	/**
	 * 获得指定日期的指定时、分、秒日期
	 * 
	 * @param date
	 * @param hours
	 * @param minutes
	 * @param seconds
	 * @return
	 */
	public static Date getSpecifiedTime(Date date, int hours, int minutes,
			int seconds) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.HOUR_OF_DAY, hours);
		c.set(Calendar.MINUTE, minutes);
		c.set(Calendar.SECOND, seconds);
		return c.getTime();
	}

	/**
	 * 获得指定日期的最小时间
	 * 
	 * @param date
	 * @return
	 */
	public static Date getDayMinTime(Date date) {
		return getSpecifiedTime(date, 0, 0, 0);
	}

	/**
	 * 获得指定日期的最大时间
	 * 
	 * @param date
	 * @return
	 */
	public static Date getDayMaxTime(Date date) {
		return getSpecifiedTime(date, 23, 59, 59);
	}

	public static Date[] getWeekPeriod(Date dt) {
		if (dt == null)
			return null;
		Date[] dtary = new Date[2];

		Calendar calendar = new GregorianCalendar();
		calendar.setFirstDayOfWeek(Calendar.MONDAY);
		calendar.setTime(dt);

		while (calendar.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
			calendar.add(Calendar.DATE, -1);
		}
		dtary[0] = getDayMinTime(calendar.getTime());

		calendar.add(Calendar.DAY_OF_WEEK, 6);
		dtary[1] = getDayMaxTime(calendar.getTime());
		return dtary;
	}

	@SuppressWarnings("deprecation")
	public static Date parseHSDate(String dtStr) {
		if (dtStr == null || dtStr.equals("")) {
			return new Date();
		}
		int year = Integer.parseInt(dtStr.substring(0, 4)) - 1900;
		int month = Integer.parseInt(dtStr.substring(4, 6)) - 1;
		int date = Integer.parseInt(dtStr.substring(6));
		return new Date(year, month, date);
	}

	@SuppressWarnings("deprecation")
	public static Date[] getMonthPeriod(Date dt) {
		int[] days = { 30, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

		if (dt == null)
			return null;
		Date[] dtary = new Date[2];
		dtary[0] = new Date(dt.getYear(), dt.getMonth(), 1, 0, 0, 0);
		dtary[1] = new Date(dt.getYear(), dt.getMonth(), days[dt.getMonth()],
				23, 59, 59);
		if (dt.getMonth() == 1 && dt.getYear() % 4 == 0)
			dtary[1].setDate(29);

		return dtary;
	}

	/**
	 * 判断两个日期是否同一天
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	public static boolean isSameDay(Date first, Date second) {
		Date range[] = getDayPeriod(first);
		return second.after(range[0]) && second.before(range[1]);
	}

	/**
	 * 判断两个日期是否同一周
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	public static boolean isSameWeek(Date first, Date second) {
		Date range[] = getWeekPeriod(first);
		return (compare(second, range[0]) >= 0 && compare(second, range[1]) <= 0);
	}

	/**
	 * 判断两个日期是否同一月
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static boolean isSameMonth(Date first, Date second) {
		return first.getYear() == second.getYear()
				&& first.getMonth() == second.getMonth();
	}

	/**
	 * 判断两个日期是否同一季度
	 * 
	 * @param first
	 * @param second
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static boolean isSameQuarter(Date first, Date second) {
		if (first.getYear() != second.getYear())
			return false;
		else {
			if (first.getMonth() <= 2 && second.getMonth() <= 2)
				return true;
			else if (first.getMonth() <= 5 && second.getMonth() <= 5
					&& first.getMonth() > 2 && second.getMonth() > 2)
				return true;
			else if (first.getMonth() <= 8 && second.getMonth() <= 8
					&& first.getMonth() > 5 && second.getMonth() > 5)
				return true;
			else if (first.getMonth() <= 11 && second.getMonth() <= 11
					&& first.getMonth() > 8 && second.getMonth() > 8)
				return true;
			else
				return false;
		}
	}

	/**
	 * 取得这个周的星期五（最后交易日日期）
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Date getLastDayOfWeek(Date date) {
		return new Date(date.getYear(), date.getMonth(), date.getDate()
				+ (5 - date.getDay()));
	}

	/**
	 * 取得这个月的最后一天
	 * 
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Date getLastDayOfMonth(Date date) {
		return new Date(date.getYear(), date.getMonth() + 1, 0);
	}

	/**
	 * 取得这个季度的第一天
	 * 
	 * @param date
	 * @return 季度的第一天
	 * @deprecated replaced by <code>getFirstDayOfQuarter</code>
	 */
	@Deprecated
	public static Date getFirstDayOfQuote(Date date) {
		return new Date(date.getYear(), date.getMonth() / 3 * 3, 1);
	}

	/**
	 * 取得这个季度的第一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getFirstDayOfQuarter(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.MONTH, c.get(Calendar.MONTH) / 3 * 3);
		c.set(Calendar.DAY_OF_MONTH, 1);
		return c.getTime();
	}

	/**
	 * 取得这个季度的最后一天
	 * 
	 * @param date
	 * @return 季度的最后一天
	 * @deprecated replaced by <code>getLastDayOfQuarter</code>
	 */
	@Deprecated
	public static Date getLastDayOfQuote(Date date) {
		return new Date(date.getYear(), (date.getMonth() / 3 + 1) * 3, 0);
	}

	/**
	 * 取得这个季度的最后一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getLastDayOfQuarter(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.MONTH, (c.get(Calendar.MONTH) / 3 + 1) * 3);
		c.set(Calendar.DAY_OF_MONTH, 0);
		return c.getTime();
	}

	/**
	 * 取得这个年度的第一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getFirstDayOfYear(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DAY_OF_MONTH, 1);
		return c.getTime();
	}

	/**
	 * 取得这个年度的最后一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getLastDayOfYear(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.set(Calendar.MONTH, Calendar.DECEMBER);
		c.set(Calendar.DAY_OF_MONTH, 31);
		return c.getTime();
	}
	@SuppressWarnings("deprecation")
	public static Date parseDate(String dateStr) {
		if (dateStr != null && !dateStr.trim().equals("")) {
			try {
				String part[] = dateStr.split("-");
				if (part.length != 3)
					return null;

				int year = Integer.parseInt(part[0]) - 1900;
				int month = Integer.parseInt(part[1]) - 1;
				int day = Integer.parseInt(part[2]);
				if (year < 0 || year > 8000 || month < 0 || month > 11
						|| day < 0 || day > 31)
					return null;

				return new Date(year, month, day);
			} catch (Exception e) {
				// TODO: handle exception
				return null;
			}

		} else {
			return null;
		}
	}

	@SuppressWarnings("deprecation")
	public static int compare(Date first, Date second) {
		if (first.getYear() > second.getYear())
			return 1;
		else if (first.getYear() < second.getYear())
			return -1;

		if (first.getMonth() > second.getMonth())
			return 1;
		else if (first.getMonth() < second.getMonth())
			return -1;

		if (first.getDate() > second.getDate())
			return 1;
		else if (first.getDate() < second.getDate())
			return -1;
		else
			return 0;
	}

	/**
	 * 获得以给定日期为基准的绝对日期（时间区间的数学运算）
	 * 
	 * @param now
	 * @param field
	 * @param amount
	 * @return
	 */
	public static Date getAbsoluteDate(Date now, int field, int amount) {
		Calendar c = Calendar.getInstance();
		c.setTime(now);
		c.add(field, amount);
		return c.getTime();
	}

	/**
	 * 获得以给定日期为基准的绝对日期（指定某一时间区间值）
	 * 
	 * @param now
	 * @param field
	 * @param amount
	 * @return
	 */
	public static Date setAbsoluteDate(Date now, int field, int amount) {
		Calendar c = Calendar.getInstance();
		c.setTime(now);
		c.set(field, amount);
		return c.getTime();
	}

	/**
	 * <p>
	 * 得到一天的开始时间
	 * </p>
	 * 
	 * @param date
	 * @return
	 */
	public static Date getDayStartDate(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
				calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
		return calendar.getTime();
	}

	/**
	 * <p>
	 * 得到一天的结束时间
	 * </p>
	 * 
	 * @param date
	 * @return
	 */
	public static Date getDayEndDate(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
				calendar.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
		return calendar.getTime();
	}

	/** 获取当前年份 */
	public static Integer getThisYear() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		return calendar.get(Calendar.YEAR);
	}

	/** 获取当前月份 */
	public static Integer getThisMonth() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		return calendar.get(Calendar.MONTH);
	}

	/**
	 * @param item
	 *            计算的栏目 只能是 day,month或year
	 * @param value
	 *            计算值
	 * @param beginTime
	 *            基准时间 为空的话默认为当前时间
	 * @return
	 */
	public static Date dateAdd(String item, int value, Date beginTime) {
		// TODO Auto-generated method stub
		if (beginTime == null)
			beginTime = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(beginTime);
		if (item.equals("day")) {
			int day = calendar.get(Calendar.DAY_OF_YEAR);
			calendar.set(Calendar.DAY_OF_YEAR, day + value);
			return calendar.getTime();
		} else if (item.equals("month")) {
			int month = calendar.get(Calendar.MONTH);
			calendar.set(Calendar.MONTH, month + value);
			return calendar.getTime();
		} else if (item.equals("year")) {
			int year = calendar.get(Calendar.YEAR);
			calendar.set(Calendar.YEAR, year + value);
			return calendar.getTime();
		}
		return null;
	}

	/**
	 * <p>
	 * 按51法则计算参数日期最近的一个年报发布日期返回的是一个 日期
	 * </p>
	 * 
	 * @param date
	 *            基准时间 为空的话默认为当前时间
	 * @return
	 */
	public static Date getAnnalsDate(Date date) {
		// TODO Auto-generated method stub
		if (date == null)
			date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH);
		if (month + 1 < 5) {
			calendar.set(year - 2, 12 - 1, 31);
		} else if (month + 1 >= 5) {
			calendar.set(year - 1, 12 - 1, 31);
		}
		return calendar.getTime();
	}

	// 获得年份
	public static int getYear(Date date) {
		// TODO Auto-generated method stub
		if (date == null)
			date = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.YEAR);
	}

	/**
	 * <p>
	 * 计算两个时间之间相差天数
	 * </p>
	 * 
	 * @param1 date 开始时间
	 * @param2 date 结束时间
	 * @return Integer
	 */
	public static Integer dateDiffByDay(Date beginDate, Date endDate) {
		Long day = (endDate.getTime() - beginDate.getTime())
				/ (1000 * 60 * 60 * 24);
		return day.intValue();
	}

	/**
	 * 获得以当前时间为基准的6个整月区间
	 * 
	 * @return List
	 */
	public static List getSixMonth() {
		Calendar calendar = Calendar.getInstance();
		List<Map> list = new ArrayList<Map>();
		for (int i = 0; i < 6; i++) {
			Map<String, Integer> map = new HashMap<String, Integer>();
			int month = calendar.get(Calendar.MONTH);
			calendar.set(Calendar.MONTH, month - 1);
			map.put("year", calendar.get(Calendar.YEAR));
			map.put("month", (calendar.get(Calendar.MONTH) + 1));
			map.put("lastyear", (calendar.get(Calendar.YEAR) - 1));
			map.put("lastmonth", (calendar.get(Calendar.MONTH) + 1));
			list.add(map);
		}
		return list;

	}

	public static Date getDate(int timeSlotType, Date dt) {
		Date createtime = null;
		String str = "";
		if (timeSlotType == Calendar.HOUR) {
			str = DateUtil.format(dt, "yyyy-MM-dd HH");
			createtime = DateUtil.parse(str, "yyyy-MM-dd HH");
		} else if (timeSlotType == Calendar.MINUTE) {
			str = DateUtil.format(dt, "yyyy-MM-dd HH:mm");
			createtime = DateUtil.parse(str, "yyyy-MM-dd HH:mm");
		} else if (timeSlotType == Calendar.SECOND) {
			str = DateUtil.format(dt, "yyyy-MM-dd HH:mm:ss");
			createtime = DateUtil.parse(str, "yyyy-MM-dd HH:mm:ss");
		}
		return createtime;
	}

	public static Date getDate(int timeSlotType, Date dt, int addtime) {
		Date createtime = null;
		String str = "";
		if (timeSlotType == Calendar.HOUR) {
			str = DateUtil.format(dt, "yyyy-MM-dd HH");
			createtime = DateUtil.parse(str, "yyyy-MM-dd HH");
			createtime = DateUtil.subDate(createtime, timeSlotType, addtime);
		} else if (timeSlotType == Calendar.MINUTE) {
			str = DateUtil.format(dt, "yyyy-MM-dd HH:mm");
			createtime = DateUtil.parse(str, "yyyy-MM-dd HH:mm");
			createtime = DateUtil.subDate(createtime, timeSlotType, addtime);
		} else if (timeSlotType == Calendar.SECOND) {
			str = DateUtil.format(dt, "yyyy-MM-dd HH:mm:ss");
			createtime = DateUtil.parse(str, "yyyy-MM-dd HH:mm:ss");
			createtime = DateUtil.subDate(createtime, timeSlotType, addtime);
		}
		return createtime;
	}
	/**
	 * 时间转换为字符串
	 * @author Akachi
	 * 2014-2-26
	 * @param cday 时间
	 * @param parrten 格式化字符串
	 * @return
	 */
	public static String dateToStr(Date cday, String parrten) {
		if(cday==null)
			return "";
		String timestr;
		if (parrten == null || parrten.equals("")) {
			parrten = "yyyy-MM-dd";
		}
		java.text.SimpleDateFormat sdf = new SimpleDateFormat(parrten);
		timestr = sdf.format(cday);
		return timestr;
	}
}