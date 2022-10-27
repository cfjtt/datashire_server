package com.eurlanda.datashire.enumeration;

import com.eurlanda.datashire.utility.EnumException;

import java.util.HashMap;
import java.util.Map;

public enum DateFormatEnum {
	//mm/dd/yy
    TYPE1(0, "mm/dd/yy", "MM/dd/yy"),
    //mm/dd/yyyy
    TYPE2(1, "mm/dd/yyyy", "MM/dd/yyyy"),
    //yy.mm.dd
    TYPE3(2, "yy.mm.dd", "yy.MM.dd"),
    //yyyy.mm.dd
    TYPE4(3, "yyyy.mm.dd", "yyyy.MM.dd"),
    //dd/mm/yy
    TYPE5(4, "dd/mm/yy", "dd/MM/yy"),
    //dd/mm/yyyy
    TYPE6(5, "dd/mm/yyyy", "dd/MM/yyyy"),
    //dd.mm.yy
    TYPE7(6, "dd.mm.yy", "dd.MM.yy"),
    //dd.mm.yyyy
    TYPE8(7, "dd.mm.yyyy", "dd.MM.yyyy"),
    //dd-mm-yy
    TYPE9(8, "dd-mm-yy", "dd-MM-yy"),
    //dd-mm-yyyy
    TYPE10(9, "dd-mm-yyyy", "dd-MM-yyyy"),
    //dd mon yy
    TYPE11(10, "dd mon yy", "dd MM yyyy"),
    //dd mon yyyy
    TYPE12(11, "dd mon yyyy", "dd MM yyyy"),
    //Mon dd), yy
    TYPE13(12, "Mon dd), yy", "MMM dd), yy"),
    //Mon dd), yyyy
    TYPE14(13, "Mon dd), yyyy", "MMM dd), yyyy"),
    //hh:mi:ss
    TYPE15(14, "hh:mi:ss", "HH:mm:ss"),
    //mon dd yyyy hh:mi:ss:mmmAM (or PM)
    TYPE16(15, "mon dd yyyy hh:mi:ss", "MMM dd yyyy HH:mm:ss"),
    //mm-dd-yy
    TYPE17(16, "mm-dd-yy", "MM-dd-yy"),
    //mm-dd-yyyy
    TYPE18(17, "mm-dd-yyyy", "MM-dd-yyyy"),
    //yy/mm/dd
    TYPE19(18, "yy-mm-dd", "yy-MM-dd"),
    //yyyy/mm/dd
    TYPE20(19, "yyyy-mm-dd", "yyyy-MM-dd"),
    //yymmdd
    TYPE21(20, "yymmdd", "yyMMdd"),
    //yyyymmdd
    TYPE22(21, "yyyymmdd", "yyyyMMdd"),
    //dd mon yyyy hh:mi:ss:mmm(24h)
    TYPE23(22, "dd mon yyyy hh:mi:ss:mmm", "dd MMM yyyy HH:mm:ss:SSS"),
    //hh:mi:ss:mmm(24h)
    TYPE24(23, "hh:mi:ss:mmm", "HH:mm:ss:SSS"),
    //yyyy-mm-dd hh:mi:ss(24h)
    TYPE25(24, "yyyy-mm-dd hh:mi:ss", "yyyy-MM-dd HH:mm:ss"),
    //yyyy-mm-dd hh:mi:ss.mmm(24h)
    TYPE26(25, "yyyy-mm-dd hh:mi:ss.mmm", "yyyy-MM-dd HH:mm:ss.SSS"),
    //yyyy-mm-ddThh:mi:ss.mmm
    TYPE27(26, "yyyy-mm-ddThh:mi:ss.mmm", "yyyy-MM-ddTHH:mm:ss.SSS"),
    //yyyy-mm-ddThh:mi:ss.mmmZ
    TYPE28(27, "yyyy-mm-ddThh:mi:ss.mmmZ", "yyyy-MM-ddTHH:mm:ss.SSSZ"),
    //dd mon yyyy hh:mi:ss:mmmAM
    TYPE29(28, "dd mon yyyy hh:mi:ss:mmm", "dd MMM yyyy HH:mm:ss:SSS"),
    //dd/mm/yyyy hh:mi:ss:mmmAM
    TYPE30(29, "dd/mm/yyyy hh:mi:ss:mmm", "dd/MM/yyyy HH:mm:ss:SSS");
    
    private int _key;
    private String _show;
    private String _value;
	
	private static Map<Integer, DateFormatEnum> map;

	/**
	 * 构造方法
	 * 
	 * @param key
	 * @param show
	 * @param value
	 */
	private DateFormatEnum(int key,String show, String value) {
		_key = key;
		_show = show;
		_value = value;
	}

	/**
	 * 得到枚举Key
	 * 
	 * @return
	 */
	public int key() {
		return _key;
	}
	
	/**
	 * 得到客户端显示的值
	 * 
	 * @return
	 */
	public String show(){
		return _show;
	}
	
	/**
	 * 得到数据处理的值
	 * 
	 * @return
	 */
	public String value(){
		return _value;
	}
	
	/**
	 * 从int到enum的转换函数
	 * 
	 * @param value
	 * @return
	 * @throws Exception
	 */
	public static DateFormatEnum valueOf(int value) throws EnumException {
		DateFormatEnum type = null;
		if (map == null) {
			map = new HashMap<Integer, DateFormatEnum>();
			DateFormatEnum[] types = DateFormatEnum.values();
			for (DateFormatEnum tmp : types) {
				map.put(tmp.key(), tmp);
			}
		}
		type = map.get(value);
		if (type == null) {
			throw new EnumException();
		}
		return type;
	}
	
	public static void main(String[] args) {
		//String time = "2012-08-28 16:27:23.963";
		/*SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ", Locale.ENGLISH);
		String time = format.format(new Date());
		System.out.println(time);*/
		DateFormatEnum[] types = DateFormatEnum.values();
		for (DateFormatEnum tmp : types) {
			try {
				if(tmp.value()!=""){
					System.out.println("update ds_transformation set date_format='"+tmp.value()+"' where transformation_type_id='35' and date_format='"+tmp.key()+"';");
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
	}
}
