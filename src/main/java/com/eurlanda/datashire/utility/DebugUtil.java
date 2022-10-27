package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.DocExtractSquid;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;

/**
 * @Author <a href="mailto:dang.lu@eurlanda.com">Ludang</a>
 * @ClassName DebugUtil
 * @Description 调时信息工具类（仅DEBUG模式）
 * @Date 2013年12月20日 上午10:38:03
 */
public class DebugUtil {
	// log level GreaterOrEqual DEBUG
	private static final boolean debugEnabled = false; //Logger.getRootLogger().isDebugEnabled();
	
	// 调试信息，打印transformation link 等日志信息
	public static String squidDetail(DataSquid squid){
		if(squid!=null && debugEnabled){
			int s=0;
			StringBuilder b = new StringBuilder(200);
			b.append("squid detail, id="+squid.getId()+", name="+squid.getName()+", type="+SquidTypeEnum.parse(squid.getSquid_type()));
			
			if(squid instanceof StageSquid){
				StageSquid ss = (StageSquid)squid;
				if(ss.getJoins()!=null && !ss.getJoins().isEmpty()){
					s=ss.getJoins().size();
					for(int i=0; i<s; i++){
						b.append("\r\n"+i+"/"+s+"\t").append(ss.getJoins().get(i));
					}
				}
			}
			
			if(squid.getColumns()!=null && !squid.getColumns().isEmpty()){
				s=squid.getColumns().size();
				for(int i=0; i<s; i++){
					b.append("\r\n"+i+"/"+s+"\t").append(squid.getColumns().get(i));
				}
			}
			if(squid.getSourceColumns()!=null && !squid.getSourceColumns().isEmpty()){
				s=squid.getSourceColumns().size();
				for(int i=0; i<s; i++){
					b.append("\r\n"+i+"/"+s+"\t").append(squid.getSourceColumns().get(i));
				}
			}
			if(squid.getTransformationLinks()!=null && !squid.getTransformationLinks().isEmpty()){
				s=squid.getTransformationLinks().size();
				for(int i=0; i<s; i++){
					b.append("\r\n"+i+"/"+s+"\t").append(squid.getTransformationLinks().get(i));
				}
			}
			if(squid.getTransformations()!=null && !squid.getTransformations().isEmpty()){
				s=squid.getTransformations().size();
				for(int i=0; i<s; i++){
					b.append("\r\n"+i+"/"+s+"\t").append(squid.getTransformations().get(i));
				}
			}
			return b.toString();
		}else{
			return "";
		}
	}

	   // 调试信息，打印transformation link 等日志信息
    public static String squidDetail(DocExtractSquid squid){
        if(squid!=null && debugEnabled){
            int s=0;
            StringBuilder b = new StringBuilder(200);
            b.append("squid detail, id="+squid.getId()+", name="+squid.getName()+", type="+SquidTypeEnum.parse(squid.getSquid_type()));
            
/*            if(squid instanceof StageSquid){
                StageSquid ss = (StageSquid)squid;
                if(ss.getJoins()!=null && !ss.getJoins().isEmpty()){
                    s=ss.getJoins().size();
                    for(int i=0; i<s; i++){
                        b.append("\r\n"+i+"/"+s+"\t").append(ss.getJoins().get(i));
                    }
                }
            }*/
            
            if(squid.getColumns()!=null && !squid.getColumns().isEmpty()){
                s=squid.getColumns().size();
                for(int i=0; i<s; i++){
                    b.append("\r\n"+i+"/"+s+"\t").append(squid.getColumns().get(i));
                }
            }
            if(squid.getSourceColumns()!=null && !squid.getSourceColumns().isEmpty()){
                s=squid.getSourceColumns().size();
                for(int i=0; i<s; i++){
                    b.append("\r\n"+i+"/"+s+"\t").append(squid.getSourceColumns().get(i));
                }
            }
            if(squid.getTransformationLinks()!=null && !squid.getTransformationLinks().isEmpty()){
                s=squid.getTransformationLinks().size();
                for(int i=0; i<s; i++){
                    b.append("\r\n"+i+"/"+s+"\t").append(squid.getTransformationLinks().get(i));
                }
            }
            if(squid.getTransformations()!=null && !squid.getTransformations().isEmpty()){
                s=squid.getTransformations().size();
                for(int i=0; i<s; i++){
                    b.append("\r\n"+i+"/"+s+"\t").append(squid.getTransformations().get(i));
                }
            }
            return b.toString();
        }else{
            return "";
        }
    }
	public static boolean isDebugenabled() {
		return debugEnabled;
	}
	public static boolean isDebugdisabled() {
		return !debugEnabled;
	}
}
