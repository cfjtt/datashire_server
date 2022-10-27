package com.eurlanda.datashire.server.utils.utility;
import org.apache.log4j.Logger;
/**
 * Created by Eurlanda - Java、 on 2017/8/2.
 */
public class FolderFileUtils {
    public static final Logger logger = Logger.getLogger(FolderFileUtils.class);
    /**
     * 对路径进行处理
     *
     * @param filePath
     * @return
     */
    public static String replacePath(String filePath) {
        if ("\\".equals(filePath.substring(filePath.length() - 1))) {
            filePath = filePath.substring(0, filePath.length() - 1);
        } else if ("/".equals(filePath.substring(filePath.length() - 1))) {
            filePath = filePath.substring(0, filePath.length() - 1);
        }
        return filePath;
    }
}
