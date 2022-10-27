package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.wutka.dtd.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * 解析DTD文件
 *
 * @author bo.dang
 * @date 2014年5月8日
 */
public class ReadDTDUtils {

    public static List<XMLNodeUtils> parseDTD(String path, String node, ReturnValue out) {

        List<XMLNodeUtils> xsdNodeUtilList = new ArrayList<XMLNodeUtils>();

        XMLNodeUtils xsdNodeUtil = null;
//      File dtdFile = new File("C:\\APIS\\WorkSpace\\tv.dtd");
//      File dtdFile = new File("D:\\xml\\book.dtd");
        //File dtdFile = new File(path);
//      BufferedReader buff = new BufferedReader(new FileReader("D:\\xml\\book.dtd"));
        BufferedReader buff = null;
        try {
            buff = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_URL);

        }
        DTDParser dtd = new DTDParser(buff);

        //InputSource in = new InputSource(new FileInputStream(new File("D:\\xml\\book.dtd")));
        DTD dt = null;
        try {
            dt = dtd.parse(true);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        List<String> elementNames = new ArrayList<String>();
        Vector ve = dt.items;
        dt.getItem(3);
        dt.getItems();
        dt.getItemsByType(DTDComment.class);

        DTDElement element = dt.rootElement;
        //System.out.println(element.getName());
        DTDItem di = null;
        Iterator it = ve.iterator();
        DTDElement de = null;
        DTDProcessingInstruction dpi = null;
        String str = null;
        DTDComment dc = null;
        DTDAttlist da = null;
        DTDEnumeration du = null;
        Boolean flag = false;
        Map<String, String> dtdMap = new HashMap<String, String>();
        int mapCount = 0;
        while (it.hasNext()) {
            Object obj = it.next();
            if (obj.getClass().equals(DTDComment.class)) {
                dc = (DTDComment) obj;
                System.out.println(" //" + dc.text);
                continue;
            } else if (obj.getClass().equals(DTDElement.class)) {
                de = (DTDElement) obj;

                if (!flag && StringUtils.isNotNull(node)) {
                    if (!de.name.equals(node)) {
                        continue;
                    }
                }
                flag = true;
                di = de.getContent();

            } else if (obj.getClass().equals(DTDAttlist.class)) {
                System.out.println();
                da = (DTDAttlist) obj;
                DTDAttribute[] dattArray = da.getAttribute();
                String name = da.getName();
                if (StringUtils.isNotNull(dattArray) && dattArray.length > 0) {
                    for (int i = 0; i < dattArray.length; i++) {
                        if (dattArray[i].getType().getClass().equals(DTDEnumeration.class)) {
                            du = (DTDEnumeration) dattArray[i].getType();
                            String[] strArray = du.getItems();
                            StringBuffer strBuff = new StringBuffer();
                            for (int k = 0; k < strArray.length; k++) {
                                strBuff.append(strArray[k]).append("|");
                            }
                            System.out.print(" Attlist: " + name + " Name:" + dattArray[i].name + " Type:" + strBuff.toString() + ", DefaultValue: " + dattArray[i].defaultValue);
                        } else {
                            if (StringUtils.isNotNull(dattArray[i].defaultValue)) {
                                System.out.print(" Attlist: " + name + " Name:" + dattArray[i].name + " Type:" + dattArray[i].getType() + ", DefaultValue: " + dattArray[i].defaultValue);
                            } else {
                                System.out.print(" Attlist: " + name + " Name:" + dattArray[i].name + " Type:" + dattArray[i].getType() + ", " + dattArray[i].getDecl().name);
                            }

                        }
                    }
                }
                continue;
            }
            //DTDElement de = (DTDElement) it.next();
            if (di != null) {
                Object ob = di.getClass();
                if (ob.equals(DTDSequence.class)) {
                    DTDSequence ds = (DTDSequence) di;
                    Vector items = ds.getItemsVec();
                    Iterator its = items.iterator();
                    int count = 0;
                    mapCount = 0;
                    dtdMap.clear();
                    while (its.hasNext()) {
                        count++;

                        DTDName des = (DTDName) its.next();
                        if (count == 1) {
                            System.out.print(" " + de.getName() + " have attributes:" + des.getValue() + ":" + des.getCardinal().name);
                        } else {
                            System.out.print(", " + des.getValue() + ":" + des.getCardinal().name);
                        }
                        dtdMap.put(des.getValue(), des.getValue());
                    }
                } else if (ob.equals(DTDMixed.class)) {
                    if (dtdMap.containsValue(de.getName())) {
                        mapCount++;
                        System.out.println("");
                        DTDMixed dm = (DTDMixed) di;
                        Vector items = dm.getItemsVec();
                        Iterator its = items.iterator();
                        while (its.hasNext()) {
                            DTDPCData des = (DTDPCData) its.next();
                            System.out.println(de.getName() + ":Type:pcdata ");
                            xsdNodeUtil = new XMLNodeUtils();
                            xsdNodeUtil.setName(de.getName());
                            xsdNodeUtil.setType(SystemDatatype.NVARCHAR.value());
                            xsdNodeUtil.setLength(256);
                            xsdNodeUtilList.add(xsdNodeUtil);

                        }

                    }
                    if (dtdMap.size() == mapCount) {
                        flag = false;
                    }
                }
            }
        }

        return xsdNodeUtilList;
    }

    public static void main(String[] args) {

        try {

            List<XMLNodeUtils> nodes = parseDTD("D:/xml/book.dtd", "Demo1", null);

            for (XMLNodeUtils node : nodes) {

                System.out.println(node.getUnboundedXpath() + ": " + node.getName() + ": " + node.getType());

            }

        } catch (Exception ex) {

            ex.printStackTrace();

        }
    }

}
