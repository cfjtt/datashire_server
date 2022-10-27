package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReadXSDUtils {

    private static List<XMLNodeUtils> list = new ArrayList<XMLNodeUtils>();
    
    private static List<String> nodeList = new ArrayList<String>();
    
    private static Boolean flag = false;
    
    private static int count = 1;
    
    private static String currentXsdPath = null;

    /**
     * 
     * 解析XSD，返回数据节点对象列表
     * @param xsd
     * 
     * @return
     * 
     * @throws Exception
     */

    public static List<XMLNodeUtils> paserXSD(String xsd, String node, ReturnValue out) {

        SAXReader saxReader = new SAXReader();

        // ByteArrayInputStream byteArrayInputStream = new
        // ByteArrayInputStream(xsd.getBytes("utf-8"));

        // Document doc = saxReader.read(byteArrayInputStream);
        Document doc = null;
        try {
        	File file = new File(xsd);
            doc = saxReader.read(file);
        } catch (DocumentException e) {
            // TODO Auto-generated catch block
            //e.printStackTrace();
            out.setMessageCode(MessageCode.ERR_URL);
            return null;
        }

        Element element = doc.getRootElement();
        // String basePath = "//" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE+
        // ":element[@name=\"" + XMLConstantUtils.MESSAGE + "\"]";

        /*
         * if(null == element.selectNodes(basePath)){ return null; }
         */
        // Element dataElement = (Element) element.selectSingleNode(basePath);
/*            Pattern p = Pattern.compile("\\\\");
            Matcher m = p.matcher(node);
            if(m.find()){
                nodeArray = node.split("\\\\");
            }
            else {
                nodeArray[0] = node;
            }*/
            nodeList.clear();
            count = 1;
            flag = false;
            currentXsdPath = null;
            Pattern p = Pattern.compile("/");
            Matcher m = p.matcher(node);
            if(m.find()){
               String[] tempArray = node.split("/");
                
                for (int i = 0; i < tempArray.length; i++) {
                    if(StringUtils.isNotNull(tempArray[i])){
                        nodeList.add(tempArray[i]);
                    }
                }
                
            }
            else {
                nodeList.add(node);
            }
            
/*            if(node.contains("\\")){
                nodeArray = node.split("\\");
            }
            else {
                nodeArray[0] = node;
            }*/
            
        XMLConstantUtils.MESSAGE = nodeList.get(0);
        String basePath = null;
        Element dataElement = null;
        if ("".equals(XMLConstantUtils.XSD_DEFAULT_NAMESPACE)) {
            if ("".equals(XMLConstantUtils.MESSAGE)) {
                dataElement = element;
            } else {
                basePath = "//element[@name=\"" + XMLConstantUtils.MESSAGE
                        + "\"]";
                dataElement = (Element) element.selectSingleNode(basePath);
            }
        } else {
            basePath = "//" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element[@name=\"" + XMLConstantUtils.MESSAGE + "\"]";
            //basePath = "//xs:element[@name=\"Orders\"]";
            dataElement = (Element) element.selectSingleNode(basePath);
        }

        String elementPath = null;

        if ("".equals(XMLConstantUtils.XSD_DEFAULT_NAMESPACE)) {
            elementPath = "//element";
        } else {
            elementPath = "//" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element";
        }

        list.clear();
        paseData(dataElement, "//", elementPath, "/");

        return list;

    }

    /**
     * 
     * 转换XSD的数据节点，生成XSDNode对象
     * 
     * 
     * 
     * @param element
     * 
     * @param xPath
     * 
     * @param xsdPath
     * 
     * @param unboundedXpath
     */

    public static void paseData(
            Element element,
            String xPath,
            String xsdPath,
            String unboundedXpath) {
        if (element == null)
            return;
        // 获取节点name属性
        String nodeName = element.attributeValue("name");
        
        // 组装xml文档中节点的XPath

        xPath += nodeName;

        unboundedXpath += nodeName;

        // 并列多节点限制属性
        String maxOccurs = element.attributeValue("maxOccurs");

        if (maxOccurs != null && !"1".equals(maxOccurs)
                && !("//" + XMLConstantUtils.MESSAGE + "").equals(xPath)) {// 节点可以有多个
            unboundedXpath += XMLConstantUtils.XSD_UNBOUNDED;
        }
        
        // 组装下一个element元素的XPath
        if(!flag && nodeList.size() > 1 && count == 1){
            currentXsdPath = xsdPath + "[@name=\"" + nodeName + "\"]" + "/"
                    + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":choice/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element" + "[@name=\"" + nodeList.get(count)+ "\"]";
            flag = true;
            //nodeArray.
        }
        else if(nodeList.size() == 1){
            currentXsdPath = xsdPath + "[@name=\"" + nodeName + "\"]" + "/"
                    + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":choice/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":sequence/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element";
        }
        else if(nodeList.size() > 2 && nodeList.size()-1 == count){
        	currentXsdPath = xsdPath + "/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":sequence/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element" + "[@name=\"" + nodeList.get(count) + "\"]";
        }
        else if(flag && nodeList.size() > count) {
/*            currentXsdPath = xsdPath + "[@name=\"" + nodeName + "\"]" + "/"
                    + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":choice/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":sequence/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element";*/
            currentXsdPath = xsdPath + "/"
                    + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":choice/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element" + "[@name=\"" + nodeList.get(count) + "\"]";
            flag = true;
        }
        else if(flag && nodeList.size() == count){
            currentXsdPath = xsdPath + "/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":sequence/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element";
        }
        else if(flag && count > nodeList.size()){
/*            currentXsdPath = xsdPath + "/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":complexType/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":sequence/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":element";*/
            flag = false;
        }

        List<Node> elementNodes = null;
        if(flag){
        	System.out.println(count+":"+nodeName+":"+currentXsdPath);
            elementNodes = element.selectNodes(currentXsdPath);
        }
        // 查找该节点下所有的element元素

        if (elementNodes != null && elementNodes.size() > 0) {// 如果下面还有element,说明不是叶子
        	count ++;
        	Iterator<Node> nodes = elementNodes.iterator();
            while (nodes.hasNext()) {
                if (!xPath.endsWith("/")) {
                    xPath += "/";
                    unboundedXpath += "/";
                }
                Element ele = (Element) nodes.next();
                //flag = true;
                paseData(ele, xPath, currentXsdPath, unboundedXpath);
            }
        } else { // 该element为叶子

            //输入的元数据地址是错误的,此时nodeList>count
           if(nodeList.size()>count){
               return;
           }
           /* if(count!=xPath.substring(2).split("/").length || count!=nodeList.size()){
                return;
            }*/
            XMLNodeUtils xsdNode = new XMLNodeUtils();

            // 获取注释节点

            String annotation = "";
            
            System.out.println(xsdPath + "[@name=\"" + nodeName + "\"]/"
                    + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":annotation/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":documentation");

            Node annotationNode = element.selectSingleNode(xsdPath + "[@name=\"" + nodeName + "\"]/"
                    + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":annotation/" + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                    + ":documentation");

            if (annotationNode != null)

                annotation = annotationNode.getText();

            // 获取节点类型属性

            String nodeType = "";

            Attribute type = element.attribute("type");

            if (type != null) {

                nodeType = type.getText();

            } else {

                String spath = xsdPath + "[@name=\"" + nodeName + "\"]/"
                        + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                        + ":simpleType/"

                        + XMLConstantUtils.XSD_DEFAULT_NAMESPACE
                        + ":restriction";

                Element typeNode = (Element) element.selectSingleNode(spath);

                if (typeNode != null) {

                    Attribute base = typeNode.attribute("base");

                    if (base != null)

                        nodeType = base.getText();

                }

            }
            String lastType = null;
            int dataType = 0;
            if(null != nodeType && nodeType.contains(":")){
                lastType = nodeType.split(":")[1];
                // String类型
                if(lastType.toLowerCase().equals("string")){
                    dataType = DbBaseDatatype.NVARCHAR.value();
                    xsdNode.setLength(256);
                }
                // Integer
                else if(lastType.toLowerCase().equals("int")){
                    dataType = DbBaseDatatype.INT.value();
                }
                // Date
                else if(lastType.toLowerCase().equals("date")){
                    dataType = DbBaseDatatype.DATETIME.value();
                }
                // DateTime
                else if(lastType.toLowerCase().equals("datetime")){
                    dataType = DbBaseDatatype.DATETIME.value();
                }
                // Decimal
                else if(lastType.toLowerCase().equals("decimal")){
                    dataType = DbBaseDatatype.DECIMAL.value();
                    xsdNode.setLength(18);
                }
                // boolean
                else if(lastType.toLowerCase().equals("boolean")){
                    dataType = DbBaseDatatype.BIT.value();
                }
                // base64Binary
                else if(lastType.toLowerCase().equals("base64binary")){
                	//dataType = DbBaseDatatype.BINARY.value();
                	dataType = DbBaseDatatype.NVARCHAR.value();
                    xsdNode.setLength(256);
                }
                // double
                else if(lastType.toLowerCase().equals("double")||lastType.toLowerCase().equals("float")){
                	dataType = DbBaseDatatype.FLOAT.value();
                }
                else if (lastType.toLowerCase().equals("short")){
                	dataType = DbBaseDatatype.TINYINT.value();
                }else{
                	dataType = DbBaseDatatype.NVARCHAR.value();
                    xsdNode.setLength(256);
                }
            }
            else {
                dataType = DbBaseDatatype.NVARCHAR.value();
                xsdNode.setLength(256);
            }
            
            xsdNode.setName(nodeName);

            xsdNode.setXPath(xPath);

            xsdNode.setAnnotation(annotation);

            xsdNode.setType(dataType);

            xsdNode.setUnboundedXpath(unboundedXpath);

            list.add(xsdNode);
        }

    }

    public static void main(String[] args) {

        try {
//        	List<XMLNodeUtils> nodes = paserXSD("D:/xml/Orders.xsd", "/Demo/Ord", null);
            List<XMLNodeUtils> nodes = paserXSD("D:/work/Trans/业务测试数据/Orders.xsd", "/Demo/Ord", null);

            for (XMLNodeUtils node : nodes) {

                System.out.println(node.getUnboundedXpath() + ": " + node.getName() + ": " + node.getType());

            }

        } catch (Exception ex) {

            ex.printStackTrace();

        }

    }
}
