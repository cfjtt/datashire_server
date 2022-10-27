package com.eurlanda.datashire.utility;

public class XMLNodeUtils {
      // 节点名称 

        private String name; 

        // 节点XPath 

        private String xPath; 

        // 节点描述 

        private String annotation; 

        // 节点类型 

        private int type; 
        
        // 长度
        private int length;

        // 业务用路径,描述路径中的unbound节点 

        private String unboundedXpath; 
        
        private String is_unbounded;

        public String getName() { 

        return name; 

        } 

        public void setName(String name) { 

        this.name = name; 

        } 

        public String getXPath() { 

        return xPath; 

        } 

        public void setXPath(String path) { 

        xPath = path; 

        } 

        public String getAnnotation() { 

        return annotation; 

        } 

        public void setAnnotation(String annotation) { 

        this.annotation = annotation; 

        } 

        public int getType() { 

        return type; 

        } 

        public void setType(int type) { 

        this.type = type; 

        } 

        public String getUnboundedXpath() { 

        return unboundedXpath; 

        } 

        public void setUnboundedXpath(String unboundedXpath) { 

        this.unboundedXpath = unboundedXpath; 

        }

        public String getIs_unbounded() {
            return is_unbounded;
        }

        public void setIs_unbounded(String is_unbounded) {
            this.is_unbounded = is_unbounded;
        }

		public int getLength() {
			return length;
		}

		public void setLength(int length) {
			this.length = length;
		} 
}
