package util;

import util.client.CalculatorImplServiceLocator;
import util.client.CalculatorService;

public class Test {

	/**
	 * 2014-11-28
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param args
	 */
	public static void main(String[] args) {
		
		try{
			CalculatorImplServiceLocator cisl = new CalculatorImplServiceLocator();
			CalculatorService cs = cisl.getCalculatorImplPort();
			cs.add(3l, 1l);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
