package main.java;

import bsh.EvalError;
import bsh.Interpreter;

public class BeanshellMain {

	public static void main(final String[] args) throws EvalError {
		
		final String s = "List list = new ArrayList(); 	int icount = Integer.parseInt(count);  for (int i=0;i<icount;i++) { list.add(String.valueOf(i)); System.out.println(\"hola\");}"; 
		final Interpreter i = new Interpreter();
		i.set("count", "3");
		i.eval(s);
		System.out.println( i.get("list") );
	}
}
