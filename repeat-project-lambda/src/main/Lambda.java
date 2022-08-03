package main;

import java.util.Map;

import runner.Runner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;


public class Lambda implements RequestHandler<Map<String, Object>, String> {
	
	public static void main(String[] args) {
		System.out.println("Lambda started: main");
		// TODO Auto-generated method stub
		Runner runner = new Runner();
		runner.init();
		runner.run();
	}
	
	public String handleRequest(Map<String, Object> m, Context c) {
		System.out.println("Lambda started: Handle Request");
		Runner runner = new Runner();
		runner.init();
		runner.run();
		return "";
	}
	

}
