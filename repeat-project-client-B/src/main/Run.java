package main;

import retrieve.Retriever;

import java.util.Timer;
import java.util.TimerTask;

public class Run {
	public static void main(String[] args) {
		Retriever ret = new Retriever();
		ret.init();
		
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
			
			if (ret.retrieveMessage()) {
				ret.retrieveFile("/home/nicolascrd/Downloads/cc/oldFile.csv", "/home/nicolascrd/Downloads/cc/newFile.json");
				System.exit(1);
			}
			
		}
		}, 0, 5000);
		
	}
}
