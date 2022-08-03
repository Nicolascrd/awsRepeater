package main;

import java.io.IOException;

import ec2Worker.RunEC2Worker;

public class Run {
	public static void main(String []args) throws IOException {
		System.out.println("Launching EMSE repeat-project");
		RunEC2Worker ec2 = new RunEC2Worker();
		ec2.initWorker();
		ec2.run();
		// ec2.stop();
	}
}
