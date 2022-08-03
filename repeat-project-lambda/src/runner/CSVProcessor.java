package runner;

import java.io.IOException;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import java.util.HashMap;
import java.nio.charset.StandardCharsets;
//import com.opencsv.;

public class CSVProcessor {
	private S3Client s3Client;
	public int totalNbSales;
	public int totalAmountSold;
	public HashMap<String, Float> avgSoldPerProduct;
	CSVProcessor(S3Client s3Client) {
		this.s3Client = s3Client;
	}
	
	public void processFile(String bucketName, String fileName) {
		 
		 GetObjectRequest gor = GetObjectRequest.builder().bucket(bucketName).key(fileName).build();
		 ResponseInputStream<GetObjectResponse> responseStream = this.s3Client.getObject(gor);
		 byte[] bytes = new byte[0];
		 try {
			bytes = responseStream.readAllBytes();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (bytes.length == 0) {
			return;
		}
		String s = new String(bytes, StandardCharsets.UTF_8);
		int totalNbSales = 0;
		int totalAmountSold = 0;
		HashMap<String, int[]> map = new HashMap<String, int[]>(); 
		// indexed by product name, array : [0] is NbSales, [1] is amountSold 
		String[] lines = s.split("\n");

		for (int i = 1; i < lines.length; i++) { // remove first line with description
			String[] line = lines[i].split(";");
			int amount = Integer.parseInt(line[3]);
			totalNbSales += 1;
			totalAmountSold += amount;
			if (map.containsKey(line[2])) {
				map.put(line[2], new int[] {map.get(line[2])[0] + 1, map.get(line[2])[1] + amount});
			} else {
				map.put(line[2], new int[] {1, amount});
			}
		}
		System.out.printf("Processed sales.csv: %d sales and %d amountSold \n", totalNbSales, totalAmountSold);
		this.totalAmountSold = totalAmountSold;
		this.totalNbSales = totalNbSales;
		this.avgSoldPerProduct = new HashMap<String, Float>();
		for (HashMap.Entry<String, int[]> entry : map.entrySet()) {
		    String key = entry.getKey();
		    int[] value = entry.getValue();
		    this.avgSoldPerProduct.put(key, (float) value[1] / value[0]);
		}
	}
}
