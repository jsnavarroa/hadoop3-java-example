package co.edu.unal.bda.hadoop;

public class Main {
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			WordCount.main("biblia/input","biblia/p1");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
