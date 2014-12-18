package edu.neu.mr.project.data;

/**
 * Class representing KMeans Information
 * @author amansharma
 *
 */
public class KMeansInfo {

	private double sum;
	private int frequency;
	private double centroid;
	
	public KMeansInfo(double d, int freq, double centroid) {
		this.sum = d;
		this.frequency = freq;
		this.centroid = centroid;
	}

	public double getSum() {
		return sum;
	}

	public void setSum(double sum) {
		this.sum = sum;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}
	
	

	public double getCentroid() {
		return centroid;
	}

	public void setCentroid(double centroid) {
		this.centroid = centroid;
	}

	@Override
	public String toString() {
		return "KMeansInfo [sum=" + sum + ", frequency=" + frequency
				+ ", centroid=" + centroid + "]";
	}

}
