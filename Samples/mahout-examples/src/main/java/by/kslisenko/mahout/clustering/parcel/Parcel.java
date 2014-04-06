package by.kslisenko.mahout.clustering.parcel;

public class Parcel {
	private int weightInGrams;
	public Parcel(int weightInGrams) {
		this.weightInGrams = weightInGrams;
	}
	public int getWeightInGrams() {
		return weightInGrams;
	}
	public void setWeightInGrams(int weightInGrams) {
		this.weightInGrams = weightInGrams;
	}
	@Override
	public String toString() {
		return "parcel, weight: " + weightInGrams;
	}
}