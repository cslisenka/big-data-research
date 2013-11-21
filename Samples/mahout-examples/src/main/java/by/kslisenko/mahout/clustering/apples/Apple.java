package by.kslisenko.mahout.clustering.apples;

public class Apple {

	private int weightInGrams;
	private AppleColor color;
	private AppleSize size;
	
	public Apple(int weightInGrams, AppleColor color, AppleSize size) {
		this.weightInGrams = weightInGrams;
		this.color = color;
		this.size = size;
	}

	public int getWeightInGrams() {
		return weightInGrams;
	}

	public void setWeightInGrams(int weightInGrams) {
		this.weightInGrams = weightInGrams;
	}

	public AppleColor getColor() {
		return color;
	}

	public void setColor(AppleColor color) {
		this.color = color;
	}

	public AppleSize getSize() {
		return size;
	}

	public void setSize(AppleSize size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return color + " " + size + " weight: " + weightInGrams + " apple";
	}
}