package cwwu.haley;

public class Point {
	private double[] coordinate;
	private int dimension;
	
	// Constructor 1 (integer, String)
	public Point(int dimension, String input){
		this.coordinate = new double[dimension];
		this.dimension = dimension;
		String[] tokens = input.split(" ");
		for(int i=0; i<tokens.length; ++i)
			this.coordinate[i] = Double.parseDouble(tokens[i]);
	}
	
	// Constructor 2 (Point)
	public Point(Point p){
		this.dimension = p.getDimension();
		this.coordinate = new double[dimension];
		for(int i=0; i<this.dimension; ++i)
			this.coordinate[i] = p.getDimensionValue(i);
	}

	// Constructor 3 (integer)
	public Point(int dimension){
		this.coordinate = new double[dimension];
		this.dimension = dimension;
	}

	// set Point via String (dimension data)
	public void setPoint(String[] input){
		for(int i=0; i<input.length; ++i)
			this.coordinate[i] = Double.parseDouble(input[i]);
	}

	// set Point via the given point (point data)
	public void setPoint(Point p) {
		for(int i=0; i<dimension; ++i)
			this.coordinate[i] = p.getDimensionValue(i);
	}
	
	// set value to assigned index of dimension
	public void setDimensionValue(int i, double value) {
		coordinate[i] = value;
	}
	
	// get corresponding dimension value of assigned index 
	public double getDimensionValue(int dimension){
		return this.coordinate[dimension];
	}

	// get distance between the given point
	public double getEuclideanDistance(Point p){
		double sum = 0.0f;
		for(int i=0; i<this.dimension; ++i)
			sum += (this.coordinate[i] - p.getDimensionValue(i)) * (this.coordinate[i] - p.getDimensionValue(i));
		return (double) Math.sqrt(sum);
	}

	// get point
	public Point getPoint(){
		return this;
	}

	// get point's dimension
	public int getDimension() {
		return dimension;
	}

	// get all dimension's info
	public double[] getAllCoordinate() {
		return coordinate;
	}
	
	// clear all dimension info
	public void clear() {
		for(int i=0; i<dimension; ++i)
			coordinate[i] = 0.0f;
	}

	// return dimension as string
	@Override
	public String toString() {
		String output = String.valueOf(coordinate[0]);
		for(int i=1; i<coordinate.length; ++i)
			output += " " + String.valueOf(coordinate[i]);
		return output;
	}
}
