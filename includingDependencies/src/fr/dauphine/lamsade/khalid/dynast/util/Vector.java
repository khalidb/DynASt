package fr.dauphine.lamsade.khalid.dynast.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class Vector { 

    //private int[] data;       // array of vector's components
    private List<Integer> data;

    // create the zero vector of length n
    public Vector() {
        this.data = new ArrayList<Integer>();
    }
 
    // create a vector from an array
    public Vector(ArrayList<Integer> data) {
        this.data = data;
        // defensive copy so that client can't alter our copy of data[]
        /*
        this.data = new int[n];
        for (int i = 0; i < n; i++)
            this.data[i] = data[i];
            */
    } 
    
    public void add(Integer i) {
    	this.data.add(i);
    }
    
    public void remove(int index) {
    	this.data.remove(index);
    }
    
    // create a vector from either an array or a vararg list
    // this constructor uses Java's vararg syntax to support
    // a constructor that takes a variable number of arguments, such as
    // Vector x = new Vector(1.0, 2.0, 3.0, 4.0);
    // Vector y = new Vector(5.0, 2.0, 4.0, 1.0);
/*
    public Vector(int... data) {
        n = data.length;

        // defensive copy so that client can't alter our copy of data[]
        this.data = new int[n];
        for (int i = 0; i < n; i++)
            this.data[i] = data[i];
    }
*/
    // return the length of the vector
    public int size() {
        return this.data.size();
    }

    
    // return the inner product of this Vector a and b
    public int dot(Vector that) {
        if (this.size() != that.size())
            throw new IllegalArgumentException("dimensions disagree");
        int sum = 0;
        for (int i = 0; i < this.size(); i++)
            sum = sum + (this.data.get(i) * that.data.get(i));
        return sum;
    }
    
    public Vector getClone() {
    	
    	Vector v = new Vector();
    	v.data.addAll(this.data);
    	return v;
    	
    }

    // return the Euclidean norm of this Vector
 /*   public int magnitude() {
        return Math.sqrt(this.dot(this));
    } */

    // return the Euclidean distance between this and that
 /*   public int distanceTo(Vector that) {
        if (this.length() != that.length())
            throw new IllegalArgumentException("dimensions disagree");
        return this.minus(that).magnitude();
    } */

    // return this + that
    public Vector plus(Vector that) {
        if (this.size() != that.size())
            throw new IllegalArgumentException("dimensions disagree");
        Vector c = new Vector();
        for (int i = 0; i < this.size(); i++)
            c.data.add(this.data.get(i) + that.data.get(i));
        return c;
    }

    // return this - that
    public Vector minus(Vector that) {
        if (this.size() != that.size())
            throw new IllegalArgumentException("dimensions disagree");
        Vector c = new Vector();
        for (int i = 0; i < this.size(); i++)
            c.data.add(this.data.get(i) - that.data.get(i));
        return c;
    }

    // return the corresponding coordinate
    public int cartesian(int i) {
        return data.get(i);
    }


    // create and return a new object whose value is (this * factor)
    public Vector scale(int factor) {
        Vector c = new Vector();
        for (int i = 0; i < this.data.size(); i++)
            c.data.add(factor * data.get(i));
        return c;
    }


    // return the corresponding unit vector
/*    public Vector direction() {
        if (this.magnitude() == 0.0)
            throw new ArithmeticException("zero-vector has no direction");
        return this.scale(1.0 / this.magnitude());
    } */

    // return a string representation of the vector
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append('(');
        for (int i = 0; i < this.size(); i++) {
            s.append(data.get(i));
            if (i < this.size()-1) s.append(", ");
        }
        s.append(')');
        return s.toString();
    }
    
    public int get(int i) {
    	return this.data.get(i);
    }

    
    public void set(int i, Integer v) {
    	this.data.set(i,v);
    }

    public ObjectArrayList<Integer> getDictinctValues(int attributeNumber) {
    	
    	ObjectArrayList<Integer> numbers = new ObjectArrayList<Integer>();
    	if (attributeNumber <2)
    		System.err.println("It seems that there is only one or no attribute in the relation");
    	
    	for (int i=1;i < (int) Math.pow(2, attributeNumber); i++)
    		if (this.data.contains(i))
    			numbers.add(i);
    	
    	return numbers;

        
    	
    }
    
    // test client
    public static void main(String[] args) {
        ArrayList<Integer> xdata =  new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));
        ArrayList<Integer> ydata =  new ArrayList<Integer>(Arrays.asList(5, 2, 4, 1)); 

        Vector x = new Vector(xdata);
        Vector y = new Vector(ydata);
        
        

        System.out.println("x        =  " + x);
        System.out.println("y        =  " + y);
        System.out.println("x + y    =  " + x.plus(y));
        System.out.println("10x      =  " + x.scale(10));
//        System.out.println("|x|      =  " + x.magnitude());
        System.out.println("<x, y>   =  " + x.dot(y));
        System.out.println("|x - y|  =  " + x.minus(y));
        
        
        
        System.out.println("clone x");
        Vector xc = x.getClone();
        xc.data.set(1, 50);
        System.out.println("original: "+x);
        System.out.println("clone: "+xc);
        
        

        
        
    }
}