package fr.dauphine.lamsade.khalid.dynast.util;

import java.util.ArrayList;
import java.util.Arrays;

public class DiffVector {
	
	int last_op_id;
	ArrayList<Boolean> vector;
	
	public DiffVector(){
		this.last_op_id = 0;
		this.vector = new ArrayList<Boolean>();
	}
	
	public DiffVector(int size, boolean b) {
		
		this.vector = new ArrayList<Boolean>();
		for (int i =0;i<size; i++)
			this.vector.add(b);
	}

	public int getLast_op_id() {
		return last_op_id;
	}

	public void setLast_op_id(int last_op_id) {
		this.last_op_id = last_op_id; 
	}
	
	public void add(Boolean value) {
		this.vector.add(value);
	}
	
	public void remove(int position) {
		this.vector.remove(position);
	}
	
	public Boolean get(int position) {
		return this.vector.get(position);
	}
	
	public ArrayList<Boolean> getVector(){
		return this.vector;
	}
	
	public void setLast(Boolean b) {
		this.vector.set(this.vector.size()-1, b);
	}
	
	public int getSize() {
		return this.vector.size();
	}
	
	public String toString() {
		return Arrays.toString(this.vector.toArray());
	}

}
