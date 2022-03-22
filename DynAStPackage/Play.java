package fr.dauphine.lamsade.khalid.dynast;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Vector;

public class Play {
	
	List<BitSet> bitsetlist;
	List<BitSet> possiblebitsetlist = new ArrayList<BitSet>();
	int num_attributes;
	
	
    void initializeBitSetList(int num_attributes) {
    	//System.out.print("2 to the power of "+this.getNumberAttributes()+" is "+Math.pow(2,this.getNumberAttributes()));
    	//for (int i=0; i<Math.pow(2,this.getNumberAttributes()); i++) {
    	for (int i=0; i<Math.pow(2,num_attributes); i++) {	
    		//System.out.println("Binary string"+Integer.toBinaryString(i));
    		//System.out.println("Bit set"+this.fromString(Integer.toBinaryString(i)));
    		this.possiblebitsetlist.add(this.fromString(Integer.toBinaryString(i*2)));
    	}
    
    }
    
    BitSet fromString(String binary) {
        BitSet bitset = new BitSet(binary.length());
        int len = binary.length();
        for (int i = len-1; i >= 0; i--) {
            if (binary.charAt(i) == '1') {
                bitset.set(len-i-1);
            }
        }
        return bitset;
    }
    
    public void printBitSetList(List<BitSet> bitsetlist) {
    	for (int i=0;i<bitsetlist.size();i++)
    		System.out.println(bitsetlist.get(i));
    	
    }
    
    public List<BitSet> zipVector(List<Vector<Boolean>> lv, int vector_size, int num_attributes){
    	BitSet bs;
    	 List<BitSet> bitsetlist = new ArrayList<BitSet>();
    	 for (int i=0;i<vector_size; i++) {
    		 bs = new BitSet();
    		 for (int j=0;j<num_attributes;j++) {
    			if (lv.get(j).get(i))
    				bs.set(j+1);
    		 }
    		 if (!bitsetlist.contains(bs))
    			 bitsetlist.add(bs);
    		 if (bitsetlist.size() == this.possiblebitsetlist.size())
    			 break; 
    	 }
    	 
    	 return bitsetlist;
    	
    }
    
    public List<BitSet> zipArray(List<ArrayList<Boolean>> la, int array_size, int num_attributes){
    	 BitSet bs;
    	 List<BitSet> bitsetlist = new ArrayList<BitSet>();
    	 for (int i=0;i<array_size; i++) {
    		 bs = new BitSet();
    		 for (int j=0;j<num_attributes;j++) {
    			if (la.get(j).get(i))
    				bs.set(j+1);
    		 }
    		 if (!bitsetlist.contains(bs))
    			 bitsetlist.add(bs);
    		 if (bitsetlist.size() == this.possiblebitsetlist.size())
    			 break; 
    	 }
    	 
    	 return bitsetlist;
    	
    }
    
    public void printVector(List<Vector<Boolean>> lv) {
    	System.out.println("Print Vectors");
    	for (int i=0;i<lv.size();i++) {
    		for (int j=0;j<lv.get(i).size();j++) {
    			System.out.print(lv.get(i).get(j)+" ");
    		}
    		System.out.println();
    	}
    		
    		
    }

	public static void main(String[] args) {
		/*
		Vector<Boolean> bool_v = new Vector<Boolean>();
		
		Vector<Integer> integer_v = new Vector<Integer>();
		
		for (int i = 0; i<1000; i++) {
			bool_v.add(true);
			integer_v.add(100);
		}
		*/
		
		long m0 = Runtime.getRuntime().freeMemory();
		
		Vector<Boolean> bool_v = new Vector<Boolean>();
		for (int i = 0; i<1000000; i++) {
			bool_v.add(true);
		}
		
		long m1 = Runtime.getRuntime().freeMemory();
		System.out.println(m0 - m1);
		Vector<Integer> bool_i = new Vector<Integer>();
		for (int i = 0; i<1000000; i++) {
			bool_i.add(1);
		}
		long m2 = Runtime.getRuntime().freeMemory();
		System.out.println(m1 - m2);
		
		
		Play p = new Play();
		int vector_size = 90000;
		
	    Instant start; 
	       
	    Instant end;
		
		Vector<Boolean> bv1 = new Vector<Boolean>();
		Vector<Boolean> bv2 = new Vector<Boolean>();
		Vector<Boolean> bv3 = new Vector<Boolean>();
		Vector<Boolean> bv4 = new Vector<Boolean>();
		List<Vector<Boolean>> lv = new ArrayList<Vector<Boolean>>();
		
		lv.add(bv1);
		lv.add(bv2);
		lv.add(bv3);
		lv.add(bv4);
		
		Random random = new Random();
		p.num_attributes=4;
		
		
		
		for (int i=0;i<vector_size;i++)
			for (int j=0;j<p.num_attributes;j++)
				//lv.get(j).add(random.nextBoolean());
				lv.get(j).add(true);
		
		p.printVector(lv);
		
				
		

		

		p.initializeBitSetList(4);
		//System.out.println("Possible bit set");
		//p.printBitSetList(p.possiblebitsetlist);
		
		
		start = Instant.now();
		List<BitSet> bitsetlist = p.zipVector(lv,vector_size,4);
		end = Instant.now();
		p.printBitSetList(bitsetlist);
		System.out.println("Duration Vector of booleans: "+Duration.between(start, end).toMillis());
		
		
		ArrayList<Integer> list_int = new ArrayList<Integer>();
		for (int i=0;i<vector_size;i++)
			list_int.add(1);
		
		fr.dauphine.lamsade.khalid.dynast.util.Vector v_1 = new fr.dauphine.lamsade.khalid.dynast.util.Vector(list_int);
		fr.dauphine.lamsade.khalid.dynast.util.Vector v_2 = new fr.dauphine.lamsade.khalid.dynast.util.Vector(list_int);
		fr.dauphine.lamsade.khalid.dynast.util.Vector v_3 = new fr.dauphine.lamsade.khalid.dynast.util.Vector(list_int);
		fr.dauphine.lamsade.khalid.dynast.util.Vector v_4 = new fr.dauphine.lamsade.khalid.dynast.util.Vector(list_int);
	

	   start = Instant.now();	
       v_1 = v_1.plus(v_2);
       v_1 = v_1.plus(v_3);
       v_1 = v_1.plus(v_4);
       List<Integer> numbers = v_1.getDictinctValues(p.num_attributes);
       end = Instant.now();
	   		
       System.out.println("Duration Vector of Integers: "+Duration.between(start, end).toMillis());	
		
		


		ArrayList<Boolean> ba1 = new ArrayList<Boolean>();
		ArrayList<Boolean> ba2 = new ArrayList<Boolean>();
		ArrayList<Boolean> ba3 = new ArrayList<Boolean>();
		ArrayList<Boolean> ba4 = new ArrayList<Boolean>();
		List<ArrayList<Boolean>> la = new ArrayList<ArrayList<Boolean>>();
		
		la.add(ba1);
		la.add(ba2);
		la.add(ba3);
		la.add(ba4);
  
		
		for (int i=0;i<vector_size;i++)
			for (int j=0;j<p.num_attributes;j++)
				//lv.get(j).add(random.nextBoolean());
				la.get(j).add(true);
		
		
		start = Instant.now();
		List<BitSet> bitsetlist1 = p.zipArray(la,vector_size,4);
		end = Instant.now();
		p.printBitSetList(bitsetlist);
		System.out.println("Duration array of booleans: "+Duration.between(start, end).toMillis());
	   
	}

}
