package fr.dauphine.lamsade.khalid.dynast.util;

import java.util.Comparator;

import org.apache.lucene.util.OpenBitSet;

public class SortOpenBitSet implements Comparator<OpenBitSet> 
{ 
    // Used for sorting in ascending order of 
    // roll number 
    public int compare(OpenBitSet o1, OpenBitSet o2) 
    { 
    	return (int)(o1.cardinality() - o2.cardinality());
    } 
} 