package fr.dauphine.lamsade.khalid.dynast;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import au.com.bytecode.opencsv.CSVReader;
import de.metanome.algorithm_integration.ColumnIdentifier;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class CompetitiveEvaluation {
	
    protected List<String> columnNames;
    protected ObjectArrayList<ColumnIdentifier> columnIdentifiers;
	String file = "", result_file; 
	FDEP_AS algo = null;
	//int numberOfBatches;
	int numberAttributes;
	List<ArrayList<Integer>> tuples;
	Instant start, end;
    
    public List<OpenBitSet> bitsetmap = new ArrayList<OpenBitSet>() ;
    
    public CompetitiveEvaluation(String file, String result_file) {
		this.file = file;
		//this.numberOfBatches = _numberOfBatches;
		this.algo = new FDEP_AS(this.numberAttributes ,new ValueComparator(true));
		this.algo.op_ID = 0;
		this.result_file = result_file;
    }

    public void execute() throws FileNotFoundException, IOException {
		File insert_OutputFile = new File("Results/insert_"+this.result_file);
		File delete_OutputFile = new File("Results/delete_"+this.result_file);
		tuples = new ArrayList<ArrayList<Integer>>();
		
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+".csv"));) {
    	    String[] values = null;
    	    if ((values = csvReader.readNext()) != null) {
    	    	this.algo.setColumnNames(Arrays.asList(values))	;
    	    	this.algo.setNumberAttributes(this.algo.getColumnNames().size());
    	    	this.numberAttributes = this.algo.getColumnNames().size();
    	    	//this.algo.initializeIndexStructure();    	
    	    }    	    
    	    while ((values = csvReader.readNext()) != null) {  	
    	    	this.tuples.add((ArrayList<Integer>) Arrays.asList(values).stream()
    	                .map(Integer::valueOf).collect(Collectors.toList()));
    	    }
    	    
    	}
		
    }
    
    public void printbitsetmap() {
    	for (OpenBitSet bs: this.bitsetmap)
    		System.out.println(BitSetUtils.toString(bs, this.numberAttributes));
    }
    
    public void computeAgreetSets() {
    	OpenBitSet bs;
    	OpenBitSet empty_bs = new OpenBitSet(this.numberAttributes);
    	
    	start = Instant.now();
    	for (int i=0; i< this.tuples.size(); i++)
    		for(int j = i + 1; j< this.tuples.size(); j++) {
    			bs = new OpenBitSet(this.numberAttributes);
    			for (int k = 0; k < this.numberAttributes; k++) {
    				if (this.tuples.get(i).get(k) == this.tuples.get(j).get(k))
    					bs.set((long) k);
    			}
    			if (!bs.equals(empty_bs))
    				if (!bitsetmap.contains(bs))
    					bitsetmap.add(bs);
    		}
    	end = Instant.now();
    	System.out.println("Agree sets computation took: "+Duration.between(start, end).toMillis());
    }
    
    public void PrintTuples() {
    	
    	for (List<Integer> tuple: this.tuples)
    		System.out.println(tuple);
    	/*
	    while ((values = csvReader.readNext()) != null) { 
	    	
	    	for (int i =0; i< values.length; i++) {
	    		if (i!=0)
	    			System.out.print(", ");
	    		else {
	    			System.out.println("");
	    			System.out.print("Line ");
	    		}
	    		System.out.print(this.algo.columnNames.get(i)+": "+Integer.parseInt(values[i]));
	    	}
	    }
	    */
    }
    
	public static void main(String[] args) {
		
		//String file = "resources/iris_10/iris-num";
		String result_file = "deletion_uniprot_90p";
		
		//String file = "resources/claims_all/claims";
		String file = "resources/deletion_uniprot_25p/uniprot_25p";
		
		CompetitiveEvaluation ce = new CompetitiveEvaluation(file, result_file);
		try {
			ce.execute();
			//ce.PrintTuples();
			ce.computeAgreetSets();
			//ce.printbitsetmap();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
