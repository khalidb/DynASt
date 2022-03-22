package fr.dauphine.lamsade.khalid.dynast;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import au.com.bytecode.opencsv.CSVReader;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
import de.metanome.algorithms.tane.algorithm_helper.FDTree;
import fr.dauphine.lamsade.khalid.dynast.util.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;

// @author Khalid Belhajjame



public class IncrementalFDep_old {
	
	
	/* file refers to the csv of the initial batch (without the extension ".csv"). For each iteration we may have 
	 * an inser and/or delete batch denoted by file_insert_i and file_delete_i, 
	 * where i refers to the iteration number. 
	 */
	String file = ""; 
	FdepAlgorithm_old1 algo = null;
 
	 
	IncrementalFDep_old(String file){
		
		this.file = file;
		this.algo = new FdepAlgorithm_old1();
	
		
		
	}
	
	public void execute() throws AlgorithmExecutionException {
		try {
			this.loadDataInitialBatch();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		 this.algo.setColumnIdentifiers();
		
		//System.out.print("Initialize BitSet Map");
		 
		this.algo.initializeScales(); 
		this.algo.initializeBitSetMap();
		this.algo.initializeInt2Bitset();
		
		
		this.algo.initializeEvidenceSets();
		this.algo.printEvidenceSets();

		
        System.out.println("Start constructing the negative cover");
        
        this.algo.negativeCover();
        
         

        this.algo.posCoverTree = new FDTree(this.algo.numberAttributes);
        this.algo.posCoverTree.addMostGeneralDependencies();
        BitSet activePath = new BitSet();
        this.algo.calculatePositiveCover(this.algo.negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        
        System.out.println("Print the dependencies of the positive cover");
        this.algo.posCoverTree.printDependencies();
        
        /* I removed the following instruction, I do not see the point, 
         * may be later I will need it.
         */
        //this.algo.addAllDependenciesToResultReceiver();
        
        
        
        //this.executeInsert(1);

        
        
        this.executedelete(2);
        
        
		
		
	}
	
	
	public void executedelete(int i) throws AlgorithmExecutionException {
		

		loadDeletedDataBatch(1);
		this.algo.deleteBitsets();
		this.algo.printBitSetMap_delete();
		
		// Need to update the indices to remove the mention of the deleted tuples.
		// This may be z problem as I am referring to the list b yindex 
		// Need to also update the evidence sets, in order to remove the elements corresponding to the deleted tuples.
		
		//I will have to figure out how to refer to the tuples in order to manage delete in a graceful manner.
		
		
	}
	
	public void executeInsert(int i) throws AlgorithmExecutionException {
		
		int n =this.algo.getTuples().size();
		this.algo.initializeBitSetMapInserion();
		System.out.println("Bitsetmap before insert");
		this.algo.printBitSetMap();
        System.out.println("1Evidence Sets before insert");
        this.algo.printEvidenceSets();
        System.out.println("#Indices before insert");
		this.algo.printIndices();
        System.out.println("1Load insert batch");
        this.loadInsertDataBatch(i);
        System.out.println("1Print evidence sets updated with the new insert");
        this.algo.printEvidenceSets();
        System.out.println("#Indices");
		this.algo.printIndices();
		
		System.out.println("Bitsetmap after insert");
		this.algo.printBitSetMap();
		System.out.println("Bitsetmap_insert");
		this.algo.printBitSetMap_insert();
	 	
		
        System.out.println("Print the dependencies of the positive cover before insertion");
        this.algo.posCoverTree.printDependencies();
		
		
		System.out.println("Negative dependencies before insertion");
		this.algo.getNegCoverTree().printDependencies();
		
		// Update the negative cover
		List<BitSet> new_bit_sets = new ArrayList<BitSet>();
		for (Map.Entry<BitSet,Integer> entry : this.algo.bitesetMap_insert.entrySet()) {
			
			if (entry.getValue() > 0) {
				if (this.algo.bitesetMap.get(entry.getKey())==0)
					new_bit_sets.add(entry.getKey());
			}
			
		}
		
		this.algo.updateNegativeCoverGivenInsertion(new_bit_sets);
		
		
		System.out.println("Negative dependencies after insertion");
		this.algo.getNegCoverTree().printDependencies();
		
        BitSet activePath = new BitSet();
        this.algo.calculatePositiveCover(this.algo.negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        
        System.out.println("Print the dependencies of the positive cover after insertion");
        this.algo.posCoverTree.printDependencies();
		
		
		
		this.algo.mergeBitSetMaps_insert();
		System.out.println("Bitsetmap after merge");
		this.algo.printBitSetMap();
		
		
	}
	
	
	
	
    private void loadDataInitialBatch() throws FileNotFoundException, IOException {
    	

    	this.algo.initializeIndexStructure();
    	
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+".csv"));) {
    	    String[] values = null;
    	    if ((values = csvReader.readNext()) != null) 
    	    	this.algo.setColumnNames(Arrays.asList(values))	;
    	    while ((values = csvReader.readNext()) != null) {  	
    	    	this.algo.addTuple(Arrays.asList(values).stream()
    	                .map(Integer::valueOf).collect(Collectors.toList()));
    	    }
    	}
    	
    	
    	this.algo.setTableName(file);
    	this.algo.setNumberAttributes(this.algo.getColumnNames().size());
    	
    	this.algo.printIndices();
    	
    	System.out.println("attribute names: "+Arrays.toString(this.algo.getColumnNames().toArray()));
    	System.out.println("number of tuples is "+this.algo.getTuples().size());
    	
    }
    
    private void loadInsertDataBatch(int i) {
    	
    	this.algo.inserted_tuples = null;
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+"_insert_"+i+".csv"));) {
    	    String[] values = null;
    	    this.algo.inserted_tuples = new ArrayList<List<Integer>>();
    	   // We assume that the file of the batch doesn't have a header 	
    	    while ((values = csvReader.readNext()) != null) {  	
    	    	this.algo.inserted_tuples.add(Arrays.asList(values).stream()
    	                .map(Integer::valueOf).collect(Collectors.toList()));
    	    }
        	System.out.println("number of inserted tuples is "+this.algo.inserted_tuples.size());
    	} catch (FileNotFoundException e) {
			System.out.println("No tuples to insert for batch "+i);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	int n = this.algo.getTuples().size();
    	
    	for(int j = 0; j < this.algo.inserted_tuples.size(); j++) {
    		for (int k = 0; k<this.algo.numberAttributes; k++) {
    			
    			if(this.algo.indices.get(k).containsKey(this.algo.inserted_tuples.get(j).get(k))) {
    				
    				this.algo.indices.get(k).get(this.algo.inserted_tuples.get(j).get(k)).add(n);
    				
    				Iterator value = algo.indices.get(k).get(this.algo.inserted_tuples.get(j).get(k)).iterator(); 
    				Integer ts = null;
    				
    				if (value.hasNext()) 
    		            ts = (Integer) value.next(); 
    				else
    					System.err.println("Found an index empty for the attribute value "+this.algo.inserted_tuples.get(j).get(k));
    				
    				Vector evn = this.algo.evidenceSets.get(k).get(ts).getClone();
    				
    				for (int l = 0; l< this.algo.getTuples().size(); l++) {
    					this.algo.evidenceSets.get(k).get(l).add(evn.get(l));
    				}
    				
    				evn.add(0);
    				this.algo.evidenceSets.get(k).add(evn);
    				
    				
    			}
    			
    			else {
    				Set<Integer> si = new HashSet<Integer>();
    				si.add(n);
    				this.algo.indices.get(k).put(this.algo.inserted_tuples.get(j).get(k), si);

    		 		
    				Vector evn = new Vector();
    				
    				for (int l = 0; l< this.algo.getTuples().size(); l++) {
    					evn.add(1);
    					this.algo.evidenceSets.get(k).get(l).add(1);
    				}
    				evn.add(0);
    				this.algo.evidenceSets.get(k).add(evn);
    				
    			}
    			
    		}

    		this.algo.getTuples().put(this.algo.generateID(),this.algo.inserted_tuples.get(j));
    		this.algo.insertBitsets(n);
    		n++;
    	}
    	
    	System.out.println("New total number of tuples: "+this.algo.getTuples().size());
    	

    	
    }
    
    private void loadDeletedDataBatch(int i)  {
    	
    	this.algo.deleted_tuples = null;
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+"_delete_"+i+".csv"));) {
    	    String[] values = null;
    	    this.algo.deleted_tuples = new ArrayList<Integer>();
    	    // We assume that the file of the batch doesn't have a header 	
    	    // There is only one line containing the Ids of the tuples to be deleted
    	    
    	    if ((values = csvReader.readNext()) != null) {  	
    	    	this.algo.deleted_tuples = Arrays.asList(values).stream()
    	               .map(Integer::valueOf).collect(Collectors.toList());
    	    }
    	    System.out.println("number of deleted tuples is "+this.algo.deleted_tuples.size());
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
    		System.out.println("No tuples to delete for batch "+i);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        for(Integer n: this.algo.deleted_tuples){
     	   System.out.println(n);
         }
    	
        System.out.println("############");
        /* Sorting in decreasing (descending) order*/
        Collections.sort(this.algo.deleted_tuples, Collections.reverseOrder());
        
        for(Integer n: this.algo.deleted_tuples){
        	   System.out.println(n);
        }

    	
    	
    	
    }
    

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String file = "resources/small_example";
		IncrementalFDep_old ifdep = new IncrementalFDep_old(file);
		try {
			ifdep.execute();
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		//ifdep.algo.printBitSetMap();
		
		//System.out.println("the first tuple: "+ifdep.algo.getTuples().get(0));
		//System.out.println("the value of the first attribute of the first tuple: "+ifdep.algo.getTuples().get(0).get(0));
		
		//ifdep.loadInsertDataBatch(1);
		//ifdep.loadDeletedDataBatch(1);
		
		
		/*
		int[] l1 = {1, 0, 1};
		int[] l2 = {1,0,1};
		int[] l3 = {1, 0, 1};
		int[] l4 = {0,1,1};
				
		Vector v1 = new Vector(l1);
		Vector v2 = new Vector(l2);
		Vector v3 = new Vector(l3);
		Vector v4 = new Vector(l4);
		
		int[] coeffs = {1,2,4,8};
		
		Vector v = v1.scale(coeffs[0]).plus(v2.scale(coeffs[1])).plus(v3.scale(coeffs[2])).plus(v4.scale(coeffs[3]));
		
		System.out.println(v);
		*/
		

	}

}
