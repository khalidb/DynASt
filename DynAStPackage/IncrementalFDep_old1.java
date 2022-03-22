package fr.dauphine.lamsade.khalid.dynast;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.Math;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

// @author Khalid Belhajjame



public class IncrementalFDep_old1 {
	
	
	int numberOfBatches = 0;
	/* file refers to the csv of the initial batch (without the extension ".csv"). For each iteration we may have 
	 * an inser and/or delete batch denoted by file_insert_i and file_delete_i, 
	 * where i refers to the iteration number. 
	 */
	String file = ""; 
	FdepAlgorithm_old1 algo = null;
 
	 
	IncrementalFDep_old1(String file, int _numberOfBatches){
		
		this.file = file;
		this.numberOfBatches = _numberOfBatches;
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
		//this.algo.printEvidenceSets();

		
        //System.out.println("Start constructing the negative cover");
        
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
        
        
        
        Instant start; 
        
        Instant end;
        
        List<Long> insert_durations = new ArrayList<Long>();
        List<Duration> delete_durations = new ArrayList<Duration>();
        
        for (int i =1; i<= this.numberOfBatches; i++) {
        	System.out.println("###### Batch "+i+" ###############");
        	System.out.println("------INSERT-----");
        	if ((new File(this.file+"_insert_"+i+".csv")).exists()) {
        		start = Instant.now();
        		this.executeInsert(i);
        		end = Instant.now();
        		insert_durations.add(Duration.between(start, end).toMillis());
        	}
        	System.out.println("------DELETE-----");
        	if ((new File(this.file+"_delete_"+i+".csv")).exists()) {
        		start = Instant.now();
        		this.executedelete(i);
        		end = Instant.now();
        		delete_durations.add(Duration.between(start, end));
        	}
        }
        
        System.out.println("Print the dependencies of the positive cover");
        this.algo.posCoverTree.printDependencies();
        
        System.out.println("###############  INSERT EXECUTION TIME ############");
        for (int i=0;i<insert_durations.size();i++)
        	System.out.println("Batch "+(i+1)+" took "+insert_durations.get(i));
        
        /*
        System.out.println("###############  DELETE EXECUTION TIME ############");
        for (int i=0;i<delete_durations.size();i++)
        	System.out.println("Batch "+(i+1)+" took "+delete_durations.get(i));
        	*/
		
		
	}
	
	
	public void executedelete(int i) throws AlgorithmExecutionException {
		

		loadDeletedDataBatch(i);
		this.algo.bitesetMap_delete = new HashMap<BitSet,Integer>();
		this.algo.deleteBitsets();
		//this.algo.printBitSetMap_delete();
		this.algo.updateEvidenceGivenDeletion();
		//this.algo.printEvidenceSets();
		
		int tmp;
		// Update the negative cover
		List<BitSet> new_bit_sets = new ArrayList<BitSet>();
		for (Map.Entry<BitSet,Integer> entry : this.algo.bitesetMap.entrySet()) {
			if (entry.getValue() > 0) {
				if (this.algo.bitesetMap_delete.containsKey(entry.getKey()) && (this.algo.bitesetMap_delete.get(entry.getKey()) >0)) {
					tmp = entry.getValue() -this.algo.bitesetMap_delete.get(entry.getKey());
					entry.setValue(tmp);
					if (tmp>0) new_bit_sets.add(entry.getKey());
				}
				else
					new_bit_sets.add(entry.getKey());
			}
		}
		 
		//System.out.println("List of New-Bit-Sets after deletion");
		//System.out.println(Arrays.toString(new_bit_sets.toArray()));
		
		this.algo.updateNegativeCoverGivenDeletion(new_bit_sets);
		 
		//System.out.println("Negative dependencies after deletion");
		this.algo.getNegCoverTree().printDependencies();
		
		this.algo.posCoverTree = new FDTree(this.algo.numberAttributes);
		this.algo.posCoverTree.addMostGeneralDependencies();
		BitSet activePath = new BitSet();
        
        this.algo.calculatePositiveCover(this.algo.negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        
        System.out.println("Dependencies of the positive cover after deletion ");
        this.algo.posCoverTree.printDependencies();
		
		
		//System.out.println("Bitsetmap after delete");
		//this.algo.printBitSetMap();
		
		
		// Need to update the indices to remove the mention of the deleted tuples.
		// This may be z problem as I am referring to the list b yindex 
		// Need to also update the evidence sets, in order to remove the elements corresponding to the deleted tuples.
		
		//I will have to figure out how to refer to the tuples in order to manage delete in a graceful manner.
		
		
	}
	
	

	public void executeInsert(int i) throws AlgorithmExecutionException {
		

		this.algo.initializeBitSetMapInserion();
		//System.out.println("Bitsetmap before insert");
		//this.algo.printBitSetMap();
        //System.out.println("1Evidence Sets before insert");
        //this.algo.printEvidenceSets();
        //System.out.println("#Indices before insert");
		//this.algo.printIndices();
        
		
		//System.out.println("1Load insert batch");
        this.loadInsertDataBatch(i);
        //System.out.println("1Print evidence sets updated with the new insert");
        //this.algo.printEvidenceSets();
        //System.out.println("#Indices");
		//this.algo.printIndices();
		
		//System.out.println("Bitsetmap after insert");
		//this.algo.printBitSetMap();
		//System.out.println("Bitsetmap_insert");
		//this.algo.printBitSetMap_insert();
	 	
		
        //System.out.println("Print the dependencies of the positive cover before insertion");
        //this.algo.posCoverTree.printDependencies();
		
		
		//System.out.println("Negative dependencies before insertion");
		//this.algo.getNegCoverTree().printDependencies();
		
		// Update the negative cover
		List<BitSet> new_bit_sets = new ArrayList<BitSet>();
		for (Map.Entry<BitSet,Integer> entry : this.algo.bitesetMap_insert.entrySet()) {
			
			if (entry.getValue() > 0) {
				if (this.algo.bitesetMap.get(entry.getKey())==0)
					new_bit_sets.add(entry.getKey());
			}
			
		}
		
		this.algo.updateNegativeCoverGivenInsertion(new_bit_sets);
		
		
		//System.out.println("Negative dependencies after insertion");
		//this.algo.getNegCoverTree().printDependencies();
		
        BitSet activePath = new BitSet();
        this.algo.calculatePositiveCover(this.algo.negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        
        /*--
        System.out.println("Dependencies of the positive cover after insertion");
        this.algo.posCoverTree.printDependencies();
        */
		
		
		
		this.algo.mergeBitSetMaps_insert();
		//System.out.println("Bitsetmap after merge");
		//this.algo.printBitSetMap();
		
		
	}
	
	
	
	
    private void loadDataInitialBatch() throws FileNotFoundException, IOException {
    	
    	this.algo.setTableName(file);
    	
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+".csv"));) {
    	    String[] values = null;
    	    if ((values = csvReader.readNext()) != null) {
    	    	this.algo.setColumnNames(Arrays.asList(values))	;
    	    	this.algo.setNumberAttributes(this.algo.getColumnNames().size());
    	    	this.algo.initializeIndexStructure();    	
    	    }
    	    while ((values = csvReader.readNext()) != null) {  	
    	    	this.algo.addTuple(Arrays.asList(values).stream()
    	                .map(Integer::valueOf).collect(Collectors.toList()));
    	    }
    	}
    	
    	

    	
    	//this.algo.printIndices();
    	
    	//System.out.println("attribute names: "+Arrays.toString(this.algo.getColumnNames().toArray()));
    	//System.out.println("number of tuples is "+this.algo.getTuples().size());
    	
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
        	//System.out.println("number of inserted tuples is "+this.algo.inserted_tuples.size());
    	} catch (FileNotFoundException e) {
			System.out.println("No tuples to insert for batch "+i);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	//int n = this.algo.getTuples().size();
    	int tuple_id;
    	
    	for(int j = 0; j < this.algo.inserted_tuples.size(); j++) {
    		tuple_id = this.algo.generateID();
    		//this.algo.tuples.put(tuple_id,this.algo.inserted_tuples.get(j));
    		for (int k = 0; k<this.algo.numberAttributes; k++) {
    			
    			if(this.algo.indices.get(k).containsKey(this.algo.inserted_tuples.get(j).get(k))) {
    				
    				//System.out.print("attribute k="+k);
    				
    				
    				Iterator value = algo.indices.get(k).get(this.algo.inserted_tuples.get(j).get(k)).iterator(); 
    				Integer ts = null;
    					
    				
    				if (value.hasNext()) 
    		            ts = (Integer) value.next(); 
    				else
    					System.err.println("Found an index empty for the attribute value "+this.algo.inserted_tuples.get(j).get(k));
    				
    				//System.out.println(", and ts="+ts);
    			    //this.algo.printEvidenceSets();
    				
    				Vector evn = this.algo.evidenceSets.get(k).get(ts).getClone();
    				
    				int l =0;
    				for(Integer key: this.algo.getTuples().keySet()) {
    				//for (int l = 0; l< this.algo.getTuples().size(); l++) {
    					//System.out.println("key: "+key+", an l = "+l);
    					this.algo.evidenceSets.get(k).get(key).add(evn.get(l++));
    				
    				this.algo.indices.get(k).get(this.algo.inserted_tuples.get(j).get(k)).add(tuple_id);	
    				}
    				
    				evn.add(0);
    				this.algo.evidenceSets.get(k).put(tuple_id,evn);
    				
    				
    			}
    			
    			else {
    				Set<Integer> si = new HashSet<Integer>();
    				si.add(tuple_id);
    				this.algo.indices.get(k).put(this.algo.inserted_tuples.get(j).get(k), si);

    		 		
    				Vector evn = new Vector();
    				
    				
    				for(Integer key: this.algo.getTuples().keySet()) {
    				//for (int l = 0; l< this.algo.getTuples().size(); l++) {
    					evn.add(1);
    					this.algo.evidenceSets.get(k).get(key).add(1);
    				}
    				evn.add(0);
    				this.algo.evidenceSets.get(k).put(tuple_id,evn);
    				
    			}
    			
    		}

    		this.algo.tuples.put(tuple_id,this.algo.inserted_tuples.get(j));
    		this.algo.insertBitsets(tuple_id);
    		//n++;
    	}
    	
    	//System.out.println("New total number of tuples: "+this.algo.getTuples().size());
    	

    	
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
    	
        for(Integer key_d: this.algo.deleted_tuples){
     	    // update the index
            for (int j=0; j< this.algo.numberAttributes; j++) {
            	this.algo.indices.get(j).get(this.algo.getTuples().get(key_d).get(j)).remove(key_d);
            }
            //remove the tuple
        	this.algo.getTuples().remove(key_d);
     	   
         }
    	
        //System.out.println("############");
        /* Sorting in decreasing (descending) order*/
        Collections.sort(this.algo.deleted_tuples, Collections.reverseOrder());
        
        for(Integer n: this.algo.deleted_tuples){
        	   System.out.println(n);
        }

    	
    	
    	
    }
    

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//String file = "resources/iris-num";
		//String file = "resources/num_bridges";
		String file = "resources/num_dataset"; // This is actually the adult dataset
		
		int number_of_batches = 10;
		IncrementalFDep_old1 ifdep = new IncrementalFDep_old1(file,number_of_batches);
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
