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
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import au.com.bytecode.opencsv.CSVReader;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
import fr.dauphine.lamsade.khalid.dynast.util.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Math;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;

import org.mp.naumann.algorithms.fd.structures.ASTree;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.apache.lucene.util.OpenBitSet;

// @author Khalid Belhajjame

 

public class IncrementalAgreeSetsMaintenance {
	 
	ASTree astree;
	String result_file;
	int numberAttributes;
	int numberOfBatches = 0;
	List<List<Long>> insert_dataLines = new ArrayList<>();
	List<List<Long>>  delete_dataLines = new ArrayList<>();
	String insert_header = "Load, Inserted bitset computation, Update ASTree, Number of evidence vectors";
	String delete_header = "Load, Sort, Deleted bitset computation, Update ASTree, Prune ASTree";
	/* file refers to the csv of the initial batch (without the extension ".csv"). For each iteration we may have 
	 * an inser and/or delete batch denoted by file_insert_i and file_delete_i, 
	 * where i refers to the iteration number. 
	 */
	String file = ""; 
	FDEP_AS algo = null;
	//int op_ID;
 
	 
	IncrementalAgreeSetsMaintenance(String file, int _numberOfBatches, String result_file){
		
		this.file = file;
		this.numberOfBatches = _numberOfBatches;
		this.algo = new FDEP_AS(this.numberAttributes ,new ValueComparator(true));
		this.algo.op_ID = 0;
		this.result_file = result_file;
 
	}
	
	public void execute() throws AlgorithmExecutionException {
		
		File insert_OutputFile = new File("Results/insert_"+this.result_file);
		File delete_OutputFile = new File("Results/delete_"+this.result_file);
		
		try {
			this.loadDataInitialBatch();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		this.algo.setColumnIdentifiers();	
		
		this.algo.initializeEvidenceSets();
		
        this.algo.negativeCover();
        
		this.astree = new ASTree(this.algo.numberAttributes);

		
        
        //System.out.println("Print BitSetMap");
		//this.algo.printBitSetMap();
        
		
        this.astree.addAgreeSets(this.algo.bitesetMap);
        
		//System.out.println("print ASTree");
		//this.astree.printTree();
		
        System.out.print("Number of agree sets: ");
        System.out.println(this.algo.bitesetMap.size());
        
        
        System.out.println("Number of evidence sets: "+this.algo.getNumberOfEvidenceSets());
        
          

        //***this.algo.posCoverTree =  this.algo.calculatePositiveCover(this.algo.negCoverTree);
//		posCoverTree.filterGeneralizations();
        
        
        // System.out.println("Negative cover");
		// this.algo.negCoverTree.printDependencies(this.numberAttributes);
         //***System.out.println("Print the dependencies of the positive cover");
         //***this.algo.posCoverTree.printDependencies(this.numberAttributes);
         
         
         //System.out.println("Evidence sets");
         //this.algo.printEvidenceSets();
        
        /* I removed the following instruction, I do not see the point, 
         * may be later I will need it.
         */
        //this.algo.addAllDependenciesToResultReceiver();
        
       //***System.out.println("Print bit set MAP");
       //***this.algo.printBitSetMap();
        
        Instant start; 
        
        Instant end;
        
        List<Long> insert_durations = new ArrayList<Long>();
        List<Long> delete_durations = new ArrayList<Long>();
       
    	

        
        for (int i =1; i<= this.numberOfBatches; i++) {
        	System.out.println("###### Batch "+i+" ###############");
        	System.out.println("------DELETE-----");
        	if ((new File(this.file+"_delete_"+i+".csv")).exists()) {
        		start = Instant.now();
        		this.delete_dataLines.add(this.executedelete_New(i));
        		end = Instant.now();
        		delete_durations.add(Duration.between(start, end).toMillis());
        		
        		System.out.println("delete_dataLine");
        		delete_dataLines.forEach(System.out::println);
        		
        		
                /*
        		System.out.println("Negative cover");
       		    this.algo.negCoverTree.printDependencies(this.numberAttributes);
       		    System.out.println("Positive cover");
        		this.algo.posCoverTree.printDependencies(this.numberAttributes);
        		*/
        		//***this.algo.printBitSetMap();
        		System.out.println("astree after deletion");
        		this.astree.printTree();
        	}
        	
        	System.out.println("------INSERT-----");
        	List<Long> result_line;
        	int num_res1, num_res2;
        	if ((new File(this.file+"_insert_"+i+".csv")).exists()) {
        			
        			num_res1 = this.algo.getNumberOfEvidenceSets();
        			System.out.println("inside loop for insertion: "+i);
                    start = Instant.now();
            		result_line = this.executeInsert_New(i);
            		//result_line = Arrays.toString(this.executeInsert_New(i).toArray());
            		end = Instant.now();
            		num_res2 = this.algo.getNumberOfEvidenceSets();
            		result_line.add((long) (num_res2 - num_res1));
            		this.insert_dataLines.add(result_line);
            		//result_line = result_line.substring(1,result_line.length()-1)+"\n";
            		//pw.write(result_line);
            		//System.out.println("Batch: "+i+", times "+result_line);
            		insert_durations.add(Duration.between(start, end).toMillis());
            		//System.out.println("astree after insertion");
            		//this.astree.printTree();
        		//System.out.println("Duration: "+Duration.between(start, end).toMillis());
                
        		//***System.out.println("Negative cover");
        		//***this.algo.negCoverTree.printDependencies(this.numberAttributes);
        		//***System.out.println("Positive cover");
        		//***this.algo.posCoverTree.printDependencies(this.numberAttributes);
            		
        		} 
        		
        	
        }

         
        //System.out.println("Print the dependencies of the positive cover");
        //this.algo.posCoverTree.printDependencies();
        
        if(insert_durations.size()> 0) {
        	System.out.println("###############  INSERT EXECUTION TIME ############");
        	for (int i=0;i<insert_durations.size();i++)
        		System.out.println("Batch "+(i+1)+" took "+insert_durations.get(i));
        
        }
        if(delete_durations.size()> 0) {
        	System.out.println("###############  DELETE EXECUTION TIME ############");
        	for (int i=0;i<delete_durations.size();i++)
        		System.out.println("Batch "+(i+1)+" took "+delete_durations.get(i));
        }
        
        
        if (insert_dataLines.size() > 0) {
        	try (PrintWriter pw = new PrintWriter(insert_OutputFile)) {
        		pw.println(insert_header);
        		insert_dataLines.stream()
        		.map(this::convertToCSV)
        		.forEach(pw::println);
        	} catch (FileNotFoundException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        	insert_OutputFile.exists();
        }
        
        if (delete_dataLines.size() > 0) {
        	try (PrintWriter pw = new PrintWriter(delete_OutputFile)) {
        		pw.println(delete_header);
        		delete_dataLines.stream()
        			.map(this::convertToCSV)
        			.forEach(pw::println);
        	} catch (FileNotFoundException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        	delete_OutputFile.exists();
        }
        
        
        
	//this.algo.posCoverTree.printDependencies();
        
	}
	
public void execute_adapted_for_deletion(int batch_size) throws AlgorithmExecutionException {
		
		File delete_OutputFile = new File("Results/delete_"+this.result_file);
		
		try {
			this.loadDataInitialBatch();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		this.algo.setColumnIdentifiers();	
		
		this.algo.initializeEvidenceSets();
		
        this.algo.negativeCover();
        
		this.astree = new ASTree(this.algo.numberAttributes);
   
		
        this.astree.addAgreeSets(this.algo.bitesetMap);
        

		
        System.out.print("Number of agree sets: ");
        System.out.println(this.algo.bitesetMap.size());
        
        
        //this.astree.printTree();
        
        
        
        System.out.println("Number of evidence sets: "+this.algo.getNumberOfEvidenceSets());
        
        System.out.println("Number of tuples: "+this.algo.getTuples().keySet().size());
        System.out.println("min ID: "+Collections.min(this.algo.getTuples().keySet())); 
        System.out.println("max ID: "+Collections.max(this.algo.getTuples().keySet()));  

        
        List<Long> delete_durations = new ArrayList<Long>();
        int current_max = this.algo.getTuples().keySet().size();
        int current_min = current_max - batch_size;
        Instant start; 
        
        Instant end;
        
        while (current_min >= 0) {
        	System.out.println("New batch");
    		//start = Instant.now();
    		this.delete_dataLines.add(this.executedelete_New(current_min, current_max));
    		//end = Instant.now();
    		//delete_durations.add(Duration.between(start, end).toMillis());
    		
    		//System.out.println("delete_dataLine");
    		//delete_dataLines.forEach(System.out::println);
    		
    		//System.out.println("astree after deletion");
    		//this.astree.printTree();
    		
    		current_max = current_max - batch_size;
    		current_min = current_min - batch_size;
    		break;
        }
        
        if (delete_dataLines.size() > 0) {
        	try (PrintWriter pw = new PrintWriter(delete_OutputFile)) {
        		pw.println(delete_header);
        		delete_dataLines.stream()
        			.map(this::convertToCSV)
        			.forEach(pw::println);
        	} catch (FileNotFoundException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        	delete_OutputFile.exists();
        }
        
	
        
	}

public void execute_adapted_for_deletion1() throws AlgorithmExecutionException {
	
	File delete_OutputFile = new File("Results/delete_"+this.result_file);
	
	try {
		this.loadDataInitialBatch();
	} catch (FileNotFoundException e) {
		e.printStackTrace();
	} catch (IOException e) {
		e.printStackTrace();
	}
	
	
	this.algo.setColumnIdentifiers();	
	
	this.algo.initializeEvidenceSets();
	
    this.algo.negativeCover();
    
	this.astree = new ASTree(this.algo.numberAttributes);

	
    this.astree.addAgreeSets(this.algo.bitesetMap);
    

	
    System.out.print("Number of agree sets: ");
    System.out.println(this.algo.bitesetMap.size());
    
    
    //this.astree.printTree();
    
    
    
    System.out.println("Number of evidence sets: "+this.algo.getNumberOfEvidenceSets());
    
    System.out.println("Number of tuples: "+this.algo.getTuples().keySet().size());
    System.out.println("min ID: "+Collections.min(this.algo.getTuples().keySet())); 
    System.out.println("max ID: "+Collections.max(this.algo.getTuples().keySet()));  

    int batch_size = 10;
    List<Long> delete_durations = new ArrayList<Long>();
    int current_max = this.algo.getTuples().keySet().size();
    int current_min = current_max - batch_size;
    
    System.out.println("Processing delete of a batch of size "+batch_size);
    this.delete_dataLines.add(this.executedelete_New(current_min, current_max));
	current_max = current_min;

	batch_size = 50;
	System.out.println("Processing delete of a batch of size "+batch_size);
	current_min = current_min - batch_size;
	this.delete_dataLines.add(this.executedelete_New(current_min, current_max));
	current_max = current_min;
	
	batch_size = 100;
	System.out.println("Processing delete of a batch of size "+batch_size);
	current_min = current_min - batch_size;
	this.delete_dataLines.add(this.executedelete_New(current_min, current_max));
	current_max = current_min;
	
	batch_size = 500;
	System.out.println("Processing delete of a batch of size "+batch_size);
	current_min = current_min - batch_size;
	this.delete_dataLines.add(this.executedelete_New(current_min, current_max));
	current_max = current_min;
	
	batch_size = 1000;
	System.out.println("Processing delete of a batch of size "+batch_size);
	current_min = current_min - batch_size;
	this.delete_dataLines.add(this.executedelete_New(current_min, current_max));
	current_max = current_min;
    
    if (delete_dataLines.size() > 0) {
    	try (PrintWriter pw = new PrintWriter(delete_OutputFile)) {
    		pw.println(delete_header);
    		delete_dataLines.stream()
    			.map(this::convertToCSV)
    			.forEach(pw::println);
    	} catch (FileNotFoundException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    	delete_OutputFile.exists();
    }
    

    
}
	
	public String convertToCSV(List<Long> data) {
		String line = "";
		String sep = " , ";
		
		for (int i = 0; i< data.size();i++) {
			line = line + data.get(i);
			if (i != data.size() - 1)
				line = line + sep;
		}
		return line;	
	}



	public List<Long> executedelete_New(int i) throws AlgorithmExecutionException {
		
		List<Long> delete_duration;
		Instant start, end;
		int att_value;
		
		delete_duration= loadDeletedDataBatch(i);
		//this.algo.bitesetMap_delete = new HashMap<OpenBitSet,Integer>();
		
		
		//***start = Instant.now();
		this.algo.deleteBitsets_New(delete_duration);
        for(Integer key_d: this.algo.deleted_tuples){
     	    // update the index
            for (int j=0; j< this.algo.numberAttributes; j++) {
            	
            	att_value = this.algo.getTuples().get(key_d).get(j);
            	this.algo.indices.get(j).get(att_value).remove(key_d);
            	
            	if (this.algo.indices.get(j).get(att_value).size() == 0) {
            		this.algo.indices.get(j).remove(att_value);
            		this.algo.AVES.get(j).remove(att_value);
            	}
            		
            }
            //remove the tuple
        	this.algo.getTuples().remove(key_d);
     	   
         }
		//***end = Instant.now();
		//**delete_duration.add(Duration.between(start, end).toMillis());
        
        
        
		
        start = Instant.now();
		this.astree.removeAgreeSets(this.algo.bitsets_cardinality_reduced);
		end = Instant.now();
		delete_duration.add(Duration.between(start, end).toMillis());
		
		
        start = Instant.now();
		this.astree.pruneASTree();
		end = Instant.now();
		delete_duration.add(Duration.between(start, end).toMillis());
		
		//this.algo.printBitSetMap_delete();
		
		//this.algo.printEvidenceSets();
		

		
		
		//***if (deleted_bitsets.size()>0) {
		//***	start = Instant.now();
		//***	int tmp;
		//***
		
		//***	this.algo.removeViolatedFdsFromCover(deleted_bitsets);
			
		//*** end = Instant.now();
		//*** delete_duration.add(Duration.between(start, end).toMillis());
		//***
		
		//***	Instant start_1, end_1, start_2, end_2;
		
		//***	start_1 = Instant.now();

			
			//System.out.println("Bitsets to be removed from the negative cover");
			//for(int l=0;l<deleted_bitsets.size();l++)
			//	System.out.println(BitSetUtils.toString(deleted_bitsets.get(l), this.numberAttributes));
		
			//this.algo.updateNegativeCoverGivenDeletion(deleted_bitsets);
			//this.algo.updateNegativeCoverGivenDeletion_old(new_bit_sets);
		
			//System.out.println("Dependencies of the negative cover");
			//this.algo.negCoverTree.printDependencies(this.numberAttributes);
		
		//***end_1 = Instant.now();
		//***	delete_duration.add(Duration.between(start_1, end_1).toMillis());
			//System.out.println("Time required for computing the negative cover: "+Duration.between(start_1, end_1).toMillis());
		 
			//System.out.println("Negative dependencies after deletion");
			//this.algo.getNegCoverTree().printDependencies();
		//***	start_2 = Instant.now();
        
		//***	end_2 = Instant.now();
		//***	delete_duration.add(Duration.between(start_2, end_2).toMillis());
			//System.out.println("Time required computing the positive cover: "+Duration.between(start_2, end_2).toMillis());
        
			//		posCoverTree.filterGeneralizations();
        
		//***} 
		//***else {
		//***	//System.out.println("The negative and positive cover did not need to be updated");
		//***	delete_duration.add((long) 0);
		//***	delete_duration.add((long) 0);
		//***	delete_duration.add((long) 0);
		//***}

		
			//System.out.println("Dependencies of the positive cover after deletion ");
			//this.algo.posCoverTree.printDependencies(this.numberAttributes);
			//this.algo.posCoverTree.printDependencies(this.numberAttributes);
        
        // Print tuples
        //System.out.println("Number of tuples: "+this.algo.getTuples().size());
        //for (Integer key : this.algo.getTuples().keySet()) {
        //	System.out.print(Arrays.toString(this.algo.getTuples().get(key).toArray()));
        //}
        	
		
		
		//System.out.println("Bitsetmap after delete");
		//this.algo.printBitSetMap();
		
		
		// Need to update the indices to remove the mention of the deleted tuples.
		// This may be z problem as I am referring to the list b yindex 
		// Need to also update the evidence sets, in order to remove the elements corresponding to the deleted tuples.
		
		//I will have to figure out how to refer to the tuples in order to manage delete in a graceful manner.
		
		
		//***Long sum = (long) 0;
		//***for (int l =0; l< delete_duration.size();l++)
		//***	sum += delete_duration.get(l);
		//***delete_duration.add(sum);
		//***delete_duration.add(sum - delete_duration.get(0));
		

		return delete_duration;
		
	}
	
	
	public List<Long> executedelete_New(int min, int max) throws AlgorithmExecutionException {
		
		List<Long> delete_duration;
		Instant start, end;
		int att_value;
		
		delete_duration= loadDeletedDataBatch(min, max);
		//this.algo.bitesetMap_delete = new HashMap<OpenBitSet,Integer>();
		
		
		
		this.algo.deleteBitsets_New(delete_duration);
		
		start = Instant.now();
        for(Integer key_d: this.algo.deleted_tuples){
     	    // update the index
        	
            for (int j=0; j< this.algo.numberAttributes; j++) {
            	
            	att_value = this.algo.getTuples().get(key_d).get(j);
            	this.algo.indices.get(j).get(att_value).remove(key_d);
            	
            	if (this.algo.indices.get(j).get(att_value).size() == 0) {
            		this.algo.indices.get(j).remove(att_value);
            		this.algo.AVES.get(j).remove(att_value);
            	}
            		
            }
            
            //remove the tuple
        	this.algo.getTuples().remove(key_d);
     	   
         }
		end = Instant.now();
		delete_duration.add(Duration.between(start, end).toMillis());
        
        System.out.println("Duration for the computation of bitsets to remove: "+ Duration.between(start, end).toMillis());
        
        OpenBitSet key;
        Integer old_val, new_val;
        List<OpenBitSet> as_to_remove = new ArrayList<OpenBitSet>();
        
        List<OpenBitSet> reduces_as = new ArrayList<OpenBitSet>();
        for (Map.Entry<OpenBitSet, Integer> entry : this.algo.bitsets_cardinality_reduced.entrySet()) {
        	//reduces_as.add(entry.getKey().clone());
        	key = entry.getKey();
        	old_val = this.algo.bitesetMap.get(key);
        	if (old_val != null) {
        		new_val = old_val - entry.getValue();
        		if (new_val <= 0)
        			as_to_remove.add(key);
        		this.algo.bitesetMap.replace(key, new_val);
        	}
        }
        
        System.out.println("Number of agree-sets the cardinality of which is reduced: "+this.algo.bitsets_cardinality_reduced.size());
        System.out.println("Number of agree-sets that need to be removed: "+as_to_remove.size());
        
        
        Collections.sort(reduces_as,new SortOpenBitSet());
		System.out.println("Number of agree sets subjects to cardinality reduction is "+reduces_as.size());
        
		start = Instant.now();
		this.astree.removeAgreeSets(this.algo.bitsets_cardinality_reduced);
        //this.astree.reduceAS(reduces_as, this.algo.bitsets_cardinality_reduced);
		//System.out.println("Size of bitsetls_cardinality_reduced: "+this.algo.bitsets_cardinality_reduced.size());
		end = Instant.now();
		
		delete_duration.add(Duration.between(start, end).toMillis());
		System.out.println("Duration for the reduction of agree sets cardinalaities: "+Duration.between(start, end).toMillis()+"ms");
		
        start = Instant.now();
		this.astree.pruneASTree();
		end = Instant.now();
		
		delete_duration.add(Duration.between(start, end).toMillis());
		
		//this.algo.printBitSetMap_delete();
		
		//this.algo.printEvidenceSets();	
		
		//***if (deleted_bitsets.size()>0) {
		//***	start = Instant.now();
		//***	int tmp;
		//***
		
		//***	this.algo.removeViolatedFdsFromCover(deleted_bitsets);
			
		//*** end = Instant.now();
		//*** delete_duration.add(Duration.between(start, end).toMillis());
		//***
		
		//***	Instant start_1, end_1, start_2, end_2;
		
		//***	start_1 = Instant.now();
			
			//System.out.println("Bitsets to be removed from the negative cover");
			//for(int l=0;l<deleted_bitsets.size();l++)
			//	System.out.println(BitSetUtils.toString(deleted_bitsets.get(l), this.numberAttributes));
		
			//this.algo.updateNegativeCoverGivenDeletion(deleted_bitsets);
			//this.algo.updateNegativeCoverGivenDeletion_old(new_bit_sets);
		
			//System.out.println("Dependencies of the negative cover");
			//this.algo.negCoverTree.printDependencies(this.numberAttributes);
		
		//***end_1 = Instant.now();
		//***	delete_duration.add(Duration.between(start_1, end_1).toMillis());
			//System.out.println("Time required for computing the negative cover: "+Duration.between(start_1, end_1).toMillis());
		 
			//System.out.println("Negative dependencies after deletion");
			//this.algo.getNegCoverTree().printDependencies();
		//***	start_2 = Instant.now();
        
		//***	end_2 = Instant.now();
		//***	delete_duration.add(Duration.between(start_2, end_2).toMillis());
			//System.out.println("Time required computing the positive cover: "+Duration.between(start_2, end_2).toMillis());
        
			//		posCoverTree.filterGeneralizations();
        
		//***} 
		//***else {
		//***	//System.out.println("The negative and positive cover did not need to be updated");
		//***	delete_duration.add((long) 0);
		//***	delete_duration.add((long) 0);
		//***	delete_duration.add((long) 0);
		//***}

		
			//System.out.println("Dependencies of the positive cover after deletion ");
			//this.algo.posCoverTree.printDependencies(this.numberAttributes);
			//this.algo.posCoverTree.printDependencies(this.numberAttributes);
        
        // Print tuples
        //System.out.println("Number of tuples: "+this.algo.getTuples().size());
        //for (Integer key : this.algo.getTuples().keySet()) {
        //	System.out.print(Arrays.toString(this.algo.getTuples().get(key).toArray()));
        //}
        	
		
		
		//System.out.println("Bitsetmap after delete");
		//this.algo.printBitSetMap();
		
		
		// Need to update the indices to remove the mention of the deleted tuples.
		// This may be z problem as I am referring to the list b yindex 
		// Need to also update the evidence sets, in order to remove the elements corresponding to the deleted tuples.
		
		//I will have to figure out how to refer to the tuples in order to manage delete in a graceful manner.
		
		
		//***Long sum = (long) 0;
		//***for (int l =0; l< delete_duration.size();l++)
		//***	sum += delete_duration.get(l);
		//***delete_duration.add(sum);
		//***delete_duration.add(sum - delete_duration.get(0));
		

		return delete_duration;
		
	}
	
	
	

	public List<Long> executeInsert(int i) throws AlgorithmExecutionException {
		
		List<Long> insert_duration;
		/* this.algo.initializeBitSetMapInserion();*/
		this.algo.bitesetMap_insert = new HashMap<OpenBitSet,Integer>();
		Instant start, end;

		
		//System.out.println("Bitsetmap before insert");
		//this.algo.printBitSetMap();
        //System.out.println("1Evidence Sets before insert");
        //this.algo.printEvidenceSets();
        //System.out.println("#Indices before insert");
		//this.algo.printIndices();
        
		
		//System.out.println("1Load insert batch");
        insert_duration = this.loadInsertDataBatch_New(i);
        
        //System.out.println("insert line after load: "+Arrays.toString(insert_duration.toArray()));
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
        start = Instant.now();
		List<OpenBitSet> new_bit_sets = new ArrayList<OpenBitSet>();

		for (Map.Entry<OpenBitSet,Integer> entry : this.algo.bitesetMap_insert.entrySet()) {
			
			/**** Optimization: I am not sure we need to test if the bitset is equal to 0. 
			 * Also, why is the second if condition useful for ? 
			 */
			
			if (entry.getValue() > 0) {
				if (!this.algo.bitesetMap.containsKey(entry.getKey()))
					new_bit_sets.add(entry.getKey());
			}
			
		}
		end = Instant.now();
		insert_duration.add(Duration.between(start, end).toMillis());
		
		
		if (new_bit_sets.size() > 0) {
			start = Instant.now();
			this.algo.updateNegativeCoverGivenInsertion(new_bit_sets);
			end = Instant.now();
			insert_duration.add(Duration.between(start, end).toMillis());
		
		
			//System.out.println("Negative dependencies after insertion");
			//this.algo.getNegCoverTree().printDependencies();
			start = Instant.now();
			this.algo.posCoverTree = 
					this.algo.calculatePositiveCover(this.algo.negCoverTree);
			end = Instant.now();
			insert_duration.add(Duration.between(start, end).toMillis());
			//		posCoverTree.filterGeneralizations();
        
			/*--
        	System.out.println("Dependencies of the positive cover after insertion");
        	this.algo.posCoverTree.printDependencies();
			 */
		}
		else {
			//System.out.println("The negative and positive covers did not need to be updated");
			insert_duration.add((long) 0);
			insert_duration.add((long) 0);
		}
		
		
		
		start = Instant.now();
		this.algo.mergeBitSetMaps_insert();
		////System.out.println("Bitsetmap after merge");
		//this.algo.printBitSetMap();
		end = Instant.now();
		insert_duration.add(Duration.between(start, end).toMillis());
		
		Long sum = (long) 0;
		for (int l =0; l< insert_duration.size();l++)
			sum += insert_duration.get(l);
		insert_duration.add(sum);
		insert_duration.add(sum - insert_duration.get(0));
		
		//System.out.println("insert line: "+Arrays.toString(insert_duration.toArray()));
		
		return insert_duration;
		
	}
	
	
	public List<Long> executeInsert_New(int i) throws AlgorithmExecutionException {
		
		List<Long> insert_duration;
		OpenBitSet key;
		//this.algo.initializeBitSetMapInserion();
		OpenBitSet diffAttrs, equalAttrs;
		Instant start, end;

		
		//System.out.println("Bitsetmap before insert");
		//this.algo.printBitSetMap();
        //System.out.println("1Evidence Sets before insert");
        //this.algo.printEvidenceSets();
        //System.out.println("#Indices before insert");
		//this.algo.printIndices();
        
		/*
		System.out.println("Tuples that exit prior to the insertion of the batch");
		for (Map.Entry<Integer,List<Integer>> entry : this.algo.getTuples().entrySet()) {
			System.out.print("Id: "+ entry.getKey());
			System.out.println(" tuple: "+Arrays.toString(entry.getValue().toArray()));
		}
		*/
		
		/*
		System.out.println("Bitset map before insertion");
		this.algo.printBitSetMap();
		*/
		//System.out.println("1Load insert batch");
        insert_duration = this.loadInsertDataBatch_New(i);
        
        //System.out.println("insert line after load: "+Arrays.toString(insert_duration.toArray()));
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
        start = Instant.now();
        //***List<OpenBitSet> new_bit_sets = new ArrayList<OpenBitSet>();
        //***for (Map.Entry<OpenBitSet,Integer> entry : this.algo.bitesetMap_insert.entrySet()) {
			
		//***	/**** Optimization: I am not sure we need to test if the bitset is equal to 0. 
		//***	 * Also, why is the second if condition useful for ? 
		//***	 */
		//***	key = entry.getKey();
		//***	if (!this.algo.bitesetMap.containsKey(key)) {
		//***		new_bit_sets.add(key);
		//***		//System.out.println("New open bit set given insertion: "+BitSetUtils.toString(key,this.algo.numberAttributes));
		//***	}
			//this.algo.bitesetMap.merge(key,this.algo.bitesetMap_insert.get(key), Integer::sum);
			
			
		//***}
		
		this.astree.addAgreeSets(this.algo.bitesetMap_insert);
		end = Instant.now();
		insert_duration.add(Duration.between(start, end).toMillis());
		
		
		
		
		
		//***Collections.sort(new_bit_sets,new SortOpenBitSet());
		
		//System.out.println("new_bit_sets sorted");
		//System.out.println(BitSetUtils.toString(new_bit_sets.get(0), this.algo.numberAttributes));
		//System.out.println(BitSetUtils.toString(new_bit_sets.get(new_bit_sets.size() - 1), this.algo.numberAttributes));
			
		
		//***if (new_bit_sets.size() > 0) {
		//***	start = Instant.now();
			
			
		//***    for (int k = 0; k< new_bit_sets.size();k++) {
		//***    	diffAttrs = new_bit_sets.get(k);
		//***    	equalAttrs = new OpenBitSet(numberAttributes);
		//***    	equalAttrs.set(0, this.numberAttributes);
		//***    	equalAttrs.andNot(diffAttrs);
		//***    	for (int j = 0; j<numberAttributes; j++) {
		//***    		if (diffAttrs.get(j)) {
		//***    			this.algo.negCoverTree.addFunctionalDependency(equalAttrs,j);
		//***    			this.algo.specializePositiveCover_New(this.algo.posCoverTree, equalAttrs, j);
		//***    		}
	        		
		//***   	}
	        	
		//***    }
			
		//***	end = Instant.now();
		//***	insert_duration.add(Duration.between(start, end).toMillis());
		
		
			//System.out.println("Negative dependencies after insertion");
			//this.algo.getNegCoverTree().printDependencies();
		//***start = Instant.now();
			// No nded to call calculate positive cover 
			//this.algo.posCoverTree = 
			//		this.algo.calculatePositiveCover(this.algo.negCoverTree);
		//***	end = Instant.now();
		//***	insert_duration.add(Duration.between(start, end).toMillis());
			//		posCoverTree.filterGeneralizations();
        
			/*--
        	System.out.println("Dependencies of the positive cover after insertion");
        	this.algo.posCoverTree.printDependencies();
			 */
		//***}
		//***else {
			//System.out.println("The negative and positive covers did not need to be updated");
		//***	insert_duration.add((long) 0);
		//***	insert_duration.add((long) 0);
		//***}
		
		//System.out.println("Bitset map after insertion");
		//this.algo.printBitSetMap();
		
		//***start = Instant.now();
		//***this.algo.mergeBitSetMaps_insert();
		////System.out.println("Bitsetmap after merge");
		//this.algo.printBitSetMap();
		//***end = Instant.now();
		//***insert_duration.add(Duration.between(start, end).toMillis());
		
		//***Long sum = (long) 0;
		//***for (int l =0; l< insert_duration.size();l++)
		//***	sum += insert_duration.get(l);
		//***insert_duration.add(sum);
		//***insert_duration.add(sum - insert_duration.get(0));
		
		//System.out.println("insert line: "+Arrays.toString(insert_duration.toArray()));
		
		return insert_duration;
		
	}
	
	public void printEvidenceSetsSize() {
		
		for (int i=0;i<this.numberOfBatches;i++) {
			System.out.println("Number of vectros for attribute "+i+" is "+this.algo.evidenceSets.get(i).size());
		    System.out.println("Each vector of size "+this.algo.evidenceSets.get(i).get(0).getVector().size()) ;
		}
	}
	
    private void loadDataInitialBatch() throws FileNotFoundException, IOException {
    	
    	this.algo.setTableName(file);
    	
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+".csv"));) {
    	    String[] values = null;
    	    if ((values = csvReader.readNext()) != null) {
    	    	this.algo.setColumnNames(Arrays.asList(values))	;
    	    	this.algo.setNumberAttributes(this.algo.getColumnNames().size());
    	    	this.numberAttributes = this.algo.getColumnNames().size();
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
    
    private List<Long> loadInsertDataBatch(int i) {
    	
    	List<Long> load_durations = new ArrayList<Long>();
    	//System.out.println("Load duration at initialization: "+Arrays.toString(load_durations.toArray()));

    	Instant start, end;
    	this.algo.inserted_tuples = null;
    	
    	start = Instant.now();
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
    	end = Instant.now();
    	long d = Duration.between(start, end).toMillis();
    	//System.out.println("First element of load duration: "+d);
    	
    	load_durations.add(d);
    	//System.out.println("Load duration after adding the first element: "+Arrays.toString(load_durations.toArray()));

    	//int n = this.algo.getTuples().size();
    	int tuple_id;
    	
    	start = Instant.now();
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
    				
    				DiffVector evn = this.algo.evidenceSets.get(k).get(ts);
    				
    				int l =0;
    				int curr_op = ++this.algo.op_ID;
    				for(Integer key: this.algo.getTuples().keySet()) {
    				//for (int l = 0; l< this.algo.getTuples().size(); l++) {
    					//System.out.println("key: "+key+", an l = "+l);
    					if (this.algo.evidenceSets.get(k).get(key).getLast_op_id() != curr_op) {
    						this.algo.evidenceSets.get(k).get(key).add(evn.get(l++));
    						this.algo.evidenceSets.get(k).get(key).setLast_op_id(curr_op);
    					}
    				
    				this.algo.indices.get(k).get(this.algo.inserted_tuples.get(j).get(k)).add(tuple_id);	
    				}
    				
    				//evn.add(false);
    				this.algo.evidenceSets.get(k).put(tuple_id,evn);
    				
    				
    			}
    			
    			else {
    				int curr_op = ++this.algo.op_ID;
    				Set<Integer> si = new HashSet<Integer>();
    				si.add(tuple_id);
    				this.algo.indices.get(k).put(this.algo.inserted_tuples.get(j).get(k), si);

    		 		
    				DiffVector evn = new DiffVector();
    				
    				
    				for(Integer key: this.algo.getTuples().keySet()) {
    				//for (int l = 0; l< this.algo.getTuples().size(); l++) {
    					evn.add(true);
    					if (this.algo.evidenceSets.get(k).get(key).getLast_op_id() != curr_op) {
    						this.algo.evidenceSets.get(k).get(key).add(true);
    						this.algo.evidenceSets.get(k).get(key).setLast_op_id(curr_op);
    					}
    				}
    				evn.add(false);
    				this.algo.evidenceSets.get(k).put(tuple_id,evn);
    				
    			}
    			
    		}
    		


    		this.algo.tuples.put(tuple_id,this.algo.inserted_tuples.get(j));
    		this.algo.insertBitsets(tuple_id);
    		//n++;
    	}
		end = Instant.now();
		load_durations.add(Duration.between(start, end).toMillis());
    	
    	//System.out.println("New total number of tuples: "+this.algo.getTuples().size());
    	
    	//System.out.println("Load duration at the end of the load sub-routine: "+Arrays.toString(load_durations.toArray()));

    	return load_durations;
    	
    }
    
    private List<Long> loadInsertDataBatch_New(int i) {
    	
    	List<Long> load_durations = new ArrayList<Long>();
    	this.algo.bitesetMap_insert = new HashMap<OpenBitSet,Integer>();
    	//System.out.println("Load duration at initialization: "+Arrays.toString(load_durations.toArray()));

    	Integer att_value;
    	int current_att_value_size;
    	Instant start, end;
    	this.algo.inserted_tuples = null;
    	
    	start = Instant.now();
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
    	end = Instant.now();
    	long d = Duration.between(start, end).toMillis();
    	//System.out.println("First element of load duration: "+d);
    	
    	load_durations.add(d);
    	//System.out.println("Load duration after adding the first element: "+Arrays.toString(load_durations.toArray()));

    	//int n = this.algo.getTuples().size();
    	int tuple_id;
    	Set<Integer> tuple_cluster;
    	
		current_att_value_size = this.algo.AVES.get(0).entrySet().iterator().next().getValue().getSize();
    	
    	start = Instant.now();
    	
    	//System.out.println("Size of bitset map insert: "+this.algo.bitesetMap_insert.size());
    	
    	for(int j = 0; j < this.algo.inserted_tuples.size(); j++) {
    		tuple_id = this.algo.generateID();
    		current_att_value_size++;
    		//this.algo.tuples.put(tuple_id,this.algo.inserted_tuples.get(j));
    		for (int k = 0; k<this.algo.numberAttributes; k++) {
    			
    			att_value = this.algo.inserted_tuples.get(j).get(k);
    			
    			// update the inverted attribute index IAIdx
				if (this.algo.indices.get(k).containsKey(att_value))
					this.algo.indices.get(k).get(att_value).add(tuple_id);
				else {
					tuple_cluster = new HashSet<Integer>();
					tuple_cluster.add(tuple_id);
					this.algo.indices.get(k).put(att_value, tuple_cluster);
				}
    			
    			
    			for(Integer key: this.algo.AVES.get(k).keySet()) {
    				this.algo.AVES.get(k).get(key).add(true);	
    			}
    			
    			
    			
    			if (this.algo.AVES.get(k).containsKey(att_value)) {
    				this.algo.AVES.get(k).get(att_value).setLast(false);
    			}
    			else {
    				
    				DiffVector vec = new DiffVector(current_att_value_size,true);
    				vec.setLast(false);
    				this.algo.AVES.get(k).put(att_value, vec);	
    			}
    			
        		
    		}
    		
    		this.algo.tuples.put(tuple_id,this.algo.inserted_tuples.get(j));
    		this.algo.insertBitsets_New(this.algo.inserted_tuples.get(j));
			
    	}
    	
 
		end = Instant.now();
		load_durations.add(Duration.between(start, end).toMillis());
    	
    	//System.out.println("New total number of tuples: "+this.algo.getTuples().size());
    	
    	//System.out.println("Load duration at the end of the load sub-routine: "+Arrays.toString(load_durations.toArray()));

    	return load_durations;
    	
    }
  
    
    private List<Long> loadDeletedDataBatch(int min, int max)  {
    	
    	List<Long> load_duration = new ArrayList<Long>();
    	
    	Instant start, end;
    	
    	
    	start = Instant.now();
    	
    	this.algo.deleted_tuples = new ArrayList<Integer>();
    	this.algo.deleted_tuples = IntStream.range(min, max).boxed(). collect(Collectors.toList());
    	

        end = Instant.now();
        load_duration.add(Duration.between(start, end).toMillis());
        
        
        // I am not sure that sorting is necessary any more //
        /* Sorting in decreasing (descending) order*/
        
        start = Instant.now();
        Collections.sort(this.algo.deleted_tuples, Collections.reverseOrder());
        end = Instant.now();
        load_duration.add(Duration.between(start, end).toMillis());
        
        //System.out.println("IDs of deleted tuples: "+Arrays.toString(this.algo.deleted_tuples.toArray()));

        
        /*
        for(Integer n: this.algo.deleted_tuples){
        	   System.out.println(n);
        }
        */

    	return load_duration;
    	
    	
    }
    
    private List<Long> loadDeletedDataBatch(int i)  {
    	
    	List<Long> load_duration = new ArrayList<Long>();
    	
    	Instant start, end;
    	
    	
    	start = Instant.now();
    	Integer att_value;
    	this.algo.deleted_tuples = null;
    	try (CSVReader csvReader = new CSVReader(new FileReader(this.file+"_delete_"+i+".csv"));) {
    	    String[] values = null;
    	    this.algo.deleted_tuples = new ArrayList<Integer>();
    	    // We assume that the file of the batch doesn't have a header 	
    	    // There is only one line containing the Ids of the tuples to be deleted
    	    
    	    if ((values = csvReader.readNext()) != null) {  	
    	    	this.algo.deleted_tuples = Arrays.asList(values).stream().map(Integer::valueOf).collect(Collectors.toList());
    	    }
    	    //System.out.println("number of deleted tuples is "+this.algo.deleted_tuples.size());
    	} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
    		System.out.println("No tuples to delete for batch "+i);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	

        end = Instant.now();
        load_duration.add(Duration.between(start, end).toMillis());
    	
        // I am not sure that sorting is necessary any more //
        /* Sorting in decreasing (descending) order*/
        
        start = Instant.now();
        Collections.sort(this.algo.deleted_tuples, Collections.reverseOrder());
        end = Instant.now();
        load_duration.add(Duration.between(start, end).toMillis());
        
        /*
        for(Integer n: this.algo.deleted_tuples){
        	   System.out.println(n);
        }
        */

    	return load_duration;
    	
    	
    }
    

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//String file = "resources/claims_all/claims_all";
		//String result_file = "claims_all";
		
		//String file = "resources/claims_50/claims_10/claims_10";
		//String result_file = "claims_10_insert_2nd_run";
		
		//String file = "resources/num_bridges_full";
		//String result_file = "num_bridges_10";
		
		//String file = "resources/flight_1000/num_flight_1k";
		//String result_file = "flight_150_run3";
		//int batch_size = 150;
		
		//String file = "resources/uniprot_150/num_uniprot";
		//String result_file = "uniprot_150_run3";
		//int batch_size = 150;
		
		
		
		//String file = "resources/adult_001/num_adult";
		//String result_file = "adult_100_percent_run3";
		//int batch_size = 32000;
		
		
		//String file = "resources/claims/90percent/claims";
		//String result_file = "claims_insert_90_percent";
		
		//String file = "resources/claims_50/claims_50/claims_50";
		//String result_file = "claims_50";
		
		//String file = "resources/num_bridges_full";
		//String result_file = "bridges_all";
		
		//String file = "resources/num_flight_400/num_flight";
		//String file = "resources/num_flight_50";
		//String result_file = "num_flight_1k";
		
		//String file = "resources/uniprot_10/uniprot_10";
		//String result_file = "uniprot_10_run3";
		
		//String file = "resources/num_flight_400/num_flight_400";
		//String result_file = "flight_insert_400";
		
		//String file = "resources/adult_50_insert/adult_50";
		//String result_file = "adult_insert_50";
		
		//String file = "resources/num_bridges";
		//String file = "resources/num_dataset"; // This is actually the adult dataset
		//String file = "resources/num_flight_1k";
		//String result_file = "num_flight_1k";
		//String file = "resources/num_flight_400/num_flight_400";
		//String file = "resources/num_flight_50";
		//String file = "resources/num_adult_50/num_adult_50";
		//String file = "resources/num_adult_10/num_adult_10";
		
		
		
	    //String file = "resources/num_bridges_full";
	    //String file = "resources/example/example1";
	    //String file = "resources/num_flight_delete/num_flight_1k_d";
		
		//String file = "resources/claims/10percent/claims";
		//String result_file = "claims_10_percent";
		
		//String file = "resources/single/99percent/single";
		//String result_file = "single_99_percent";
		
		
		 //Most recent one 

		 //String file = "resources/small_example2";
		 //String result_file = "small_example2";
		 
		 //String file = "resources/small_example";
		 //String result_file = "small_example";
		 
		 //String file = "resources/cpu/cpu_base";
		 //String result_file = "cpu_base";
		
		//String file = "resources/adult_deletion/adult_10p";
		//String result_file = "temp_adult_10p";
		
		String file = "resources/uniprot_150/num_uniprot";
		String result_file = "delete_uniprot_50";
	    //String file = "resources/flight_1000/num_flight_1k";
		//String result_file = "delete_flight_50";
		//String file = "resources/adult_1/num_adult";
		//String result_file = "delete_adult_75p";
		//String file = "resources/claims_1/claims";
		//String result_file = "delete_claims_75p";
		int batch_size = 50;
		
		int number_of_batches = 1;
		//int number_of_batches = 2;
		
		IncrementalAgreeSetsMaintenance ifdep = new IncrementalAgreeSetsMaintenance(file,number_of_batches,result_file);
		ifdep.numberOfBatches = 9561;
		try {
			//ifdep.execute();
			//ifdep.execute_adapted_for_deletion1();
			ifdep.execute_adapted_for_deletion(batch_size);
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}

}
