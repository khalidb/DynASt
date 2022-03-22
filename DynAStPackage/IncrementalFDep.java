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

import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;
import org.apache.lucene.util.OpenBitSet;

// @author Khalid Belhajjame

 

public class IncrementalFDep {
	 
	String result_file;
	int numberAttributes;
	int numberOfBatches = 0;
	List<List<Long>> insert_dataLines = new ArrayList<>();
	List<List<Long>>  delete_dataLines = new ArrayList<>();
	String insert_header = "Load, Evidence sets update, BitSetMap update, Negative cover update, Positive cover update, Merge BitSet map, Total, Total (without data loading)";
	String delete_header = "Load, Sort, Deleted bitset computation, New bitset computation, Negative cover update, Positive cover update, Total, Total (without data loading)";
	/* file refers to the csv of the initial batch (without the extension ".csv"). For each iteration we may have 
	 * an inser and/or delete batch denoted by file_insert_i and file_delete_i, 
	 * where i refers to the iteration number. 
	 */
	String file = ""; 
	FDEP algo = null;
	//int op_ID;
 
	 
	IncrementalFDep(String file, int _numberOfBatches, String result_file){
		
		this.file = file;
		this.numberOfBatches = _numberOfBatches;
		this.algo = new FDEP(this.numberAttributes ,new ValueComparator(true));
		this.algo.op_ID = 0;
		this.result_file = result_file;
 
	}
	
	public void execute() throws AlgorithmExecutionException {
		
		File insert_OutputFile = new File("Results/insert_"+this.result_file);
		File delete_OutputFile = new File("Results/delete_"+this.result_file);
		
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
		
		//this.algo.initializeScales(); 
		
		 
		/* this.algo.initializeBitSetMap(); */
		//this.algo.initializeInt2Bitset();
		
		//System.out.println("Start initializing evidence sets");
		//System.out.println("number of attributes in the main program: "+this.numberAttributes);
		
		
		this.algo.initializeEvidenceSets();
		//this.algo.printEvidenceSets();
		

		
		System.out.println("Construct the negative cover");
		
        //System.out.println("Start constructing the negative cover");
        //System.out.println("number of attributes: "+this.algo.numberAttributes);
        this.algo.negativeCover();
        
         

        this.algo.posCoverTree =  this.algo.calculatePositiveCover(this.algo.negCoverTree);
//		posCoverTree.filterGeneralizations();
        
        
        // System.out.println("Negative cover");
		// this.algo.negCoverTree.printDependencies(this.numberAttributes);
         System.out.println("Print the dependencies of the positive cover");
         this.algo.posCoverTree.printDependencies(this.numberAttributes);
         
         
         //System.out.println("Evidence sets");
         //this.algo.printEvidenceSets();
        
        /* I removed the following instruction, I do not see the point, 
         * may be later I will need it.
         */
        //this.algo.addAllDependenciesToResultReceiver();
        
        System.out.println("Print bit set MAP");
        this.algo.printBitSetMap();
        
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
                /*
        		System.out.println("Negative cover");
       		    this.algo.negCoverTree.printDependencies(this.numberAttributes);
       		    System.out.println("Positive cover");
        		this.algo.posCoverTree.printDependencies(this.numberAttributes);
        		*/
        		//this.algo.printBitSetMap();
        	}
        	System.out.println("------INSERT-----");
        	if ((new File(this.file+"_insert_"+i+".csv")).exists()) {
        		start = Instant.now();
        		this.insert_dataLines.add(this.executeInsert_New(i));
        		end = Instant.now();
        		insert_durations.add(Duration.between(start, end).toMillis());
        		//System.out.println("Duration: "+Duration.between(start, end).toMillis());
                
        		System.out.println("Negative cover");
       		    this.algo.negCoverTree.printDependencies(this.numberAttributes);
       		    System.out.println("Positive cover");
        		this.algo.posCoverTree.printDependencies(this.numberAttributes);
        		
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
	
	public List<Long> executedelete(int i) throws AlgorithmExecutionException {
		
		List<Long> delete_duration;
		Instant start, end;
		int att_value;
		
		delete_duration= loadDeletedDataBatch(i);
		//this.algo.bitesetMap_delete = new HashMap<OpenBitSet,Integer>();
		
		
		start = Instant.now();
		List<OpenBitSet> deleted_bitsets = this.algo.deleteBitsets_New();
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
		//this.algo.printBitSetMap_delete();
		
		//this.algo.printEvidenceSets();
		

		
		
		if (deleted_bitsets.size()>0) {
			start = Instant.now();
			int tmp;
			// Update the negative cover
			/* Removed this block for the incremental update of the negaztive cover tree */
			List<OpenBitSet> new_bit_sets = new ArrayList<OpenBitSet>();
			for (Map.Entry<OpenBitSet,Integer> entry : this.algo.bitesetMap.entrySet()) {
			
			//System.out.println("BitSetMap before deletion");
			//this.algo.printBitSetMap();

				if (this.algo.bitesetMap_delete.containsKey(entry.getKey()) && (this.algo.bitesetMap_delete.get(entry.getKey()) >0)) {
					tmp = entry.getValue() - this.algo.bitesetMap_delete.get(entry.getKey());
					entry.setValue(tmp);
					if (tmp>0) new_bit_sets.add(entry.getKey());
				}
				else
					new_bit_sets.add(entry.getKey());
			
			}
		
		 end = Instant.now();
		 delete_duration.add(Duration.between(start, end).toMillis());
		
			//System.out.println("List of New-Bit-Sets after deletion");
			//System.out.println(Arrays.toString(new_bit_sets.toArray()));
		
			Instant start_1, end_1, start_2, end_2;
		
			start_1 = Instant.now();


			//System.out.println("Bitsets to be removed from the negative cover");
			//for(int l=0;l<deleted_bitsets.size();l++)
			//	System.out.println(BitSetUtils.toString(deleted_bitsets.get(l), this.numberAttributes));
		
			//this.algo.updateNegativeCoverGivenDeletion(deleted_bitsets);
			this.algo.updateNegativeCoverGivenDeletion_old(new_bit_sets);
		
			//System.out.println("Dependencies of the negative cover");
			//this.algo.negCoverTree.printDependencies(this.numberAttributes);
		
			end_1 = Instant.now();
			delete_duration.add(Duration.between(start_1, end_1).toMillis());
			//System.out.println("Time required for computing the negative cover: "+Duration.between(start_1, end_1).toMillis());
		 
			//System.out.println("Negative dependencies after deletion");
			//this.algo.getNegCoverTree().printDependencies();
			start_2 = Instant.now();
			this.algo.posCoverTree = this.algo.calculatePositiveCover(this.algo.negCoverTree);
        
			end_2 = Instant.now();
			delete_duration.add(Duration.between(start_2, end_2).toMillis());
			//System.out.println("Time required computing the positive cover: "+Duration.between(start_2, end_2).toMillis());
        
			//		posCoverTree.filterGeneralizations();
        
		} 
		else {
			//System.out.println("The negative and positive cover did not need to be updated");
			delete_duration.add((long) 0);
			delete_duration.add((long) 0);
			delete_duration.add((long) 0);
		}

		
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
		
		Long sum = (long) 0;
		for (int l =0; l< delete_duration.size();l++)
			sum += delete_duration.get(l);
		delete_duration.add(sum);
		delete_duration.add(sum - delete_duration.get(0));

		
		return delete_duration;
		
	}
	
	
	public List<Long> executedelete_New(int i) throws AlgorithmExecutionException {
		
		List<Long> delete_duration;
		Instant start, end;
		int att_value;
		
		delete_duration= loadDeletedDataBatch(i);
		//this.algo.bitesetMap_delete = new HashMap<OpenBitSet,Integer>();
		
		
		start = Instant.now();
		ArrayList<OpenBitSet> deleted_bitsets = this.algo.deleteBitsets_New();
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
		//this.algo.printBitSetMap_delete();
		
		//this.algo.printEvidenceSets();
		

		
		
		if (deleted_bitsets.size()>0) {
			start = Instant.now();
			int tmp;

		
			this.algo.removeViolatedFdsFromCover(deleted_bitsets);
			
		 end = Instant.now();
		 delete_duration.add(Duration.between(start, end).toMillis());

		
			Instant start_1, end_1, start_2, end_2;
		
			start_1 = Instant.now();

			
			//System.out.println("Bitsets to be removed from the negative cover");
			//for(int l=0;l<deleted_bitsets.size();l++)
			//	System.out.println(BitSetUtils.toString(deleted_bitsets.get(l), this.numberAttributes));
		
			//this.algo.updateNegativeCoverGivenDeletion(deleted_bitsets);
			//this.algo.updateNegativeCoverGivenDeletion_old(new_bit_sets);
		
			//System.out.println("Dependencies of the negative cover");
			//this.algo.negCoverTree.printDependencies(this.numberAttributes);
		
			end_1 = Instant.now();
			delete_duration.add(Duration.between(start_1, end_1).toMillis());
			//System.out.println("Time required for computing the negative cover: "+Duration.between(start_1, end_1).toMillis());
		 
			//System.out.println("Negative dependencies after deletion");
			//this.algo.getNegCoverTree().printDependencies();
			start_2 = Instant.now();
        
			end_2 = Instant.now();
			delete_duration.add(Duration.between(start_2, end_2).toMillis());
			//System.out.println("Time required computing the positive cover: "+Duration.between(start_2, end_2).toMillis());
        
			//		posCoverTree.filterGeneralizations();
        
		} 
		else {
			//System.out.println("The negative and positive cover did not need to be updated");
			delete_duration.add((long) 0);
			delete_duration.add((long) 0);
			delete_duration.add((long) 0);
		}

		
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
		
		Long sum = (long) 0;
		for (int l =0; l< delete_duration.size();l++)
			sum += delete_duration.get(l);
		delete_duration.add(sum);
		delete_duration.add(sum - delete_duration.get(0));

		
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
		List<OpenBitSet> new_bit_sets = new ArrayList<OpenBitSet>();
		for (Map.Entry<OpenBitSet,Integer> entry : this.algo.bitesetMap_insert.entrySet()) {
			
			/**** Optimization: I am not sure we need to test if the bitset is equal to 0. 
			 * Also, why is the second if condition useful for ? 
			 */
			key = entry.getKey();
			if (!this.algo.bitesetMap.containsKey(key)) {
				new_bit_sets.add(key);
				//System.out.println("New open bit set given insertion: "+BitSetUtils.toString(key,this.algo.numberAttributes));
			}
			//this.algo.bitesetMap.merge(key,this.algo.bitesetMap_insert.get(key), Integer::sum);
			
			
		}
		end = Instant.now();
		insert_duration.add(Duration.between(start, end).toMillis());
		
		Collections.sort(new_bit_sets,new SortOpenBitSet());
		
		//System.out.println("new_bit_sets sorted");
		//System.out.println(BitSetUtils.toString(new_bit_sets.get(0), this.algo.numberAttributes));
		//System.out.println(BitSetUtils.toString(new_bit_sets.get(new_bit_sets.size() - 1), this.algo.numberAttributes));
			
		
		if (new_bit_sets.size() > 0) {
			start = Instant.now();
			
			
	        for (int k = 0; k< new_bit_sets.size();k++) {
	        	diffAttrs = new_bit_sets.get(k);
	        	equalAttrs = new OpenBitSet(numberAttributes);
	        	equalAttrs.set(0, this.numberAttributes);
	        	equalAttrs.andNot(diffAttrs);
	        	for (int j = 0; j<numberAttributes; j++) {
	        		if (diffAttrs.get(j)) {
	        			this.algo.negCoverTree.addFunctionalDependency(equalAttrs,j);
	        			this.algo.specializePositiveCover_New(this.algo.posCoverTree, equalAttrs, j);
	        		}
	        		
	        	}
	        	
	        }
			
			end = Instant.now();
			insert_duration.add(Duration.between(start, end).toMillis());
		
		
			//System.out.println("Negative dependencies after insertion");
			//this.algo.getNegCoverTree().printDependencies();
			start = Instant.now();
			// No nded to call calculate positive cover 
			//this.algo.posCoverTree = 
			//		this.algo.calculatePositiveCover(this.algo.negCoverTree);
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
		
		//System.out.println("Bitset map after insertion");
		//this.algo.printBitSetMap();
		
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
		
		//String file = "resources/iris-num";
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
		 String file = "resources/small_example";
		 String result_file = "small_example";
		 
		 //String file = "resources/cpu/cpu_base";
		 //String result_file = "cpu_base";
		
		
		int number_of_batches = 2;
		IncrementalFDep ifdep = new IncrementalFDep(file,number_of_batches,result_file);
		try {
			ifdep.execute();
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		
		
		
		
		
		
		/*
		System.out.println("Negative cover");
		ifdep.algo.negCoverTree.printDependencies(ifdep.numberAttributes);
		System.out.print("Number of FDs: "+ifdep.algo.posCoverTree.getFunctionalDependencies().size());
	    ifdep.algo.posCoverTree.printDependencies(ifdep.numberAttributes);
	    */
		

		
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
		
		/*
		ArrayList<OpenBitSet> vectorList = new ArrayList<OpenBitSet>();
		int num_vectors = 57884;
		int size_vector = 203970;
		OpenBitSet vec;
		for (int i = 0; i< num_vectors; i++) {
			System.out.println(i);
			vec = new OpenBitSet(203970);
			vec.set(0, size_vector -1);
			vectorList.add(vec);
		}
		
		for (int i = 0; i< vectorList.size();i++)
			System.out.println(BitSetUtils.toString(vectorList.get(i)));
		
		*/
	}

}
