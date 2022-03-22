package fr.dauphine.lamsade.khalid.dynast;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class DepMiner {
	
	//Column Names
	//protected List<Integer> columnIDs;
	protected List<String> columnNames;
	
	//Tuples
	Map<Integer,List<Integer>> Tuples;
	
	//partitions
	List<Map<Integer,Set<Integer>>> PI;
	
	//Stripped partitions
	List<Map<Integer,Set<Integer>>> S_PI;

	int numAttributes;
	
	//Inserted tuples
	List<Integer> Delta_tuples_plus;

	//Deleted tuples
	List<Integer> Delta_tuples_minus;	
	
	//Maximum, the element of the member sets designate tuples 
	Set<List<Integer>> Maximal_Sets;
	
	//Agree sets, the elements of the member sets designates attributes
	Set<Set<Integer>> AgreeSets;
	
	//Specify for each tuple its associated equivalence classes
	// Map<tuple_id,Map<Attr_ID,Set<Partition_ID>>
	Map<Integer,Map<Integer,Set<Integer>>> EC;
	
	Integer current_max_tuple_id; 
	
	DepMiner(){
		current_max_tuple_id = 0;
		Delta_tuples_plus = new ArrayList<Integer>();
		Delta_tuples_minus = new ArrayList<Integer>();
		
		
	}
	

	public void setColumnIDs(List<String> columnNames) {
		this.columnNames = columnNames;
	}
	
	public void Load_Initial_file(String file) throws FileNotFoundException, IOException {
		Tuples = new HashMap<Integer,List<Integer>>();
		
    	try (CSVReader csvReader = new CSVReader(new FileReader(file+".csv"));) {
    	    String[] values = null;
    	    if ((values = csvReader.readNext()) != null) {
    	    	this.columnNames = Arrays.asList(values);
    	    	//for(String s : values) 
    	    	//	this.columnIDs.add(Integer.valueOf(s));
    	    	this.numAttributes = this.columnNames.size();
    	    }    	    
    	    while ((values = csvReader.readNext()) != null) {  	
    	    	this.Tuples.put(current_max_tuple_id,
    	    			(ArrayList<Integer>) Arrays.asList(values).stream()
    	                .map(Integer::valueOf).collect(Collectors.toList())
    	                );
    	    	current_max_tuple_id++;
    	    }
    	    
    	}
		
    }
	
	public void generatePartitions() {
		Integer tuple_id;
		
		PI =new ArrayList<Map<Integer,Set<Integer>>>();
		
		for (int att = 0; att < this.numAttributes; att++)
			PI.add(new HashMap<Integer,Set<Integer>>());
		
		for (Map.Entry<Integer, List<Integer>> entry : this.Tuples.entrySet()) {
			tuple_id = entry.getKey();
			for (int att = 0; att < this.numAttributes; att++) {
				
				if (!PI.get(att).containsKey(entry.getValue().get(att)))
					PI.get(att).put(entry.getValue().get(att), new HashSet());
				
				PI.get(att).get(entry.getValue().get(att)).add(tuple_id);
				
			}
		}
		
	}
	
	public void generateStrippedPartitions() {
		S_PI =new ArrayList<Map<Integer,Set<Integer>>>();
		
		for (int att=0; att < this.numAttributes; att++) {
			
			Map<Integer, Set<Integer>> map = new HashMap<Integer, Set<Integer>>();
			
			for (Map.Entry<Integer, Set<Integer>> entry : this.PI.get(att).entrySet()) {
				if (entry.getValue().size() > 1)
					map.put(entry.getKey(), new HashSet(entry.getValue()));
			}
			
			S_PI.add(map);
		}
		
	}
	
	public void generateMaximalSets() throws Exception {
		
		Set<Set<Integer>> SofS = new HashSet();
		Set<Set<Integer>> SofS_1 = new HashSet();
		
		//System.out.println("SPI");
		for (int att = 0; att < numAttributes; att++) {
			for (Map.Entry<Integer, Set<Integer>> entry : this.S_PI.get(att).entrySet()) {
				//System.out.println(entry.getValue().toString());
				SofS.add(entry.getValue());
			}
		}
		
		for (Set s_1: SofS) {
			for (Set s_2: SofS) {
				if (s_2.containsAll(s_1) && !s_1.equals(s_2));
				break;
			}
			SofS_1.add(s_1);
		}
		
		
		Maximal_Sets = new HashSet();
		List l;
		for (Set s : SofS_1) {
			l = new ArrayList(s);
			Collections.sort(l);
			Maximal_Sets.add(l);
		}
		
	}
	
	// Bulk method for finding agree sets 
	public Set<Set<Integer>> findAgreeSets(String file) throws Exception {
		
		Instant start, end;
		
		int attr_id;
		int equivalence_class_id;
		
		this.AgreeSets = new HashSet<Set<Integer>>();
		
		this.Load_Initial_file(file);
		
		System.out.println("Loaded file");
		
		start = Instant.now();
		
		this.generatePartitions();
		
		System.out.println("Created partitions");
		
		this.generateStrippedPartitions();
		
		System.out.println("Created stripped partitions");
		
		this.profileStrippedPartitions();
		
		EC = new HashMap<Integer,Map<Integer,Set<Integer>>>();
		
		attr_id = 0;
		for(Map<Integer,Set<Integer>> pi: S_PI) {
			for (Map.Entry<Integer, Set<Integer>> entry : pi.entrySet()) {
				equivalence_class_id = entry.getKey();
				for (Integer tuple_id:entry.getValue()) {
					if (!EC.containsKey(tuple_id))
						EC.put(tuple_id, new HashMap<Integer,Set<Integer>>());
					if 	(!EC.get(tuple_id).containsKey(attr_id))
						EC.get(tuple_id).put(attr_id, new HashSet());
						
					EC.get(tuple_id).get(attr_id).add(equivalence_class_id);
				}
			}
			attr_id++;
		}
		
		System.out.println("Assocaited triples with equivalence classes");
		
		this.generateMaximalSets();
		
		System.out.println("Constructed maximal sets");
		
		this.profileMaximalSets();
		
		//this.printMaximalSets();
		
		//this.printMaximalSets();
		Integer val_i, val_j;
		
		Set<Set<Integer>> treated_pairs = new HashSet<Set<Integer>>();
		for (List<Integer> tuples: this.Maximal_Sets) {
			for (int i =0; i< tuples.size(); i++) 
				for (int j=i+1; j< tuples.size(); j++) {
					val_i = tuples.get(i);
					val_j = tuples.get(j);
					Set set_i_j = new HashSet() ;
					set_i_j.add(val_i); 
					set_i_j.add(val_j); 
					if (!treated_pairs.contains(set_i_j)) {
						treated_pairs.add(set_i_j);
						Set as = new HashSet<Integer>();
						for (int attr = 0; attr < numAttributes; attr++) {
							//System.out.println("EC.get(tuples.get(i)).get(attr) "+EC.get(tuples.get(i)).get(attr).toString());
							//System.out.println("EC.get(tuples.get(j)).get(attr)");
							if ((EC.get(val_i).get(attr) != null) && (EC.get(val_j).get(attr) != null)) {
								Set<Integer> intersection = new HashSet<Integer>(EC.get(val_i).get(attr));
								intersection.retainAll(EC.get(val_j).get(attr));
								if (intersection.size() > 0)
									as.add(attr);
							}
						}
						this.AgreeSets.add(as);
					}
				}
					
		}
		
		end = Instant.now();
		
		System.out.println("Processing time: "+Duration.between(start, end).toMillis()+"ms");
		
		System.out.println("Constructed agree sets");
		
		//this.printAgreeSets();
		return this.AgreeSets;
	}
	
	private void profileMaximalSets() {
	
		System.out.println("Number of maximal sets: "+this.Maximal_Sets.size());
		
		int max = 2;
		int min = 2;
		for (List<Integer> s : this.Maximal_Sets) {
			if (s.size() > max)
				max = s.size();
			if (s.size() < min)
				min = s.size();
		}
		
		System.out.println("min: "+min+ " and max: "+max);
		
	}
	
	private void profileStrippedPartitions() {
				
		int max = 2;
		int min = 2;
		for (Map<Integer,Set<Integer>> map: S_PI) {
			for (Map.Entry<Integer, Set<Integer>> entry : map.entrySet()) {
				if (entry.getValue().size() > max)
					max = entry.getValue().size();
				if (entry.getValue().size() < min)
					min = entry.getValue().size();
			}
			
			
		}

		
		System.out.println("Stripped partitions, min: "+min+ " and max: "+max);
		
	}


	public void printAgreeSets() {
		System.out.println("Agree Sets");
		for (Set<Integer> s: this.AgreeSets) {
			System.out.println(s.toString());
		}
		
		
	}
	
	public void printPartitions() {
		for (int att = 0; att < this.numAttributes ; att++) {
			System.out.println("Equivalence classes for attribute "+att);
			for (Map.Entry<Integer, Set<Integer>> entry : this.PI.get(att).entrySet()) {
				System.out.println("attribute value: "+entry.getKey()+",   tuples IDs: "+Arrays.toString(entry.getValue().toArray()));
			}
			
		}
	}
	
	public void printStrippedPartitions() {
		for (int att = 0; att < this.numAttributes ; att++) {
			System.out.println("Equivalence classes for attribute "+att);
			for (Map.Entry<Integer, Set<Integer>> entry : this.S_PI.get(att).entrySet()) {
				System.out.println("attribute value: "+entry.getKey()+",   tuples IDs: "+Arrays.toString(entry.getValue().toArray()));
			}
			
		}
	}
	
	public void printMaximalSets() {
		System.out.println("Maximal Sets");
		for (List<Integer> l: Maximal_Sets) {
			System.out.println(Arrays.toString(l.toArray()));
		}
	}

	public static void main(String[] args) throws Exception {
		
		//String file = "resources/Adult_1/num_adult";
		//String file = "resources/small_example3";
		//String file = "resources/flight_1000/num_flight_1k";
		String file = "resources/uniprot_10/num_uniprot";
		
		DepMiner dm = new DepMiner();
		dm.findAgreeSets(file);
		/*
		try {
			dm.Load_Initial_file(file);
			dm.generatePartitions();
			dm.generateStrippedPartitions();
			
			//dm.printPartitions();
			//dm.printStrippedPartitions();
			
			dm.generateMaximalSets();
			
			dm.printMaximalSets();
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/

	}

}
