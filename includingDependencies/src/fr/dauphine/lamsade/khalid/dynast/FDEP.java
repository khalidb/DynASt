package fr.dauphine.lamsade.khalid.dynast;

/*
 * Code adapted and extended by Khalid Belhajjame
 */

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import fr.dauphine.lamsade.khalid.dynast.util.AVPair;
import fr.dauphine.lamsade.khalid.dynast.util.DiffVector;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class FDEP {
	
    protected List<String> columnNames;
    protected ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    
    int possiblebitsetlistsize;
    
    
    
    // Evidence sets the first index is used for the attribute, and the second for the ids of the tuple
    protected ObjectArrayList<HashMap<Integer,DiffVector>> evidenceSets;
    
    // New structure for evidence vectors
    protected ObjectArrayList<HashMap<Integer,DiffVector>> AVES;
    
	List<List<Integer>> inserted_tuples;
	List<Integer> deleted_tuples;
	List<Integer> tuple_ids = new ArrayList<Integer>();
	ObjectArrayList<HashMap<Integer,Set<Integer>>> indices; // Used for indexing the tuples by attribute values

	int[] scales; // Used to compute the bitsets
	Map int2bitset;
	int op_ID = 0;
    /* a hashmap the key of which is bitset, 
     * and the value represents the number of tuple pairs 
     * that have different and equal attributes as specified by the bitset. 
     */
    HashMap<OpenBitSet,Integer> bitesetMap = new HashMap<>();
    HashMap<OpenBitSet,Integer> bitesetMap_insert = new HashMap<>();
    HashMap<OpenBitSet,Integer> bitesetMap_delete = new HashMap<>();
    HashMap<OpenBitSet,Integer> bitesetMap_after_delete = new HashMap<>();

    protected int numberAttributes;

    protected FDTree negCoverTree;
    protected FDTree posCoverTree;
    //protected ObjectArrayList<List<Integer>> tuples = new ObjectArrayList<List<Integer>>();
    
    // I changed the types of this structure to be able to deal with tuple deletion
    protected Map<Integer,List<Integer>> tuples = new HashMap<Integer,List<Integer>>();
    protected int tuple_ID =0;

    protected FunctionalDependencyResultReceiver fdResultReceiver;


    

    private ValueComparator valueComparator;
	private String tableName;
	private OpenBitSet emptybitset;

    public FDEP(int numAttributes, ValueComparator valueComparator) {
        this.numberAttributes = numAttributes;
        this.valueComparator = valueComparator;
        this.emptybitset = new OpenBitSet(this.numberAttributes);
    }

    public FDTree execute(int[][] records) {
        FDTree negCoverTree = this.calculateNegativeCover(records);
        //negCoverTree.filterGeneralizations(); // TODO: (= remove all generalizations) Not necessary for correctness because calculating the positive cover does the filtering automatically if there are generalizations in the negCover, but for maybe for performance (?)
        records = null;

        //long t = System.currentTimeMillis();
        FDTree posCoverTree = this.calculatePositiveCover(negCoverTree);
        negCoverTree = null;
        //LOG.info("t = " + (System.currentTimeMillis() - t));

        //posCoverTree.filterDeadElements();

        return posCoverTree;
    }

    private FDTree calculateNegativeCover(int[][] records) {
        FDTree negCoverTree = new FDTree(this.numberAttributes, -1);
        for (int i = 0; i < records.length; i++)
            for (int j = i + 1; j < records.length; j++)
                this.addViolatedFdsToCover(records[i], records[j], negCoverTree);
        return negCoverTree;
    }

    /**
     * Find the least general functional dependencies violated by t1 and t2 and update the negative
     * cover accordingly. Note: t1 and t2 must have the same length.
     */
    private void addViolatedFdsToCover(int[] t1, int[] t2, FDTree negCoverTree) {
        OpenBitSet equalAttrs = new OpenBitSet(t1.length);
        for (int i = 0; i < t1.length; i++)
            if (this.valueComparator.isEqual(t1[i], t2[i]))
                equalAttrs.set(i);

        OpenBitSet diffAttrs = new OpenBitSet(t1.length);
        diffAttrs.set(0, this.numberAttributes);
        diffAttrs.andNot(equalAttrs);

        negCoverTree.addFunctionalDependency(equalAttrs, diffAttrs);
    }

    FDTree calculatePositiveCover(FDTree negCoverTree) {
        FDTree posCoverTree = new FDTree(this.numberAttributes, -1);
        posCoverTree.addMostGeneralDependencies();
        OpenBitSet activePath = new OpenBitSet(this.numberAttributes);
 
        this.calculatePositiveCover(posCoverTree, negCoverTree, activePath);

        return posCoverTree;
    }

    private void calculatePositiveCover(FDTree posCoverTree, FDTreeElement negCoverSubtree, OpenBitSet activePath) {
        OpenBitSet fds = negCoverSubtree.getFds();
        for (int rhs = fds.nextSetBit(0); rhs >= 0; rhs = fds.nextSetBit(rhs + 1))
            this.specializePositiveCover(posCoverTree, activePath, rhs);

        if (negCoverSubtree.getChildren() != null) {
            for (int attr = 0; attr < this.numberAttributes; attr++) {
                if (negCoverSubtree.getChildren()[attr] != null) {
                    activePath.set(attr);
                    this.calculatePositiveCover(posCoverTree, negCoverSubtree.getChildren()[attr], activePath);
                    activePath.clear(attr);
                }
            }
        }
    }

    private void specializePositiveCover(FDTree posCoverTree, OpenBitSet lhs, int rhs) {
        List<OpenBitSet> specLhss = null;
        specLhss = posCoverTree.getFdAndGeneralizations(lhs, rhs);
        for (OpenBitSet specLhs : specLhss) {
            posCoverTree.removeFunctionalDependency(specLhs, rhs);
            for (int attr = this.numberAttributes - 1; attr >= 0; attr--) {
                if (!lhs.get(attr) && (attr != rhs)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsFdOrGeneralization(specLhs, rhs))
                        posCoverTree.addFunctionalDependency(specLhs, rhs);
                    specLhs.clear(attr);
                }
            }
        }
    }
    
    
    
    
    public int generateID() {
    	this.tuple_ids.add(tuple_ID);
    	return this.tuple_ID++;
    }
    
    void initializePossibleBitSetListSize() {
    	this.possiblebitsetlistsize = (int) Math.pow(2,this.numberAttributes);
    	
    	/*
    	//System.out.print("2 to the power of "+this.getNumberAttributes()+" is "+Math.pow(2,this.getNumberAttributes()));
    	//for (int i=0; i<Math.pow(2,this.getNumberAttributes()); i++) {
    	for (int i=0; i<Math.pow(2,this.numberAttributes); i++) {	
    		//System.out.println("Binary string"+Integer.toBinaryString(i));
    		//System.out.println("Bit set"+this.fromString(Integer.toBinaryString(i)));
    		this.possiblebitsetlist.add(this.fromString(Integer.toBinaryString(i*2)));
    	}
    	*/
    
    }
    
    
    
    void initializeBitSetMap() {
    	//System.out.print("2 to the power of "+this.getNumberAttributes()+" is "+Math.pow(2,this.getNumberAttributes()));
    	//for (int i=0; i<Math.pow(2,this.getNumberAttributes()); i++) {
    	Double max = Math.pow(2,this.getNumberAttributes());
    	for (int i=0; i<max; i++) {	
    		//System.out.println("Binary string"+Integer.toBinaryString(i*2));
    		//System.out.println("Bit set"+this.fromString(Integer.toBinaryString(i)));
    		//System.out.println("Loops end in "+ (max -i)+" iterations");
    		this.bitesetMap.put(this.fromString(Integer.toBinaryString(i*2)), 0);
    	}
    
    }
    
    void initializeBitSetMapInserion() {
    	//System.out.print("2 to the power of "+this.getNumberAttributes()+" is "+Math.pow(2,this.getNumberAttributes()));
    	//for (int i=0; i<Math.pow(2,this.getNumberAttributes()); i++) {
    	for (int i=0; i<Math.pow(2,this.getNumberAttributes()); i++) {	
    		//System.out.println("Binary string"+Integer.toBinaryString(i));
    		//System.out.println("Bit set"+this.fromString(Integer.toBinaryString(i)));
    		this.bitesetMap_insert.put(this.fromString(Integer.toBinaryString(i*2)), 0);
    	}
    
    }
    
    void printBitSetMap() {
    	System.out.println("Print BitSetMap: ");
    	System.out.println("number of attributes: "+this.numberAttributes);
    	for (OpenBitSet name: this.bitesetMap.keySet()){
            String key = BitSetUtils.toString(name,this.numberAttributes);
            Integer value = this.bitesetMap.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap: ");
    	
    }
    
    void mergeBitSetMaps_insert() {
    	

    	
    	this.bitesetMap_insert.forEach(
    		    (key, value) -> this.bitesetMap.merge(key, value, (v1,v2) -> v1+v2));
    	
    	//System.out.println("Size of bitsetMap after the merge: "+this.bitesetMap.size());
 
    	
    	
    	
    }
    
    void printBitSetMap_insert() {
    	System.out.println("Print BitSetMap_insert: ");
    	for (OpenBitSet name: this.bitesetMap_insert.keySet()){
            String key = name.toString();
            Integer value = this.bitesetMap_insert.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap_insert: ");
    	
    }
    
    void printBitSetMap_delete() {
    	System.out.println("Print delete: ");
    	for (OpenBitSet name: this.bitesetMap_delete.keySet()){
            String key = name.toString();
            Integer value = this.bitesetMap_delete.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap_delete: ");
    	
    }
    
    OpenBitSet fromString(String binary) {
        OpenBitSet bitset = new OpenBitSet(binary.length());
        int len = binary.length();
        for (int i = len-1; i >= 0; i--) {
            if (binary.charAt(i) == '1') {
                bitset.set(len-i-1);
            }
        }
        return bitset;
    }
    
 
    public void execute() throws AlgorithmExecutionException {
        initialize();
        
        //System.out.println("Start constructing the negative cover");
        
        negativeCover();
        
        this.tuples = null;

        posCoverTree = calculatePositiveCover(negCoverTree );
//		posCoverTree.filterGeneralizations();
//        addAllDependenciesToResultReceiver();
    }

    private void initialize() throws AlgorithmExecutionException, InputGenerationException, InputIterationException {
        loadData();
        setColumnIdentifiers();
    }

    
    /**
     * Calculate the negative Cover for the current relation.
     */
    void negativeCover() {
        negCoverTree = new FDTree(this.numberAttributes,-1);
        for (int i = 0; i < tuples.size(); i++) {
            for (int j = i + 1; j < tuples.size(); j++) {
                violatedFds(tuples.get(i), tuples.get(j));
            }
        }
        
        //System.out.println("Dependencies of the negative cover");
        //negCoverTree.printDependencies(this.numberAttributes);
  
        
    }

    /**
     * Update the negative Cover for the current relation given new inserted tuples
     */
    void updateNegativeCoverGivenInsertion(List<OpenBitSet> new_bit_sets) {
        
        OpenBitSet diffAttr;
        OpenBitSet equalAttr = new OpenBitSet(this.numberAttributes);
        //equalAttr.set(0, this.numberAttributes); //+1 before
        
        for (int i=0; i<new_bit_sets.size();i++){

            equalAttr.set(0, this.numberAttributes); //+1 before // I changed the 0 to 1
        	
        	diffAttr = new_bit_sets.get(i);
        	equalAttr.andNot(diffAttr);
        	
        	//System.out.println("Insert the following bitset to the negative cover: "+diffAttr);
        	//System.out.println("with the corresponding equalAttr: "+equalAttr);
        	
            for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
                negCoverTree.addFunctionalDependency(equalAttr, a); 
            }
            //this.negCoverTree.filterSpecializations();
        	
            
        	
        }
        
        
    }
    
    /**
     * Update the negative Cover for the current relation given new inserted tuples
     */
    void updateNegativeCoverGivenDeletion_old(List<OpenBitSet> new_bit_sets) {
        
    	this.negCoverTree = new FDTree(this.numberAttributes,-1);
        OpenBitSet diffAttr;
        OpenBitSet equalAttr = new OpenBitSet(this.numberAttributes);
        //equalAttr.set(0, this.numberAttributes); //+1 before
        
        for (int i=0; i<new_bit_sets.size();i++){

            equalAttr.set(0, this.numberAttributes); //+1 before
        	
        	diffAttr = new_bit_sets.get(i);
        	equalAttr.andNot(diffAttr);
        	
        	//System.out.println("Insert the following bitset to the negative cover: "+diffAttr);
        	//System.out.println("with the corresponding equalAttr: "+equalAttr);
        	
            for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
                negCoverTree.addFunctionalDependency(equalAttr, a);
                
            }
           // this.negCoverTree.filterSpecializations();
        	
            
        	
        }
        
        
    }
    
    /**
     * Incremental Update the negative Cover for the current relation given new inserted tuples
     */
    void updateNegativeCoverGivenDeletion(List<OpenBitSet> deleted_bitsets) {
        
    	
        OpenBitSet diffAttr;
        OpenBitSet equalAttr = new OpenBitSet(this.numberAttributes);
        //equalAttr.set(0, this.numberAttributes); //+1 before
        
        
        for (int i=0; i<deleted_bitsets.size();i++){

            equalAttr.set(0, this.numberAttributes); //+1 before 
            
        	
        	diffAttr = deleted_bitsets.get(i);
        	equalAttr.andNot(diffAttr);
        	
        	//System.out.println("Insert the following bitset to the negative cover: "+diffAttr);
        	//System.out.println("with the corresponding equalAttr: "+equalAttr);
        	
            for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            	//System.out.println("The FD: "+BitSetUtils.toString(equalAttr,this.numberAttributes)+"->"+a+" was removed from the negative cover");
                negCoverTree.removeFunctionalDependency(equalAttr, a); 
            	
                
            }
            //this.negCoverTree.filterSpecializations();    
        	
        }
        
        
    }
    
    
    /**
     * Find the least general functional dependencies violated by t1 and t2
     * and add update the negative cover accordingly.<br/>
     * Note: t1 and t2 must have the same length.
     *
     * @param t1 An ObjectArrayList with the values of one entry of the relation.
     * @param t2 An ObjectArrayList with the values of another entry of the relation.
     */
    private void violatedFds(List<Integer> t1, List<Integer> t2) {
 
    	//System.out.println("Start by comparing tuple "+Arrays.toString(t1.toArray()) + " and "+Arrays.toString(t2.toArray()));
    	
    	OpenBitSet equalAttr = new OpenBitSet(this.numberAttributes);
        equalAttr.set(0, this.numberAttributes); // +1 before
        OpenBitSet diffAttr = new OpenBitSet(this.numberAttributes);
        for (int i = 0; i < t1.size(); i++) {
            Object val1 = t1.get(i);
            Object val2 = t2.get(i);
            // Handling of null values. Currently assuming NULL values are equal.
            if (val1 == null && val2 == null) {
                continue;
            } else if ((val1 == null && val2 != null) || !(val1.equals(val2))) {
                // BitSet start with 1 for first attribute
                diffAttr.set(i); //+1 before
            }
        }
        
        //System.out.println("equalAttr: "+BitSetUtils.toString(equalAttr,this.numberAttributes));
        
        //System.out.println("t1: "+Arrays.toString(t1.toArray())+ " and t2: "+Arrays.toString(t1.toArray()));
        //System.out.println("diffAttr: "+BitSetUtils.toString(diffAttr,this.numberAttributes));
        
        
        equalAttr.andNot(diffAttr);
        //System.out.println("equalAttr: "+BitSetUtils.toString(equalAttr,this.numberAttributes));
               
        
        
        /* Update the bitset map with the outcome of the new comparison */
        //this.bitesetMap.put(diffAttr, this.bitesetMap.get(diffAttr)+1);
        this.bitesetMap.merge(diffAttr, 1, Integer::sum);

        //System.out.println("bitset after update: "+ this.bitesetMap.get(diffAttr)+" for the bitset "+diffAttr);
        
        
        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            negCoverTree.addFunctionalDependency(equalAttr, a);
        }
        
    }


    /**
     * Fetch the data from the database and keep it as List of Lists.
     *
     * @throws AlgorithmExecutionException
     * @throws AlgorithmConfigurationException
     */
    private void loadData() throws AlgorithmExecutionException, AlgorithmConfigurationException {
        RelationalInput ri = null;
        /*
        if (this.relationalInputGenerator != null) {
            ri = this.relationalInputGenerator.generateNewCopy();
        } else if (this.databaseConnectionGenerator != null && this.tableName != null) {
            String sql = "SELECT * FROM " + this.tableName;
            ri = this.databaseConnectionGenerator.generateRelationalInputFromSql(sql, this.tableName);
        } else if (this.relationalInputGenerator == null) {
         */
        	this.tableName = "test";
        	this.columnNames = Arrays.asList(new String[]{"A", "B", "C", "D"});
        	this.numberAttributes = 4;
        	this.addTuple(Arrays.asList(new Integer[]{1,1, 1,1}));
        	this.addTuple(Arrays.asList(new Integer[]{1,2,2,1}));
        	this.addTuple(Arrays.asList(new Integer[]{2,1,1,2}));
        	

        /*
        if (ri != null) {
            this.columnNames = ri.columnNames();
            this.tableName = ri.relationName();
            this.numberAttributes = ri.numberOfColumns();
            while (ri.hasNext()) {
                List<Integer> row = ri.next().stream()
    	                .map(Integer::valueOf).collect(Collectors.toList());
                this.addTuple(row);
            }
        }
        */
        
        this.initializePossibleBitSetListSize();
    }

    void setColumnIdentifiers() {
        this.columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(
                this.columnNames.size());
        for (String column_name : this.columnNames) {
            columnIdentifiers.add(new ColumnIdentifier(this.tableName,
                    column_name));
        }
    }


	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<String> getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}

	public ObjectArrayList<ColumnIdentifier> getColumnIdentifiers() {
		return columnIdentifiers;
	}

	public void setColumnIdentifiers(ObjectArrayList<ColumnIdentifier> columnIdentifiers) {
		this.columnIdentifiers = columnIdentifiers;
	}

	public int getNumberAttributes() {
		return numberAttributes;
	}

	public void setNumberAttributes(int numberAttributes) {
		this.numberAttributes = numberAttributes; 
	}

	public FDTree getNegCoverTree() {
		return negCoverTree;
	}

	public void setNegCoverTree(FDTree negCoverTree) {
		this.negCoverTree = negCoverTree;
	}

	public FDTree getPosCoverTree() {
		return posCoverTree;
	}

	public void setPosCoverTree(FDTree posCoverTree) {
		this.posCoverTree = posCoverTree;
	}

	public Map<Integer, List<Integer>> getTuples() {
		return tuples;
	}

	
	public int addTuple(List<Integer> t) {
		
		int id = this.generateID();
		this.tuples.put(id, t);
		
		for (int j=0; j< this.numberAttributes;j++) {
			if (indices.get(j).containsKey(t.get(j)))
				indices.get(j).get(t.get(j)).add(id); 
			else {
				Set<Integer> si = new HashSet<Integer>();
				si.add(id);
				indices.get(j).put(t.get(j), si);
			}
		}
		return id;
		
	}

	public FunctionalDependencyResultReceiver getFdResultReceiver() {
		return fdResultReceiver;
	}

	public void setFdResultReceiver(FunctionalDependencyResultReceiver fdResultReceiver) {
		this.fdResultReceiver = fdResultReceiver;
	}
	
	
	public void initializeIndexStructure() {
		
		indices = new ObjectArrayList<HashMap<Integer,Set<Integer>>>();
		HashMap<Integer,Set<Integer>> tmp = new HashMap<Integer,Set<Integer>>();
		for (int i=0; i< this.numberAttributes;i++)
			indices.add(new HashMap<Integer,Set<Integer>>()); 
		
	}
	
	public void printIndices() {
		for (int j=0; j< this.numberAttributes;j++) {
			System.out.println("Index for attribute "+j);
			for (Map.Entry<Integer, Set<Integer>> entry : this.indices.get(j).entrySet()) {
			    System.out.println(entry.getKey()+" : "+entry.getValue().toString());
			}
			
			
		}
		
		
	}
	
	public void initializeEvidenceSets() {
		
		/****** Optimization: the initialiation of the evidence needs to be optimized ****/
		/* The following code block has been replaced with the below uncommented code
		evidenceSets = new ObjectArrayList<HashMap<Integer,DiffVector>>();
		
		int num_evs = 0;
		
		for (int i=0; i<this.numberAttributes; i++) {
			
			//System.out.println("Evidence sets for attribute: "+i);
			
			HashMap<Integer,DiffVector> tmp = new HashMap<Integer,DiffVector>();
			
			for (Integer key_j : this.getTuples().keySet()) {
				
			//for (int j=0; j< this.getTuples().size(); j++) {
				DiffVector evs_j =  new DiffVector();
				num_evs++;
				for (Integer key_k : this.getTuples().keySet()) {
				//for (int k=0; k < this.getTuples().size(); k++) {
					if (this.getTuples().get(key_j).get(i) == this.getTuples().get(key_k).get(i))
						evs_j.add(false);
					else
						evs_j.add(true);
				}
				tmp.put(key_j,evs_j);
			}	
			evidenceSets.add(tmp);		
		}
		
		System.out.println("Number of attribute-value vectors created at initialization using the old method: "+num_evs);

		*/
		
		/* The old code before the optimization is above */
		
		/*
		AVES = new ObjectArrayList<HashMap<Integer,DiffVector>>();
		HashMap<Integer,DiffVector> AVES_i;
		
		int num_att_value_vectors = 0;
		
		for (int i=0; i<this.numberAttributes; i++) {
			
			AVES_i = new HashMap<Integer,DiffVector>();
			for (Integer key_j : this.getTuples().keySet()) {
				System.out.println("Handling tuple: "+key_j);
				if (!AVES_i.containsKey(this.getTuples().get(key_j).get(i))) {
					AVES_i.put(this.getTuples().get(key_j).get(i), this.initializeAttributeValueVector(i, this.getTuples().get(key_j).get(i))); 
					num_att_value_vectors++;
					
				}	
			}
			AVES.add(AVES_i);
		}
		*/
		
		AVES = new ObjectArrayList<HashMap<Integer,DiffVector>>();
		for (int i=0; i<this.numberAttributes; i++) 
			AVES.add(new HashMap<Integer,DiffVector>());
		
		
		for (Integer key_j : this.getTuples().keySet()) {
			//System.out.println("Handling tuple: "+key_j);
			for (int i=0; i<this.numberAttributes; i++) {
				if (!AVES.get(i).containsKey(this.getTuples().get(key_j).get(i))) {
					AVES.get(i).put(this.getTuples().get(key_j).get(i), this.initializeAttributeValueVector(i, this.getTuples().get(key_j).get(i)));
				}
			}

		}
		
		
		//System.out.println("Number of attribute-value vectors created at initialization using the new method: "+num_att_value_vectors);
		
		//this.printAVES();
		//this.printEvidenceSets();
		
	}
	
	protected DiffVector initializeAttributeValueVector(Integer attribute, Integer value) {
		DiffVector av_vector = new DiffVector();
		for (Integer key_k : this.getTuples().keySet()) {
			if (value == this.getTuples().get(key_k).get(attribute))
				av_vector.add(false);
			else
				av_vector.add(true);	
		}
		return av_vector;
	}
	
	
	public void printAVES() {
		
		for (int i=0; i<this.AVES.size(); i++) {
			
			System.out.println("* AVES for Attribute "+i);
		 	
			
			for (int key: this.AVES.get(i).keySet()) {		
				System.out.print("--- attribute value "+key+" ");
					System.out.println(this.AVES.get(i).get(key).toString());
			
			}	
		}
		
	}
	
	
	public void printEvidenceSets() {
		
		for (int i=0; i<this.evidenceSets.size(); i++) {
			
			System.out.println("* Evidence sets for Attribute "+i);
		 	
			
			for (int key: this.evidenceSets.get(i).keySet()) {		
				System.out.print("--- tuple "+key+" ");
					System.out.println(this.evidenceSets.get(i).get(key).toString());
			
			}	
		}
		
	}
	

	/* 
	 * The following method update the bitsetmap following the insertion of new tuples
	 */
	public void insertBitsets(int n) {
		
			
			List<ArrayList<Boolean>> vecs = new ArrayList<ArrayList<Boolean>> ();
			for (int j =0; j<this.numberAttributes; j++) 
				vecs.add(this.evidenceSets.get(j).get(n).getVector());
			
			/*--
			System.out.println("Vector computed for tuples "+n);
			System.out.println(v.toString());
			*/
			
			
			/* This code was replaced by the block that follows on the 29th of July 2020 
			 * 	    	 OpenBitSet bs;
	    	 List<OpenBitSet> bitsetlist = new ArrayList<OpenBitSet>();
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i))
	    				bs.set(j); //+1 before
	    		 }
	    		 // The following code was replaced by the only instruction (the one that follows the commented out block) to allow tuple-relation evidence set to be a multiset 
	    		 //if (!bitsetlist.contains(bs))
	    		 //	 bitsetlist.add(bs);
	    		 //if (bitsetlist.size() == this.possiblebitsetlistsize)
	    		 //	 break; 
	    		 
	    		 bitsetlist.add(bs);
			 */
			
	    	 OpenBitSet bs;
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i))
	    				bs.set(j); //+1 before
	    		 }
	    		 /* The following code was replaced by the only instruction (the one that follows the commented out block) to allow tuple-relation evidence set to be a multiset 
	    		 if (!bitsetlist.contains(bs))
	    			 bitsetlist.add(bs);
	    		 if (bitsetlist.size() == this.possiblebitsetlistsize)
	    			 break; 
	    		 */
	    		 this.bitesetMap_insert.merge(bs, 1, Integer::sum);
	    		 
	    	 }
	
 			
		//}
		
	}
	

    
	
	/* 
	 * The following method update the bitsetmap following the insertion of new tuples
	 */
	public void insertBitsets_New(List<Integer> tuple) {
		 
			
		
			//System.out.println("Computing the bitsets that needs to be added given the inserted tuple: "+Arrays.toString(tuple.toArray()));
			List<ArrayList<Boolean>> vecs = new ArrayList<ArrayList<Boolean>> ();
			for (int j =0; j<this.numberAttributes; j++) 
				vecs.add(this.AVES.get(j).get(tuple.get(j)).getVector());
			
			/*--
			System.out.println("Vector computed for tuples "+n);
			System.out.println(v.toString());
			*/
			
			
			/* This code was replaced by the block that follows on the 29th of July 2020 
			 * 	    	 OpenBitSet bs;
	    	 List<OpenBitSet> bitsetlist = new ArrayList<OpenBitSet>();
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i))
	    				bs.set(j); //+1 before
	    		 }
	    		 // The following code was replaced by the only instruction (the one that follows the commented out block) to allow tuple-relation evidence set to be a multiset 
	    		 //if (!bitsetlist.contains(bs))
	    		 //	 bitsetlist.add(bs);
	    		 //if (bitsetlist.size() == this.possiblebitsetlistsize)
	    		 //	 break; 
	    		 
	    		 bitsetlist.add(bs);
			 */
			
	    	 OpenBitSet bs;
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i))
	    				bs.set(j); //+1 before
	    		 }
	    		 /* The following code was replaced by the only instruction (the one that follows the commented out block) to allow tuple-relation evidence set to be a multiset 
	    		 if (!bitsetlist.contains(bs))
	    			 bitsetlist.add(bs);
	    		 if (bitsetlist.size() == this.possiblebitsetlistsize)
	    			 break; 
	    		 */
	    		 if (!bs.equals(emptybitset))
	    		   this.bitesetMap_insert.merge(bs, 1, Integer::sum);
	    		
	    		 
	    	 }
	
 			
		//}
		
	}
	
	
	/* 
	 * The following method update the bitsetmap following the insertion of new tuples
	 */
	public List<OpenBitSet> deleteBitsets() {
		
		List<ArrayList<Boolean>> vecs;
		OpenBitSet bs;
		List<OpenBitSet> bitsetlist;
		List<OpenBitSet> deleted_bitsets = new ArrayList<OpenBitSet>();
		OpenBitSet emptybs = new OpenBitSet(this.numberAttributes);
		
		for (int k=0; k<this.deleted_tuples.size(); k++) {
			//System.out.println("Tuple deleted: "+this.deleted_tuples.get(k));
			
			vecs = new ArrayList<ArrayList<Boolean>> ();
			
			//System.out.println("deleted_tuples.get(k): "+deleted_tuples.get(k));
			
			//System.out.println("Boolean Vectors that needs to be zipped given deletion: ");
			for (int j =0; j<this.numberAttributes; j++) {
				vecs.add(this.evidenceSets.get(j).get(this.deleted_tuples.get(k)).getVector());
				//System.out.println(Arrays.toString(this.evidenceSets.get(j).get(this.deleted_tuples.get(k)).getVector().toArray()));
			}
			
			/* The following block was modified for optilization purposes on the 29th of July 2020
			
			bitsetlist = new ArrayList<OpenBitSet>();
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i)) {
	    				bs.set(j); //+1 before
	    			}
	    			   
	    		 }
	    		 // The following code was commented out by a single instruction (the one that follows) to allow tuple relation evidence set to become a multiset
	    		 //if (!bitsetlist.contains(bs)) {
	    		 //	 bitsetlist.add(bs);
	    		 //	 //System.out.println("Bitset: "+  BitSetUtils.toString(bs, this.numberAttributes));
	    		 //}
	    		 //if (bitsetlist.size() == this.possiblebitsetlistsize)
	    		 //	 break; 
	    		
	    		 bitsetlist.add(bs);
	    		 
	    	 }
			
	    	
	    	for (int i=0;i<bitsetlist.size();i++) {
	    		//System.out.println("bitsetlist.get(i): "+bitsetlist.get(i));
	    		//this.printBitSetMap();
	    		if (!bitsetlist.get(i).equals(emptybs)) {
	    			if (this.bitesetMap.get(bitsetlist.get(i)) == 1) {
	    				this.bitesetMap.remove(bitsetlist.get(i));
	    				deleted_bitsets.add(bitsetlist.get(i));
	    			}
	    			else
	    				this.bitesetMap.merge(bitsetlist.get(i), -1, Integer::sum);
	    		}
	    	}
	    	
			
	    	//System.out.println("Evidence Set BEFORE removin tuple: "+this.deleted_tuples.get(k));
	    	//this.printEvidenceSets();
	    	
	    	this.updateEvidenceGivenDeletion(this.deleted_tuples.get(k));
	    	
	    	//System.out.println("Evidence Set AFTER removin tuple: "+this.deleted_tuples.get(k));
	    	//this.printEvidenceSets();
	    	//this.printEvidenceSets();
	    	*/
	    	

	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i)) {
	    				bs.set(j); //+1 before
	    			}
	    			   
	    		 }
	    		 /* The following code was commented out by a single instruction (the one that follows) to allow tuple relation evidence set to become a multiset
	    		 if (!bitsetlist.contains(bs)) {
	    			 bitsetlist.add(bs);
	    			 //System.out.println("Bitset: "+  BitSetUtils.toString(bs, this.numberAttributes));
	    		 }
	    		 if (bitsetlist.size() == this.possiblebitsetlistsize)
	    			 break; 
	    		*/
	    		 
	    		 
	    		 if (!bs.equals(emptybs)) {
		    			if (this.bitesetMap.get(bs) == 1) {
		    				this.bitesetMap.remove(bs);
		    				deleted_bitsets.add(bs);
		    			}
		    			else
		    				this.bitesetMap.merge(bs, -1, Integer::sum); 
	    		 }
	    		 
	    	 }
	    	
			
	    	//System.out.println("Evidence Set BEFORE removin tuple: "+this.deleted_tuples.get(k));
	    	//this.printEvidenceSets();
	    	
	    	this.updateEvidenceGivenDeletion(this.deleted_tuples.get(k));
	    	
	    	//System.out.println("Evidence Set AFTER removin tuple: "+this.deleted_tuples.get(k));
	    	//this.printEvidenceSets();
	    	//this.printEvidenceSets();
	    	
	    	
		}
		//System.out.println("Bit Set Map after Deletion");
		//this.printBitSetMap();
		
		return deleted_bitsets;
	}
	
	
	/* 
	 * The following method update the bitsetmap following the insertion of new tuples
	 */
	public ArrayList<OpenBitSet> deleteBitsets_New() {
		 
		List<ArrayList<Boolean>> vecs;
		OpenBitSet bs;
		List<OpenBitSet> bitsetlist;
		ArrayList<OpenBitSet> deleted_bitsets = new ArrayList<OpenBitSet>();
		OpenBitSet emptybs = new OpenBitSet(this.numberAttributes);
		List<Integer> deleted_tuple;
		
		for (int k=0; k<this.deleted_tuples.size(); k++) {
			//System.out.println("Tuple deleted: "+this.deleted_tuples.get(k));
			
			vecs = new ArrayList<ArrayList<Boolean>> ();
			
			//System.out.println("deleted_tuples.get(k): "+deleted_tuples.get(k));
			
			//System.out.println("Boolean Vectors that needs to be zipped given deletion: ");
			deleted_tuple = this.getTuples().get(this.deleted_tuples.get(k));
			for (int j =0; j<this.numberAttributes; j++) {	
				vecs.add(this.AVES.get(j).get(deleted_tuple.get(j)).getVector());
				//System.out.println(Arrays.toString(this.evidenceSets.get(j).get(this.deleted_tuples.get(k)).getVector().toArray()));
			}
			

	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new OpenBitSet(this.numberAttributes);
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i)) {
	    				bs.set(j); //+1 before
	    			}
	    			   
	    		 }
	    		 
	    		 
	    		 if (!bs.equals(emptybs)) {
		    			if (this.bitesetMap.get(bs) == 1) {
		    				this.bitesetMap.remove(bs);
		    				deleted_bitsets.add(bs);
		    			}
		    			else
		    				this.bitesetMap.merge(bs, -1, Integer::sum); 
	    		 }
	    		 
	    	 }
	    	
			
	    	//System.out.println("Evidence Set BEFORE removin tuple: "+this.deleted_tuples.get(k));
	    	//this.printEvidenceSets();
	    	
	    	this.updateEvidenceGivenDeletion_New(this.deleted_tuples.get(k), deleted_tuple);
	    	
	    	//System.out.println("Evidence Set AFTER removin tuple: "+this.deleted_tuples.get(k));
	    	//this.printEvidenceSets();
	    	//this.printEvidenceSets();
	    	
	    	
		}
		//System.out.println("Bit Set Map after Deletion");
		//this.printBitSetMap();
		
		return deleted_bitsets;
	}
	
	
    void removeViolatedFdsFromCover(ArrayList<OpenBitSet> non_fds) {
    	
        OpenBitSet equalAttrs, diffAttrs;
        
        //System.out.println("Size of non_fds: "+non_fds.size());
        List<OpenBitSet> gen_fds;
        OpenBitSet emptyset = new OpenBitSet(this.numberAttributes);
        
        for(int i = 0; i<non_fds.size(); i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(this.numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	
        	//System.out.println("Deletion: case of equalAttrs "+BitSetUtils.toString(equalAttrs,numberAttributes)+" and diffAttrs "+BitSetUtils.toString(diffAttrs,numberAttributes));
        	
        	for (int j=0; j< diffAttrs.size();j++) {
        		if (diffAttrs.get(j)) {
        			//System.out.println("Remove "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" from the negative cover");
        			negCoverTree.findTreeElement(equalAttrs).removeFd(j); // instead of removeFunctionalDependency(equalAttrs, j), which remove all generalizations
        			//System.out.println("Negative cover");
        			//negCoverTree.printDependencies(numberAttributes);
        			if (this.hasFdSpecialization(negCoverTree, equalAttrs.clone(), j)) {
                    	//System.out.println(BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" has a specialization in the negative cover");
                    }
                    else {
                    	//System.out.println("The non FD "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" does not have a specialization in the negatove cover");
                    	//System.out.println("Getgeneralizations from the negative cover");
                    	gen_fds = negCoverTree.getGeneralizations(equalAttrs,j);
                    	if (gen_fds.size() == 0) {
                    		this.removeSpecializationsFromPositiveCover(emptyset, j);
                    		posCoverTree.addFunctionalDependency(emptyset.clone(),j);
                    		
                    	}
                    	for (int k =0; k< gen_fds.size(); k ++) {
                    		this.removeSpecializationsFromPositiveCover(gen_fds.get(k), j);
                    		this.specializePositiveCover_New(posCoverTree, gen_fds.get(k), j);
                    		
                    	}
                    }
        		}
        	
        	}	
        }
    }
	
    public boolean hasFdSpecialization(FDTree fdtree, OpenBitSet lhs, int rhs) {
    	
    	boolean result = false;
    	FDTreeElement fd_e;
    	
    	for(int i=0; i< this.numberAttributes; i++) {
    		if (result == false) {
    			if ((!lhs.get(i)) && (i != rhs)) {
    				lhs.set(i);
    				fd_e = fdtree.findTreeElement(lhs);
    			
    				if (fd_e != null) {
    					if (fd_e.hasRhsAttribute(rhs)) {
    						if (fd_e.isFd(rhs)) {
    							result = true;
    						}
    						else {
    							result = hasFdSpecialization(fdtree,lhs,rhs);	
    						}
    					}
    				}
    				else 
    					result = false;
    				lhs.clear(i);
    				
    			}
    		}
    		else break;		
    	}
    	
    	return result;
    		
    }

    private void removeSpecializationsFromPositiveCover(OpenBitSet lhs, int rhs) {
        /*
    	List<OpenBitSet> specLhss = null;
        specLhss = posCoverTree.getFdAndGeneralizations(lhs, rhs);
        for (OpenBitSet specLhs : specLhss) {
            posCoverTree.removeFunctionalDependency(specLhs, rhs);
        }*/
    	
    	//System.out.println("removeSpecialization is called with lhs: "+BitSetUtils.toString(lhs,numberAttributes)+" and rhs: "+rhs);
    	FDTreeElement fd_element = posCoverTree.findTreeElement(lhs);
    	removeSpecializationElementsFromPositiveCover(lhs,rhs);
    	
    }
    
    private void removeSpecializationElementsFromPositiveCover(OpenBitSet lhs, int rhs) {
    	
    	FDTreeElement fde;
    	
    	for (int i = 0; i<numberAttributes; i++) {
    		if (!lhs.get(i)) {
    			lhs.set(i);
    			fde = posCoverTree.findTreeElement(lhs);
    			if ((fde != null) && (fde.hasRhsAttribute(rhs))) {
    				if (fde.isFd(rhs)) 
    					posCoverTree.removeFunctionalDependency(lhs, rhs);
    				else
    					removeSpecializationElementsFromPositiveCover(lhs,rhs);
    			}
    			lhs.clear(i);
    		} 
    	}
    	
    }
    
    void specializePositiveCover_New(FDTree posCoverTree, OpenBitSet lhs, int rhs) {
        List<OpenBitSet> specLhss = null;
        specLhss = posCoverTree.getFdAndGeneralizations(lhs, rhs);
        for (OpenBitSet specLhs : specLhss) {
            posCoverTree.removeFunctionalDependency(specLhs, rhs);
            for (int attr = this.numberAttributes - 1; attr >= 0; attr--) {
                if (!lhs.get(attr) && (attr != rhs)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsFdOrGeneralization(specLhs, rhs))
                        posCoverTree.addFunctionalDependency(specLhs, rhs);
                    specLhs.clear(attr);
                }
            }
        }
    }
	public void initializeScales() {
		scales = new int[this.numberAttributes];
		for (int i=0; i<this.numberAttributes;i++)
			scales[i] = (int) Math.pow(2,i);
		
	}

	/*
	public void initializeInt2Bitset() {
		
		this.int2bitset = new HashMap<Integer,OpenBitSet>();
		for(int i =0;i<Math.pow(2, this.numberAttributes); i++) {
			OpenBitSet bs = BitSet.valueOf(new long[]{i*2}); 
			int2bitset.put(i, bs);
			
		}
		
		
		//System.out.println("# Int2BitSet");
		//for (Object objectName : int2bitset.keySet()) {
		//	   System.out.println(objectName);
		//	   System.out.println(int2bitset.get(objectName));
		//	 }
			 
		

	}
    */

	public void updateEvidenceGivenDeletion(int tuple_id) {

		
		int curr_op;
		
		
		
			curr_op = ++op_ID;
			for (int att =0; att<this.numberAttributes; att++) {
				for (Integer key_v: this.evidenceSets.get(att).keySet()) {
					if (this.evidenceSets.get(att).get(key_v).getLast_op_id() != curr_op) {
						this.evidenceSets.get(att).get(key_v).remove(tuple_ids.indexOf(tuple_id));
						this.evidenceSets.get(att).get(key_v).setLast_op_id(curr_op);
						
					}
				}
			}	

		
            //System.out.println("Remove tuple with id: "+tuple_id+" which is in position "+tuple_ids.indexOf(tuple_id));
            
            //System.out.println("tuple_ids: "+Arrays.toString(tuple_ids.toArray()));
			for (int att =0; att<this.numberAttributes; att++) {
				//this.evidenceSets.get(att).remove(tuple_id);
				this.evidenceSets.get(att).remove(tuple_id);
			}
			
			tuple_ids.remove(Integer.valueOf(tuple_id));
			
		
		
	}


	public void updateEvidenceGivenDeletion_New(int tuple_id, List<Integer> deleted_tuple) {
			
			

			for (int att =0; att<this.numberAttributes; att++) {
				for (Integer key_v: this.AVES.get(att).keySet()) {
						this.AVES.get(att).get(key_v).remove(tuple_ids.indexOf(tuple_id)); 
						
					}
			}
			
			tuple_ids.remove(Integer.valueOf(tuple_id));
			
			//%%%%%% Needs to check if an attribute value vector needs to be removed al together
			// because there is no tuple with such an attribute value.
			// The way I see it now, it have something similar to Plis, which I called IAIdx
			
			
		
		
	}
    
	public static void main(String[] args) {
		
		FDEP algo;
        algo = new FDEP(4, new ValueComparator(true));

		
		//algo.numberAttributes = 4;
		FDTree negCoverTree = new FDTree(4,0);
		FDTree posCoverTree;
		OpenBitSet lhs = new OpenBitSet();
		lhs.set(0);
		lhs.set(3);

		negCoverTree.addFunctionalDependency(lhs, 1);  
		negCoverTree.addFunctionalDependency(lhs, 2); 
		
		
	    lhs = new OpenBitSet();
		lhs.set(1);
		lhs.set(2);
		negCoverTree.addFunctionalDependency(lhs, 0);
		negCoverTree.addFunctionalDependency(lhs, 3);
		
		lhs = new OpenBitSet();
		lhs.set(2);
		negCoverTree.addFunctionalDependency(lhs, 3);
		
		
		
        /* System.out.println("Negative FDs");
        List<OpenBitSetFD> neg_fds = negCoverTree.getFunctionalDependencies();
        for (int i=0; i< neg_fds.size();i++)
        	System.out.println(neg_fds.get(i).toString(4));
        */	
		//negCoverTree.filterSpecializations();
        
        
 
		
		

        
        posCoverTree = algo.calculatePositiveCover(negCoverTree);

        System.out.println("Positive FDs");
        List<OpenBitSetFD> pos_fds = posCoverTree.getFunctionalDependencies();
        for (int i=0; i< pos_fds.size();i++)
        	System.out.println(pos_fds.get(i).toString(4));
		//negCoverTree.filterSpecializations();

        
	    lhs = new OpenBitSet();
		lhs.set(1);
		lhs.set(2);
		negCoverTree.removeFunctionalDependency(lhs, 3);  
        
        System.out.println("Negative FDs after deletion");
        List<OpenBitSetFD> neg_fds = negCoverTree.getFunctionalDependencies();
        for (int i=0; i< neg_fds.size();i++)
        	System.out.println(neg_fds.get(i).toString(4));		
		
        

        
        posCoverTree = algo.calculatePositiveCover(negCoverTree);

        System.out.println("Positive FDs After Deletion");
        pos_fds = posCoverTree.getFunctionalDependencies();
        for (int i=0; i< pos_fds.size();i++)
        	System.out.println(pos_fds.get(i).toString(4));
		//negCoverTree.filterSpecializations();
        //negCoverTree.printDependencies();
        
      /* 
        System.out.println("Positive FDs");
        algo.posCoverTree.printDependencies();
        
		bs = new BitSet();
		bs.set(2);
		bs.set(3);
		algo.negCoverTree.deleteGeneralizations(bs,1,0);
		algo.negCoverTree.deleteGeneralizations(bs,4,0);
        	
		
		algo.negCoverTree.filterSpecializations();
		
		System.out.println("Negative cover FDs after the update");
	    algo.negCoverTree.printDependencies();
		
	    algo.posCoverTree = new FDTree(4);
	    algo.posCoverTree.addMostGeneralDependencies();
	    activePath = new BitSet();
	    algo.calculatePositiveCover(algo.negCoverTree, activePath);
		
        System.out.println("Popsitive FDs after removing {2,3}->1 and {2,3}->4 from the negatiove cover");
        algo.posCoverTree.printDependencies();
		*/
		
        
		

	}

}
