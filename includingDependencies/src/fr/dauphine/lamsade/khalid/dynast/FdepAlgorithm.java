package fr.dauphine.lamsade.khalid.dynast;

/*
 * Code extended by Khalid Belhajjame
 */

import java.util.ArrayList; 
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.algorithm_types.FunctionalDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.DatabaseConnectionGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
//import de.metanome.algorithms.tane.algorithm_helper.FDTree;
//import de.metanome.algorithms.tane.algorithm_helper.FDTreeElement;
import fr.dauphine.lamsade.khalid.dynast.util.DiffVector;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class FdepAlgorithm implements FunctionalDependencyAlgorithm,
        RelationalInputParameterAlgorithm,
        StringParameterAlgorithm {

    public static final String INPUT_SQL_CONNECTION = "DatabaseConnection";
    public static final String INPUT_TABLE_NAME = "Table_Name";
    public static final String INPUT_TAG = "Relational Input";


    private DatabaseConnectionGenerator databaseConnectionGenerator;
    private RelationalInputGenerator relationalInputGenerator;
    protected String tableName;
    protected List<String> columnNames;
    protected ObjectArrayList<ColumnIdentifier> columnIdentifiers;
    
    int possiblebitsetlistsize;
    
    // Evidence sets the first index is used for the attribute, and the second for the ids of the tuple
    protected ObjectArrayList<HashMap<Integer,DiffVector>> evidenceSets;
    
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
    HashMap<BitSet,Integer> bitesetMap = new HashMap<>();
    HashMap<BitSet,Integer> bitesetMap_insert = new HashMap<>();
    HashMap<BitSet,Integer> bitesetMap_delete = new HashMap<>();
    HashMap<BitSet,Integer> bitesetMap_after_delete = new HashMap<>();

    protected int numberAttributes;

    protected FDTree negCoverTree;
    protected FDTree posCoverTree;
    //protected ObjectArrayList<List<Integer>> tuples = new ObjectArrayList<List<Integer>>();
    
    // I changed the types of this structure to be able to deal with tuple deletion
    protected Map<Integer,List<Integer>> tuples = new HashMap<Integer,List<Integer>>();
    protected int tuple_ID =0;

    protected FunctionalDependencyResultReceiver fdResultReceiver;

    
    
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
    	System.out.print("2 to the power of "+this.getNumberAttributes()+" is "+Math.pow(2,this.getNumberAttributes()));
    	//for (int i=0; i<Math.pow(2,this.getNumberAttributes()); i++) {
    	Double max = Math.pow(2,this.getNumberAttributes());
    	for (int i=0; i<max; i++) {	
    		//System.out.println("Binary string"+Integer.toBinaryString(i*2));
    		//System.out.println("Bit set"+this.fromString(Integer.toBinaryString(i)));
    		System.out.println("Loops end in "+ (max -i)+" iterations");
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
    	for (BitSet name: this.bitesetMap.keySet()){
            String key = name.toString();
            Integer value = this.bitesetMap.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap: ");
    	
    }
    
    void mergeBitSetMaps_insert() {
    	
    	this.bitesetMap_insert.forEach(
    		    (key, value) -> this.bitesetMap.merge(key, value, (v1,v2) -> v1+v2));
    	
    	
    }
    
    void printBitSetMap_insert() {
    	System.out.println("Print BitSetMap_insert: ");
    	for (BitSet name: this.bitesetMap_insert.keySet()){
            String key = name.toString();
            Integer value = this.bitesetMap_insert.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap_insert: ");
    	
    }
    
    void printBitSetMap_delete() {
    	System.out.println("Print delete: ");
    	for (BitSet name: this.bitesetMap_delete.keySet()){
            String key = name.toString();
            Integer value = this.bitesetMap_delete.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap_delete: ");
    	
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
    
    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> requiredConfig = new ArrayList<>();
//		requiredConfig.add(new ConfigurationSpecificationSQLIterator(
//			INPUT_SQL_CONNECTION));
//		requiredConfig.add(new ConfigurationSpecificationString(INPUT_TABLE_NAME));
        requiredConfig.add(new ConfigurationRequirementRelationalInput(INPUT_TAG));

        return requiredConfig;
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) {
        if (identifier.equals(INPUT_TABLE_NAME)) {
            this.tableName = values[0];
        }
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) {
        if (identifier.equals(INPUT_TAG)) {
            this.relationalInputGenerator = values[0];
        }
    }

    @Override
    public void setResultReceiver(
            FunctionalDependencyResultReceiver resultReceiver) {
        fdResultReceiver = resultReceiver;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        initialize();
        
        System.out.println("Start constructing the negative cover");
        
        negativeCover();
        
        this.tuples = null;

        posCoverTree = new FDTree(numberAttributes,0);
        posCoverTree.addMostGeneralDependencies();
        BitSet activePath = new BitSet();
        calculatePositiveCover(negCoverTree, activePath);
//		posCoverTree.filterGeneralizations();
        addAllDependenciesToResultReceiver();
    }

    private void initialize() throws AlgorithmExecutionException, InputGenerationException, InputIterationException {
        loadData();
        setColumnIdentifiers();
    }

    /**
     * Calculate a set of fds, which do not cover the invalid dependency lhs -> a.
     */
    private void specializePositiveCover(BitSet lhs, int a) {
        BitSet specLhs = new BitSet();

        while (posCoverTree.getGeneralizationAndDelete(lhs, a, 0, specLhs)) {
            for (int attr = this.numberAttributes; attr > 0; attr--) {
                if (!lhs.get(attr) && (attr != a)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsGeneralization(specLhs, a, 0)) {
                        posCoverTree.addFunctionalDependency(specLhs, a);
                    }
                    specLhs.clear(attr);
                }
            }
            specLhs = new BitSet();
        } 
    }

    void calculatePositiveCover(FDTreeElement negCoverSubtree, BitSet activePath) {
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.isFd(attr - 1)) {
                specializePositiveCover(activePath, attr);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.getChild(attr - 1) != null) {
                activePath.set(attr);
                this.calculatePositiveCover(negCoverSubtree.getChild(attr - 1), activePath);
                activePath.clear(attr);
            } 
        }
    }
    

    /**
     * Calculate the negative Cover for the current relation.
     */
    void negativeCover() {
        negCoverTree = new FDTree(this.numberAttributes);
        for (int i = 0; i < tuples.size(); i++) {
            for (int j = i + 1; j < tuples.size(); j++) {
                violatedFds(tuples.get(i), tuples.get(j));
            }
        }
        
        System.out.println("Dependencies of the negative cover");
        negCoverTree.printDependencies();
        
        this.negCoverTree.filterSpecializations();
        
        System.out.println("Dependencies of the negative cover after filtering specializations");
        negCoverTree.printDependencies();
        
        
    }

    /**
     * Update the negative Cover for the current relation given new inserted tuples
     */
    void updateNegativeCoverGivenInsertion(List<BitSet> new_bit_sets) {
        
        BitSet diffAttr;
        BitSet equalAttr = new BitSet();
        equalAttr.set(1, this.numberAttributes + 1);
        
        for (int i=0; i<new_bit_sets.size();i++){

            equalAttr.set(1, this.numberAttributes + 1);
        	
        	diffAttr = new_bit_sets.get(i);
        	equalAttr.andNot(diffAttr);
        	
        	//System.out.println("Insert the following bitset to the negative cover: "+diffAttr);
        	//System.out.println("with the corresponding equalAttr: "+equalAttr);
        	
            for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
                negCoverTree.addFunctionalDependency(equalAttr, a); 
            }
            this.negCoverTree.filterSpecializations();
        	
            
        	
        }
        
        
    }
    
    /**
     * Update the negative Cover for the current relation given new inserted tuples
     */
    void updateNegativeCoverGivenDeletion(List<BitSet> new_bit_sets) {
        
    	this.negCoverTree = new FDTree(this.numberAttributes);
        BitSet diffAttr;
        BitSet equalAttr = new BitSet();
        equalAttr.set(1, this.numberAttributes + 1);
        
        for (int i=0; i<new_bit_sets.size();i++){

            equalAttr.set(1, this.numberAttributes + 1);
        	
        	diffAttr = new_bit_sets.get(i);
        	equalAttr.andNot(diffAttr);
        	
        	//System.out.println("Insert the following bitset to the negative cover: "+diffAttr);
        	//System.out.println("with the corresponding equalAttr: "+equalAttr);
        	
            for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
                negCoverTree.addFunctionalDependency(equalAttr, a);
                
            }
            this.negCoverTree.filterSpecializations();
        	
            
        	
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
 
    	System.out.println("Start by comparing tuple "+Arrays.toString(t1.toArray()) + " and "+Arrays.toString(t2.toArray()));
    	
    	BitSet equalAttr = new BitSet();
        equalAttr.set(1, this.numberAttributes + 1);
        BitSet diffAttr = new BitSet();
        for (int i = 0; i < t1.size(); i++) {
            Object val1 = t1.get(i);
            Object val2 = t2.get(i);
            // Handling of null values. Currently assuming NULL values are equal.
            if (val1 == null && val2 == null) {
                continue;
            } else if ((val1 == null && val2 != null) || !(val1.equals(val2))) {
                // BitSet start with 1 for first attribute
                diffAttr.set(i + 1);
            }
        }
        
        
        equalAttr.andNot(diffAttr);
        
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
        if (this.relationalInputGenerator != null) {
            ri = this.relationalInputGenerator.generateNewCopy();
        } else if (this.databaseConnectionGenerator != null && this.tableName != null) {
            String sql = "SELECT * FROM " + this.tableName;
            ri = this.databaseConnectionGenerator.generateRelationalInputFromSql(sql, this.tableName);
        } else if (this.relationalInputGenerator == null) {
         
        	this.tableName = "test";
        	this.columnNames = Arrays.asList(new String[]{"A", "B", "C", "D"});
        	this.numberAttributes = 4;
        	this.addTuple(Arrays.asList(new Integer[]{1,1, 1,1}));
        	this.addTuple(Arrays.asList(new Integer[]{1,2,2,1}));
        	this.addTuple(Arrays.asList(new Integer[]{2,1,1,2}));
        	
        }
        else {
            throw new AlgorithmConfigurationException("No input Generator set.");
        }
        
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

    private void addAllDependenciesToResultReceiver(FDTreeElement fds, BitSet activePath) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.fdResultReceiver == null) {
            return;
        }
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.isFd(attr - 1)) {
                int j = 0;
                ColumnIdentifier[] columns = new ColumnIdentifier[activePath.cardinality()];
                for (int i = activePath.nextSetBit(0); i >= 0; i = activePath.nextSetBit(i + 1)) {
                    columns[j++] = this.columnIdentifiers.get(i - 1);
                }
                ColumnCombination colCombination = new ColumnCombination(columns);
                FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get(attr - 1));
//				System.out.println(fdResult.toString());
                fdResultReceiver.receiveResult(fdResult);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.getChild(attr - 1) != null) {
                activePath.set(attr);
                this.addAllDependenciesToResultReceiver(fds.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }


    /**
     * Add all functional Dependencies to the FunctionalDependencyResultReceiver.
     * Do nothing if the object does not have a result receiver.
     *
     * @throws CouldNotReceiveResultException
     * @throws ColumnNameMismatchException 
     */
    void addAllDependenciesToResultReceiver() throws CouldNotReceiveResultException, ColumnNameMismatchException {
        if (this.fdResultReceiver == null) {
            return;
        }
        this.addAllDependenciesToResultReceiver(posCoverTree, new BitSet());
    }

	@Override
	public String getAuthors() {
		return "Jannik Marten, Jan-Peer Rudolph";
	}

	@Override
	public String getDescription() {
		return "Dependency Induction-based FD discovery";
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
		
		evidenceSets = new ObjectArrayList<HashMap<Integer,DiffVector>>();
		
		for (int i=0; i<this.numberAttributes; i++) {
			System.out.println("Evidence sets for attribute: "+i);
			
			HashMap<Integer,DiffVector> tmp = new HashMap<Integer,DiffVector>();
			
			for (Integer key_j : this.getTuples().keySet()) {
			
			//for (int j=0; j< this.getTuples().size(); j++) {
				DiffVector evs_j =  new DiffVector();
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
			
	    	 BitSet bs;
	    	 List<BitSet> bitsetlist = new ArrayList<BitSet>();
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new BitSet();
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i))
	    				bs.set(j+1);
	    		 }
	    		 if (!bitsetlist.contains(bs))
	    			 bitsetlist.add(bs);
	    		 if (bitsetlist.size() == this.possiblebitsetlistsize)
	    			 break; 
	    	 }
			
	    	for (int i=0;i<bitsetlist.size();i++) 
	    		this.bitesetMap_insert.merge(bitsetlist.get(0), 1, Integer::sum);
	
			
 			
		//}
		
	}
	
	
	/* 
	 * The following method update the bitsetmap following the insertion of new tuples
	 */
	public void deleteBitsets() {
		
		List<ArrayList<Boolean>> vecs;
		BitSet bs;
		List<BitSet> bitsetlist;
		BitSet emptybs = new BitSet();
		
		for (int k=0; k<this.deleted_tuples.size(); k++) {
			
			vecs = new ArrayList<ArrayList<Boolean>> ();
			
			//System.out.println("deleted_tuples.get(k): "+deleted_tuples.get(k));
			
			//System.out.println("Boolean Vectors that needs to be zipped given deletion: ");
			for (int j =0; j<this.numberAttributes; j++) {
				vecs.add(this.evidenceSets.get(j).get(this.deleted_tuples.get(k)).getVector());
				//System.out.println(Arrays.toString(this.evidenceSets.get(j).get(this.deleted_tuples.get(k)).getVector().toArray()));
			}
			
			bitsetlist = new ArrayList<BitSet>();
	    	 for (int i=0;i<vecs.get(0).size(); i++) {
	    		 bs = new BitSet();
	    		 for (int j=0;j<this.numberAttributes;j++) {
	    			if (vecs.get(j).get(i))
	    				bs.set(j+1);
	    		 }
	    		 if (!bitsetlist.contains(bs))
	    			 bitsetlist.add(bs);
	    		 if (bitsetlist.size() == this.possiblebitsetlistsize)
	    			 break; 
	    	 }
			
	    	 
	    	for (int i=0;i<bitsetlist.size();i++) {
	    		//System.out.println("bitsetlist.get(i): "+bitsetlist.get(i));
	    		//this.printBitSetMap();
	    		if (!bitsetlist.get(i).equals(emptybs)) {
	    			if (this.bitesetMap.get(bitsetlist.get(i)) == 1)
	    				this.bitesetMap.remove(bitsetlist.get(i));
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
	    	
		}
		//System.out.println("Bit Set Map after Deletion");
		//this.printBitSetMap();
		
	}

	public void initializeScales() {
		scales = new int[this.numberAttributes];
		for (int i=0; i<this.numberAttributes;i++)
			scales[i] = (int) Math.pow(2,i);
		
	}

	public void initializeInt2Bitset() {
		
		this.int2bitset = new HashMap<Integer,BitSet>();
		for(int i =0;i<Math.pow(2, this.numberAttributes); i++) {
			BitSet bs = BitSet.valueOf(new long[]{i*2});
			int2bitset.put(i, bs);
			
		}
		
		/*
		System.out.println("# Int2BitSet");
		for (Object objectName : int2bitset.keySet()) {
			   System.out.println(objectName);
			   System.out.println(int2bitset.get(objectName));
			 }
		*/	 
		

	}

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


	public static void main(String[] args) {
		
		FdepAlgorithm algo;
        algo = new FdepAlgorithm();
		System.out.println("An instance of the FDEP algorithm was created");
/*
		try {
			algo.loadData();
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/		
		
		/*
		try {
			algo.initialize();
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Finished the initialization");
		
		System.out.println("Names of columns");
        for (ColumnIdentifier column_id : algo.columnIdentifiers) {
        	System.out.println(column_id.toString());
        } 
        
        System.out.println("Build the negative cover");
        algo.negativeCover();
        */
		
		
		
		/*
		try {
			algo.execute();
		} catch (AlgorithmExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		algo.posCoverTree.printDependencies();
        */
		
		algo.numberAttributes = 4;
		algo.negCoverTree = new FDTree(4);
		algo.posCoverTree = new FDTree(4);
		BitSet bs = new BitSet();
		bs.set(1);
		bs.set(4);
		algo.negCoverTree.addFunctionalDependency(bs, 2);
		algo.negCoverTree.addFunctionalDependency(bs, 3);
		
		bs = new BitSet();
		bs.set(2);
		bs.set(3);
		algo.negCoverTree.addFunctionalDependency(bs, 1);
		algo.negCoverTree.addFunctionalDependency(bs, 4);
		
		bs = new BitSet();
		bs.set(3);
		algo.negCoverTree.addFunctionalDependency(bs, 4);
		algo.negCoverTree.filterSpecializations();
		
		
        algo.posCoverTree.addMostGeneralDependencies();
        BitSet activePath = new BitSet();
        
        algo.calculatePositiveCover(algo.negCoverTree, activePath);

        
        System.out.println("Negative FDs");
        algo.negCoverTree.printDependencies();
        

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
		
		
        
		

	}


}
