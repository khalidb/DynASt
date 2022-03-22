package fr.dauphine.lamsade.khalid.dynast.util;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

public class FDTreeManip {
	
    int[][] records;
    int numberAttributes;
	protected FDTree negCoverTree;
    protected FDTree posCoverTree;
    HashMap<OpenBitSet,Integer> bitsetMap;
    
    FDTreeManip(int numberAttributes){
    	this.numberAttributes = numberAttributes;
    	negCoverTree = new FDTree(this.numberAttributes, -1);
    	posCoverTree = new FDTree(this.numberAttributes, -1);
        this.bitsetMap = new HashMap<OpenBitSet,Integer>();
    }
    
    private ValueComparator valueComparator = new ValueComparator(true);
    
    
    private void updateNegCoverGivenDeletionOld(int[] record) {
    	
    	this.printBitSetMap();
        
    	ArrayList<OpenBitSet> non_fds = new ArrayList<OpenBitSet>(); 
    	OpenBitSet diffAttrs;
        for (int i = 0; i < records.length; i++) {
            if (records[i] != record) {
            	diffAttrs = new OpenBitSet(numberAttributes);
            	for (int j = 0; j < record.length; j++) {
                    if (!this.valueComparator.isEqual(record[j], records[i][j]))
                        diffAttrs.set(j);    
                }
            	
                this.bitsetMap.merge(diffAttrs, -1, Integer::sum);
                if (bitsetMap.get(diffAttrs) == 0) {
                	bitsetMap.remove(diffAttrs);
            	    non_fds.add(diffAttrs);
                }
            }
        }
        
        
        Collections.sort(non_fds, new SortOpenBitSet());
        this.removeViolatedFdsFromCover(non_fds);
    	
    	
    }
    

    
    private void updateNegCoverGivenDeletion(int[] record) {
    	
    	this.printBitSetMap();
        
    	ArrayList<OpenBitSet> non_fds = new ArrayList<OpenBitSet>(); 
    	OpenBitSet diffAttrs;
        for (int i = 0; i < records.length; i++) {
            if (records[i] != record) {
            	diffAttrs = new OpenBitSet(numberAttributes);
            	for (int j = 0; j < record.length; j++) {
                    if (!this.valueComparator.isEqual(record[j], records[i][j]))
                        diffAttrs.set(j);    
                }
            	
                this.bitsetMap.merge(diffAttrs, -1, Integer::sum);
                if (bitsetMap.get(diffAttrs) == 0) {
                	bitsetMap.remove(diffAttrs);
            	    non_fds.add(diffAttrs);
                }
            }
        }
        
        //Collections.sort(non_fds, new SortOpenBitSet());
        this.removeViolatedFdsFromCover(non_fds);
    	
    	
    }
    
    private void updateNegCoverGivenInsertion(int[] record) {
    	System.out.println("---- Insertion ----");
    	ArrayList<OpenBitSet> non_fds = new ArrayList<OpenBitSet>(); 
    	OpenBitSet diffAttrs, equalAttrs;
        for (int i = 0; i < records.length; i++) {
        	diffAttrs = new OpenBitSet(numberAttributes);
        	diffAttrs.clear(0, this.numberAttributes);
            if (records[i] != record) {
            	for (int j = 0; j < record.length; j++) {
                    if (!this.valueComparator.isEqual(record[j], records[i][j]))
                        diffAttrs.set(j);    
                }
            	System.out.println("diffAttrs: "+BitSetUtils.toString(diffAttrs,numberAttributes));
            	this.bitsetMap.merge(diffAttrs, 1, Integer::sum);
            	if (this.bitsetMap.get(diffAttrs) == 1) {
                	equalAttrs = new OpenBitSet(numberAttributes);
                	equalAttrs.set(0, this.numberAttributes);
                	equalAttrs.andNot(diffAttrs);
            		non_fds.add(diffAttrs);
            		negCoverTree.addFunctionalDependency(equalAttrs,diffAttrs);
            	}
            }
        }
    
        for (int i = 0; i< non_fds.size();i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	for (int j = 0; j<numberAttributes; j++) {
        		if (diffAttrs.get(j)) {
        			negCoverTree.addFunctionalDependency(equalAttrs,j);
        			this.specializePositiveCover_New(posCoverTree, equalAttrs, j);
        		}
        		
        	}
        	
        }
        	
        
        //Collections.sort(non_fds, new SortOpenBitSet());
        //this.addViolatedFdsToCoverGivenInsertion(negCoverTree, non_fds);
        
    	
    }
    
    
    private void addViolatedFdsToCoverGivenInsertion(FDTree negCoverTree, ArrayList<OpenBitSet> non_fds) {
        
    	for (int i = 0; i<non_fds.size();i++) {
    		
    	}
 /*   	
    	OpenBitSet equalAttrs = new OpenBitSet(t1.length);
        for (int i = 0; i < t1.length; i++) {
            if (this.valueComparator.isEqual(t1[i], t2[i]))
                equalAttrs.set(i);    
        }
        
        OpenBitSet diffAttrs = new OpenBitSet(t1.length);
        diffAttrs.set(0, this.numberAttributes);
        diffAttrs.andNot(equalAttrs);
        List<OpenBitSet> listbitset;
        
        this.bitsetMap.merge(diffAttrs, 1, Integer::sum);
        
        if (bitsetMap.get(diffAttrs) == 1) {
        	
        	for (int i=0; i< diffAttrs.size();i++) {
        		if (diffAttrs.get(i)) {
        			negCoverTree.printDependencies(4);
        			if (this.hasSpecializationFD(negCoverTree, equalAttrs, i))
        					System.out.println(BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+i+" has a specialization in the negative cover");
        			if (!this.hasSpecializationFD(negCoverTree, equalAttrs, i)) {
        				System.out.println("Add the non FD "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+i+" to the negative cover");
        				negCoverTree.removeFdAndGeneralizations(equalAttrs, i);
        				negCoverTree.addFunctionalDependency(equalAttrs, i);
        				posCoverTree.removeFdAndGeneralizations(equalAttrs, i);
        	            for (int attr = this.numberAttributes - 1; attr >= 0; attr--) {
        	                if (!equalAttrs.get(attr) && (attr != i)) {
        	                    equalAttrs.set(attr);
        	                    if (!posCoverTree.containsFdOrGeneralization(equalAttrs, i))
        	                        posCoverTree.addFunctionalDependency(equalAttrs, i);
        	                    equalAttrs.clear(attr);
        	                }
        	            }
        			}
        		}
        	}
        	
        }
        */
        
    }
    private void removeViolatedFdsFromCover(ArrayList<OpenBitSet> non_fds) {
    	
        OpenBitSet equalAttrs, diffAttrs, most_special_fd, lhs;
        List<OpenBitSet> specialization_fds;
        Set<OpenBitSet> new_fds;
        
        most_special_fd = new OpenBitSet(this.numberAttributes);
        most_special_fd.set(0,this.numberAttributes);
        
        System.out.println("Size of non_fds: "+non_fds.size());
        List<OpenBitSet> gen_fds;
        List<FDTreeElement> gen_elems;
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
        			negCoverTree.removeFunctionalDependency(equalAttrs, j); 
        			//System.out.println("Negative cover");
        			//negCoverTree.printDependencies(numberAttributes);
        		}
        	}
        }
        
        //Collections.sort(non_fds, new SortOpenBitSet());
        
        for(int i = 0; i<non_fds.size(); i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(this.numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	
        	//System.out.println("Deletion: case of equalAttrs "+BitSetUtils.toString(equalAttrs,numberAttributes)+" and diffAttrs "+BitSetUtils.toString(diffAttrs,numberAttributes));
        	
        	for (int j=0; j< diffAttrs.size();j++) {
        		if (diffAttrs.get(j)) {
        			//System.out.println("Remove "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" from the negative cover");
        			//negCoverTree.findTreeElement(equalAttrs).removeFd(j); // instead of removeFunctionalDependency(equalAttrs, j), which remove all generalizations
        			//System.out.println("Negative cover");
        			//negCoverTree.printDependencies(numberAttributes);
        			
        			 			
        			
        			if (this.hasFdSpecialization(negCoverTree, equalAttrs.clone(), j)) {
                    	//System.out.println(BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" has a specialization in the negative cover");
                    }
                    else {

                    	new_fds = this.getNewGeneralFds(negCoverTree, equalAttrs,j);
                    	Iterator value = new_fds.iterator();
                    	while (value.hasNext()) {
                    		lhs = (OpenBitSet) value.next();
                    		//System.out.println("New general FDs given the removal of the non-fd: "+BitSetUtils.toString(equalAttrs,this.numberAttributes)+"->"+j);
                    		posCoverTree.addFunctionalDependency(lhs,j);
                    		this.removeSpecializationsFromPositiveCover(lhs, j);
                    		//System.out.print("FDs of the positive cover");
                    		//posCoverTree.printDependencies(numberAttributes);
                    	}

                    }
        		}
        	
        	}	
        }
    }

    
  private void removeViolatedFdsFromCover_Old_2(ArrayList<OpenBitSet> non_fds) {
    	
        OpenBitSet equalAttrs, diffAttrs, most_special_fd;
        List<OpenBitSet> specialization_fds;
        List<OpenBitSet> new_fds;
        
        most_special_fd = new OpenBitSet(this.numberAttributes);
        most_special_fd.set(0,this.numberAttributes);
        
        System.out.println("Size of non_fds: "+non_fds.size());
        List<OpenBitSet> gen_fds;
        List<FDTreeElement> gen_elems;
        OpenBitSet emptyset = new OpenBitSet(this.numberAttributes);
        
        for(int i = 0; i<non_fds.size(); i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(this.numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	
        	System.out.println("Deletion: case of equalAttrs "+BitSetUtils.toString(equalAttrs,numberAttributes)+" and diffAttrs "+BitSetUtils.toString(diffAttrs,numberAttributes));
        	
        	for (int j=0; j< diffAttrs.size();j++) {
        		if (diffAttrs.get(j)) {
        			System.out.println("Remove "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" from the negative cover");
        			negCoverTree.findTreeElement(equalAttrs).removeFd(j); // instead of removeFunctionalDependency(equalAttrs, j), which remove all generalizations
        			System.out.println("Negative cover");
        			negCoverTree.printDependencies(numberAttributes);
        		}
        	}
        }
        
        Collections.sort(non_fds, new SortOpenBitSet());
        
        for(int i = 0; i<non_fds.size(); i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(this.numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	
        	System.out.println("Deletion: case of equalAttrs "+BitSetUtils.toString(equalAttrs,numberAttributes)+" and diffAttrs "+BitSetUtils.toString(diffAttrs,numberAttributes));
        	
        	for (int j=0; j< diffAttrs.size();j++) {
        		if (diffAttrs.get(j)) {
        			//System.out.println("Remove "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" from the negative cover");
        			//negCoverTree.findTreeElement(equalAttrs).removeFd(j); // instead of removeFunctionalDependency(equalAttrs, j), which remove all generalizations
        			//System.out.println("Negative cover");
        			//negCoverTree.printDependencies(numberAttributes);
        			
        			 			
        			
        			if (this.hasFdSpecialization(negCoverTree, equalAttrs.clone(), j)) {
                    	System.out.println(BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" has a specialization in the negative cover");
                    }
                    else {
                    	specialization_fds = negCoverTree.getFdAndGeneralizations(most_special_fd,j);
                    	System.out.println("size of specialization_fds: "+specialization_fds.size());
                    	for (int m=0;m<specialization_fds.size();m++) {
                    		System.out.println("specialization: "+BitSetUtils.toString(specialization_fds.get(m),this.numberAttributes));
                    	}
                    	
                    	
                    	new_fds = this.getNewGeneralFds(negCoverTree, equalAttrs,j,specialization_fds);
                    	
                    	if (new_fds.size() != 0) {
                    		System.out.println("New general FDs given the removal of the non-fd: "+BitSetUtils.toString(equalAttrs,this.numberAttributes)+"->"+j);
	                    	for (int k = 0; k< new_fds.size();k++) {
	                    		System.out.println(BitSetUtils.toString(new_fds.get(k),this.numberAttributes)+"->"+j);
	                    		posCoverTree.addFunctionalDependency(new_fds.get(k),j);
	                    		this.removeSpecializationsFromPositiveCover(new_fds.get(k), j);
	                    		
	                    		System.out.print("FDs of the positive cover");
	                    		posCoverTree.printDependencies(numberAttributes);
	                    	}
                    	}
                    	else {
                    		this.removeSpecializationsFromPositiveCover(emptyset, j);
                    		posCoverTree.addFunctionalDependency(emptyset.clone(),j);
                    		System.out.print("FDs of the positive cover");
                    		posCoverTree.printDependencies(numberAttributes);                    		
                    	}

                    }
        		}
        	
        	}	
        }
    }


    
    // Updated on the 17th of August 2020
    private void removeViolatedFdsFromCover_Old2(ArrayList<OpenBitSet> non_fds) {
    	
        OpenBitSet equalAttrs, diffAttrs;
        
        System.out.println("Size of non_fds: "+non_fds.size());
        List<OpenBitSet> gen_fds;
        List<FDTreeElement> gen_elems;
        OpenBitSet emptyset = new OpenBitSet(this.numberAttributes);
        
        for(int i = 0; i<non_fds.size(); i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(this.numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	
        	System.out.println("Deletion: case of equalAttrs "+BitSetUtils.toString(equalAttrs,numberAttributes)+" and diffAttrs "+BitSetUtils.toString(diffAttrs,numberAttributes));
        	
        	for (int j=0; j< diffAttrs.size();j++) {
        		if (diffAttrs.get(j)) {
        			System.out.println("Remove "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" from the negative cover");
        			negCoverTree.findTreeElement(equalAttrs).removeFd(j); // instead of removeFunctionalDependency(equalAttrs, j), which remove all generalizations
        			System.out.println("Negative cover");
        			negCoverTree.printDependencies(numberAttributes);
        			
        			//comment it out
        			System.out.print("Is the rhs attribute still set in the FDTree element: ");
        			System.out.println(negCoverTree.findTreeElement(equalAttrs).hasRhsAttribute(j));
        			//comment it out
        			System.out.print("Is is still a non-FDs: ");
        			System.out.println(negCoverTree.findTreeElement(equalAttrs).isFd(j));
        			System.out.println("FD and Generalizations of "+BitSetUtils.toString(equalAttrs,this.numberAttributes)+"->"+j);
        			List<OpenBitSet> gens = negCoverTree.getFdAndGeneralizations(equalAttrs,j);
        			for (int f=0;f<gens.size();f++)
        				System.out.println(BitSetUtils.toString(gens.get(f), this.numberAttributes));
        			
        			
        			if (this.hasFdSpecialization(negCoverTree, equalAttrs.clone(), j)) {
                    	System.out.println(BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" has a specialization in the negative cover");
                    }
                    else {
                    	System.out.println("The non FD "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" does not have a specialization in the negatove cover");
                    	System.out.println("Getgeneralizations from the negative cover");
                    	gen_fds = negCoverTree.getNNMGeneralizations(equalAttrs,j);
                    	
                    	//comment it out
                    	System.out.println("size of gen_fds: "+gen_fds.size());
                    	for(int p=0;p<gen_fds.size();p++)
                    		System.out.println("gen fds: "+BitSetUtils.toString(gen_fds.get(p),this.numberAttributes));
                    	
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
    
    private void removeViolatedFdsFromCoverOld(ArrayList<OpenBitSet> non_fds) {
    	
        OpenBitSet equalAttrs, diffAttrs;
        
        System.out.println("Size of non_fds: "+non_fds.size());
       
        
        for(int i = 0; i<non_fds.size(); i++) {
        	diffAttrs = non_fds.get(i);
        	equalAttrs = new OpenBitSet(this.numberAttributes);
        	equalAttrs.set(0, this.numberAttributes);
        	equalAttrs.andNot(diffAttrs);
        	
        	System.out.println("Deletion: case of equalAttrs "+BitSetUtils.toString(equalAttrs,numberAttributes)+" and diffAttrs "+BitSetUtils.toString(diffAttrs,numberAttributes));
        	
        	for (int j=0; j< diffAttrs.size();j++) {
        		if (diffAttrs.get(j)) {
        			System.out.println("Remove "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" from the negative cover");
        			negCoverTree.findTreeElement(equalAttrs).removeFd(j); // instead of removeFunctionalDependency(equalAttrs, j), which remove all generalizations
        			System.out.println("Negative cover");
        			negCoverTree.printDependencies(numberAttributes);
        			if (this.hasFdSpecialization(negCoverTree, equalAttrs.clone(), j)) {
                    	System.out.println(BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" has a specialization in the negative cover");
                    }
                    else {
                    	System.out.println("The non FD "+BitSetUtils.toString(equalAttrs,numberAttributes)+"->"+j+" does not have a specialization in the negatove cover");
                    	System.out.println("Add such an FD in the posCoverTree");
                    	this.removeSpecializationsFromPositiveCover(equalAttrs, j);
                    	posCoverTree.addFunctionalDependency(equalAttrs, j);
                    }
        		}
        	
        	}	
        }
    }    
    
    /* @Khalid, I added this method to check if an FD has a specialization in the FD tree */
    
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
    

    public Set<OpenBitSet> getNewGeneralFds(FDTree nctree, OpenBitSet lhs, int rhs) {
    	
    	//System.out.println("Call geNewgeneralFds");
    	Set<OpenBitSet> parents = new HashSet<OpenBitSet>();
    	FDTreeElement f_ele;
    	
    	if (lhs.equals(new OpenBitSet(this.numberAttributes))) {
    		f_ele = nctree.findTreeElement(lhs);
    		if ((f_ele == null) || (!f_ele.isFd(rhs) && !f_ele.hasRhsAttribute(rhs)))
    			parents.add(lhs.clone());
    	}
    	else {
	    	int currentLhsAttr = lhs.nextSetBit(0);
	    	//System.out.println("currentLhsAttr: "+currentLhsAttr);
	    	while(currentLhsAttr >= 0) {
	    		lhs.clear(currentLhsAttr);
	    		//System.out.println("lhs: "+BitSetUtils.toString(lhs,this.numberAttributes));
	    		f_ele = nctree.findTreeElement(lhs);
	    		//System.out.println("f_ele: "+f_ele);
	    		if ((f_ele == null) || (!f_ele.isFd(rhs) && !f_ele.hasRhsAttribute(rhs))) {
	    			parents.addAll(getNewGeneralFds(nctree,lhs,rhs));
	    		}
	    		lhs.set(currentLhsAttr);
	    		currentLhsAttr = lhs.nextSetBit(currentLhsAttr+1);
	    	}
    	}
    	
    	if (parents.size() == 0) 
    		parents.add(lhs.clone());
    	
    		
    	return parents;
    }
    
    public List<OpenBitSet> getNewGeneralFds(FDTree cover, OpenBitSet lhs, int rhs, List<OpenBitSet> specialisation_fds) {
    	List<OpenBitSet> foundLhs = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet(this.numberAttributes);
        int nextLhsAttr = 0;
        System.out.println("Call of getNewGeneralFds with lhs = "+BitSetUtils.toString(lhs,this.numberAttributes)+" and the following specialization_fds");
        for (int j=0; j<specialisation_fds.size();j++)
        	System.out.println(BitSetUtils.toString(specialisation_fds.get(j),this.numberAttributes));
        System.out.println("size of specialization_fds: "+specialisation_fds.size());
        
        
        this.getNewGeneralFds(cover, lhs, rhs, nextLhsAttr, currentLhs, foundLhs, specialisation_fds);
        System.out.println("-----------");
        System.out.println("New General Fds found before return of getNewGeneralFds() are");
        for(int i=0;i<foundLhs.size();i++)
        	System.out.println(BitSetUtils.toString(foundLhs.get(i),this.numberAttributes));
        System.out.println("-----------");
        return foundLhs;
    }
    
    void getNewGeneralFds(FDTree cover, OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet currentLhs,
            List<OpenBitSet> foundLhs, List<OpenBitSet> specialization_fds) {
    	
    	int nextLhsAttr;
    	boolean has_specialization;
    	if ((currentLhs.cardinality() == 0) && (specialization_fds.size() == 0)) {
    		System.out.println("Add "+BitSetUtils.toString(currentLhs,this.numberAttributes)+"->"+rhs+" to the new general Fds of "+BitSetUtils.toString(lhs,this.numberAttributes)+"->"+rhs);
    		foundLhs.add(currentLhs.clone());
    		return;	
    	}
    	else {
    	if ((currentLhs.cardinality() == 0) && (specialization_fds != null)) {
    		currentLhsAttr = lhs.nextSetBit(currentLhsAttr);
    		while (currentLhsAttr >= 0) {
    			currentLhs.set(currentLhsAttr);
    			getNewGeneralFds(cover,lhs,rhs,currentLhsAttr,currentLhs,foundLhs,specialization_fds);
    			currentLhs.clear(currentLhsAttr);
    			currentLhsAttr = lhs.nextSetBit(currentLhsAttr+1);
    		}
    		
    	}
    	else {
    		
    		has_specialization = this.hasSpecializationIn(currentLhs,specialization_fds);
    		
    		if (has_specialization) {
    			currentLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);
    			while (currentLhsAttr >= 0) {
    				currentLhs.set(currentLhsAttr);
    				System.out.println("Call getNewGeneralFds with lhs "+BitSetUtils.toString(lhs,this.numberAttributes)+" and currentLhs: "+BitSetUtils.toString(currentLhs,this.numberAttributes));
    				System.out.println("currentLhsAttr: "+currentLhsAttr);
    				getNewGeneralFds(cover,lhs,rhs,currentLhsAttr,currentLhs,foundLhs,specialization_fds);
    				currentLhs.clear(currentLhsAttr);
    				currentLhsAttr = lhs.nextSetBit(currentLhsAttr + 1);;
    				System.out.println("currentLhsAttr at the end of the loop: "+currentLhsAttr);

    				
    			}
    		}
    		else {
    			if (!this.hasGeneralizationIn(currentLhs, foundLhs)) {
    				System.out.println("Add "+BitSetUtils.toString(currentLhs,this.numberAttributes)+"->"+rhs+" to the new general Fds of "+BitSetUtils.toString(lhs,this.numberAttributes)+"->"+rhs);
    				this.removeSpecializations(currentLhs,foundLhs);
    				foundLhs.add(currentLhs.clone());
    			}
        		return;
    		}	
    		
    	}
    	
    	}
    }
    
    public List<OpenBitSet> removeSpecializations(OpenBitSet lhs, List<OpenBitSet> fds) {
    	
    	for(int i=fds.size() - 1;i==0;i--) {	
    		if (OpenBitSet.intersectionCount(lhs,fds.get(i)) == lhs.cardinality())
    			fds.remove(i);	
    	}
    	
    	return fds;
    }
    
    public boolean hasSpecializationIn(OpenBitSet lhs, List<OpenBitSet> fds) {
    	
    	
    	boolean found = false;
    	int intersection;
    	int lhs_card;
    	for (int i = 0; i< fds.size(); i++) {
    		intersection = (int) OpenBitSet.intersectionCount(lhs,fds.get(i)) ;
    		lhs_card = (int) lhs.cardinality();
    		if (intersection == lhs_card) {
    			found = true;
    			break;
    		}

    	}
    	
    	return found;
    	
    }
    
    public boolean hasGeneralizationIn(OpenBitSet lhs, List<OpenBitSet> fds) {
    	
    	
    	boolean found = false;
    	int intersection;
    	int lhs_card;
    	for (int i = 0; i< fds.size(); i++) {
    		intersection = (int) OpenBitSet.unionCount(lhs,fds.get(i)) ;
    		lhs_card = (int) lhs.cardinality();
    		if (intersection == lhs_card) {
    			found = true;
    			break;
    		}

    	}
    	
    	return found;
    	
    }

    
    public boolean hasSpecializationFD(FDTree fdTree, OpenBitSet lhs, int rhs) {
    	
    	System.out.println("Does "+BitSetUtils.toString(lhs,numberAttributes)+"->"+rhs+" has a specialization");
    	FDTreeElement fd_element = fdTree.findTreeElement(lhs);
    	if (fd_element == null) {
    		System.out.println("fdTree.findTreeElement("+BitSetUtils.toString(lhs,numberAttributes)+") is null");
    		return false;
    	}
    	FDTreeElement[] fd_element_children = fd_element.children;
    	
        if (fd_element_children == null) {
        	System.out.println("No children founr for fdTree.findTreeElement("+BitSetUtils.toString(lhs,numberAttributes)+")");
            return false;
        }
        for (int i= 0; i < fd_element_children.length; i++)
        	if (fd_element_children[i] != null) {
        		System.out.println("Examining the fd_element_child"+BitSetUtils.toString(fd_element_children[i].getFds(),numberAttributes));
        		if (fd_element_children[i].hasRhsAttribute(rhs))
        			return true;
        	}
    	return false;
    }
    
    private void calculateNegativeCover() {
        for (int i = 0; i < records.length; i++)
            for (int j = i + 1; j < records.length; j++)
                this.addViolatedFdsToCover(records[i], records[j], negCoverTree);
    }
    
    
    
    private void addViolatedFdsToCover(int[] t1, int[] t2, FDTree negCoverTree) {
        OpenBitSet equalAttrs = new OpenBitSet(t1.length);
        for (int i = 0; i < t1.length; i++) {
            if (this.valueComparator.isEqual(t1[i], t2[i]))
                equalAttrs.set(i);
        }

        OpenBitSet diffAttrs = new OpenBitSet(t1.length);
        diffAttrs.set(0, this.numberAttributes);
        diffAttrs.andNot(equalAttrs);
        
        this.bitsetMap.merge(diffAttrs, 1, Integer::sum);

        if (bitsetMap.get(diffAttrs) == 1) {
        	negCoverTree.addFunctionalDependency(equalAttrs,diffAttrs);
        	/*
        	for (int i = 0; i<numberAttributes; i++) {
        		if (diffAttrs.get(i)) 
        			if (!this.hasSpecializationFD(negCoverTree, equalAttrs, i)){
        				negCoverTree.removeFdAndGeneralizations(equalAttrs, i);
        				negCoverTree.addFunctionalDependency(equalAttrs, i);
        			}
        	}
        	*/
        }
    }
    
    public void calculatePositiveCover() {
        posCoverTree = new FDTree(this.numberAttributes, -1);
        posCoverTree.addMostGeneralDependencies();
        OpenBitSet activePath = new OpenBitSet(this.numberAttributes);
 
        this.calculatePositiveCover(posCoverTree, negCoverTree, activePath);

    }

    private void calculatePositiveCover(FDTree posCoverTree, FDTreeElement negCoverSubtree, OpenBitSet activePath) {
        OpenBitSet fds = negCoverSubtree.getFds();
        System.out.println("Invocation of CalculatePositiveCover with fds: "+BitSetUtils.toString(fds,numberAttributes));
        for (int rhs = fds.nextSetBit(0); rhs >= 0; rhs = fds.nextSetBit(rhs + 1)) {
        	System.out.println("Call spacialize positive cover: with activatePath: "+BitSetUtils.toString(activePath,numberAttributes)+" and rhs: "+rhs);
            this.specializePositiveCover_New(posCoverTree, activePath, rhs);
        }

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
    
    public void updatePosCoverGivenDeletion(OpenBitSet lhs, int rhs) {
    	System.out.println("Invoke update poscovertree given deletion: "+BitSetUtils.toString(lhs,numberAttributes)+"->"+rhs);
    	if (!posCoverTree.containsFdOrGeneralization(lhs, rhs)) {
    	 System.out.print("The tree does not contains the FD or one of its generalization, and therefore added");
    	 this.posCoverTree.addFunctionalDependency(lhs, rhs);
    	}
   
    	//this.removeSpecialization(lhs, rhs);
    	
    }

    private void specializePositiveCover_New(FDTree posCoverTree, OpenBitSet lhs, int rhs) {
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
    
    private void removeSpecializationsFromPositiveCover(OpenBitSet lhs, int rhs) {
        /*
    	List<OpenBitSet> specLhss = null;
        specLhss = posCoverTree.getFdAndGeneralizations(lhs, rhs);
        for (OpenBitSet specLhs : specLhss) {
            posCoverTree.removeFunctionalDependency(specLhs, rhs);
        }*/
    	
    	System.out.println("removeSpecialization is called with lhs: "+BitSetUtils.toString(lhs,numberAttributes)+" and rhs: "+rhs);
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
    
    
    void printBitSetMap() {
    	System.out.println("Print BitSetMap: ");
    	System.out.println("number of attributes: "+this.numberAttributes);
    	for (OpenBitSet name: this.bitsetMap.keySet()){
            String key = BitSetUtils.toString(name,this.numberAttributes);
            Integer value = this.bitsetMap.get(name);  
            System.out.println(key + " " + value);  
    	} 
    	System.out.println("Finished printing BitSetMap: ");
    	
    }
    
    public void justToSaveOledMain() {
    	
		int numberAttributes = 4;
		FDTreeManip fdm = new FDTreeManip(numberAttributes);
		FDTree fdt = new FDTree(numberAttributes,-1);
		OpenBitSet lhs = new OpenBitSet(4);
		lhs.set(0);lhs.set(3);
		OpenBitSet rhs = new OpenBitSet(4);
		rhs.set(0, numberAttributes);
		rhs.andNot(lhs);
		// add the FD 1001 -> 2 to the FD tree
		System.out.println("Add functionalities dependencies: "+BitSetUtils.toString(lhs,numberAttributes)+"->"+BitSetUtils.toString(rhs,numberAttributes));
		fdt.addFunctionalDependency(lhs, rhs);
		fdt.printDependencies(numberAttributes);
		
		//fdm.printChildren(fdt,0);
		
		//System.out.println("fdt.findTreeElement(lhs): "+BitSetUtils.toString(fdt.findTreeElement(lhs).getFds(),numberAttributes)+" for lhs: "+BitSetUtils.toString(lhs,numberAttributes));
		
		OpenBitSet lhs1 = new OpenBitSet(4);
		int rhs1 = 1;
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(0);
		lhs1.set(3);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(1);
		lhs1.set(2);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		
		System.out.println("==============");
		
		lhs1 = new OpenBitSet(4);
		rhs1 = 2;
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(0);
		lhs1.set(3);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(1);
		lhs1.set(2);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));

		
		System.out.println("==============");
		lhs1 = new OpenBitSet(4);
		rhs1 = 0;
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(0);
		lhs1.set(3);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(1);
		lhs1.set(2);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
	
		System.out.println("==============");
		lhs1 = new OpenBitSet(4);
		rhs1 = 3;
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(0);
		lhs1.set(3);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(0);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.clear(1);
		lhs1.set(2);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));
		lhs1.set(1);
		System.out.println("Does "+BitSetUtils.toString(lhs1,numberAttributes)+"-> "+rhs1+" has a specialization: "+ fdm.hasFdSpecialization(fdt, lhs1, rhs1));

    	/*
		    	   		int numberAttributes = 4;
		FDTreeManip fdm = new FDTreeManip(numberAttributes);
		FDTree fdt = new FDTree(numberAttributes,-1);
		OpenBitSet lhs = new OpenBitSet(4);
		lhs.set(0);lhs.set(3);
		OpenBitSet rhs = new OpenBitSet(4);
		rhs.set(0, numberAttributes);
		rhs.andNot(lhs);
		// add the FD 1001 -> 2 to the FD tree
		System.out.println("Add functionalities dependencies: "+BitSetUtils.toString(lhs,numberAttributes)+"->"+BitSetUtils.toString(rhs,numberAttributes));
		fdt.addFunctionalDependency(lhs, rhs);
		fdt.printDependencies(numberAttributes);
		
		OpenBitSet lhs1 = new OpenBitSet(4);
		lhs1.set(0);
		System.out.println("Remove the functional dependency: "+BitSetUtils.toString(lhs1,numberAttributes)+"->"+1);
		//fdt.removeFunctionalDependency(lhs1, 1);
		fdt.addFunctionalDependency(lhs1, 1);
		System.out.println("Add 1000 -> 1");
		fdt.printDependencies(numberAttributes);
		System.out.println("Remove 1001 -> 1");
		fdt.removeFdAndGeneralizations(lhs, 1);
		fdt.printDependencies(numberAttributes);
		
		FDTreeElement ele = fdt.findTreeElement(lhs1);
		if ((ele != null) && (ele.hasRhsAttribute(1)))
			System.out.println(BitSetUtils.toString(lhs1,numberAttributes)+"->"+1+" is a functional dependency");
		else
			System.out.println(BitSetUtils.toString(lhs1,numberAttributes)+"->"+1+" is not a functional dependency");
		
		fdt.removeFunctionalDependency(lhs, 1);
		//fdt.findTreeElement(lhs).removeRhsAttribute(1);
		fdt.printDependencies(numberAttributes);
		
		ele = fdt.findTreeElement(lhs1);
		if ((ele != null) && (ele.hasRhsAttribute(1)))
			System.out.println(BitSetUtils.toString(lhs1,numberAttributes)+"->"+1+" is a functional dependency");
		else
			System.out.println(BitSetUtils.toString(lhs1,numberAttributes)+"->"+1+" is not a functional dependency");   	  


    	 */
		
		
		/* Sorting open bit sets 
		 
		 		ArrayList<OpenBitSet> non_fds = new ArrayList<OpenBitSet>(); 
		OpenBitSet o1 = new OpenBitSet(numberAttributes);
		o1.set(0);o1.set(3);
		OpenBitSet o2 = new OpenBitSet(numberAttributes);
		o2.set(0);o2.set(1);o2.set(2);o2.set(3);
		OpenBitSet o3 = new OpenBitSet(numberAttributes);
		o3.set(0);o3.set(1);
		OpenBitSet o4 = new OpenBitSet(numberAttributes);
		o4.set(1);o4.set(2);o4.set(3);
		OpenBitSet o5 = new OpenBitSet(numberAttributes);
		o5.set(1);o5.set(3);
		OpenBitSet o6 = new OpenBitSet(numberAttributes);
		o6.set(2);
		OpenBitSet o7 = new OpenBitSet(numberAttributes);
		
		non_fds.add(o1); non_fds.add(o2);non_fds.add(o3);non_fds.add(o4);
		non_fds.add(o5);non_fds.add(o6);non_fds.add(o7);
		
		Collections.sort(non_fds, new SortOpenBitSet());

		System.out.println("\nSorted non FDs"); 
        for (int i=0; i<non_fds.size(); i++) 
            System.out.println(BitSetUtils.toString(non_fds.get(i),numberAttributes)); 

		 
		 */
    }
    
    public void printChildren(FDTreeElement fd_element, int level) {
    	
    	
    	//System.out.println("fd_element: "+BitSetUtils.toString(fd_element.+" for level "+level);
    	System.out.println("fd_element is "+BitSetUtils.toString(fd_element.getFds(),numberAttributes)+" in level "+level);
    	for (int i = 0; i<numberAttributes; i++)
    		if (fd_element.hasRhsAttribute(i))
    			System.out.println("has RHS attribute: "+i);
    	FDTreeElement[] fd_element_children = fd_element.getChildren();
    	if (fd_element_children != null) {
    		level++;
    		for (int i= 0; i < fd_element_children.length; i++) {
    			System.out.println("i: "+i);
    			if (fd_element_children[i] != null) {
    				System.out.println("i: "+i+" is not null");
    				this.printChildren(fd_element_children[i], level);
    			}
    		}
    	}
    	}
    



    
	public static void old_main(String[] args) {
		
		int numberAttributes = 4;
		FDTreeManip fdm = new FDTreeManip(numberAttributes);
		
		
		int[] record1 = new int[] {1, 1, 1, 1};
		int[] record2 = new int[] {1, 2, 2, 1};
		int[] record4 = new int[] {1, 1, 2, 2};
		int[][] records = new int[][] {record1, record2, record4};
		fdm.records = records;
	
		
		fdm.calculateNegativeCover();
		System.out.println("Initial non fds");
		fdm.negCoverTree.printDependencies(4);
		
		fdm.printBitSetMap();
		
		fdm.calculatePositiveCover();
		
		System.out.println("Initial Fds");
		fdm.posCoverTree.printDependencies(4);
		
		OpenBitSet lhs1 = new OpenBitSet(fdm.numberAttributes);
		lhs1.set(0);lhs1.set(3);
		fdm.negCoverTree.removeFunctionalDependency(lhs1, 1);
		fdm.negCoverTree.removeFunctionalDependency(lhs1, 2);
		
		
		OpenBitSet lhs2 = new OpenBitSet(fdm.numberAttributes);
		lhs2.set(0);lhs2.set(2);
		fdm.negCoverTree.removeFunctionalDependency(lhs2, 1);
		fdm.negCoverTree.removeFunctionalDependency(lhs2, 3);
		
		System.out.println("FDs of the negative cover");
		fdm.negCoverTree.printDependencies(4);
		
		Set<OpenBitSet> parents = fdm.getNewGeneralFds(fdm.negCoverTree, lhs1, 1);
		System.out.println("-- parents of : "+BitSetUtils.toString(lhs1,fdm.numberAttributes)+" given the rhs "+1);
		Iterator value = parents.iterator();
		while (value.hasNext())
			System.out.println(BitSetUtils.toString((OpenBitSet) value.next(),fdm.numberAttributes));
		

		parents = fdm.getNewGeneralFds(fdm.negCoverTree, lhs1, 2);
		System.out.println("-- parents of : "+BitSetUtils.toString(lhs1,fdm.numberAttributes)+" given the rhs "+2);
		value = parents.iterator();
		while (value.hasNext())
			System.out.println(BitSetUtils.toString((OpenBitSet) value.next(),fdm.numberAttributes));

		

	}
	
    public static void  main(String[] args) {
		int numberAttributes = 4;
		FDTreeManip fdm = new FDTreeManip(numberAttributes);
		
		
		int[] record1 = new int[] {1, 1, 1, 1};
		int[] record2 = new int[] {1, 2, 2, 1};
		int[] record3 = new int[] {2, 1, 1, 2};
		int[] record4 = new int[] {1, 1, 2, 2};
		int[][] records = new int[][] {record1, record2, record3};
		fdm.records = records;
	
		
		fdm.calculateNegativeCover();
		System.out.println("Initial non fds");
		fdm.negCoverTree.printDependencies(4);
		
		fdm.printBitSetMap();
		
		fdm.calculatePositiveCover();
		
		System.out.println("Initial Fds");
		fdm.posCoverTree.printDependencies(4);
		
		OpenBitSet most_specific_lhs = new OpenBitSet(numberAttributes);
		most_specific_lhs.set(0,numberAttributes);
		fdm.negCoverTree.ge
		

		
		/*
		System.out.println("-----Remove Record: "+Arrays.toString(record3));
		Instant start_1, end_1;
		start_1 = Instant.now();
		fdm.updateNegCoverGivenDeletion(record3);
		end_1 = Instant.now();
		
		
		
		System.out.println("Negative cover: Non fds after deletion");
		fdm.negCoverTree.printDependencies(4);
		System.out.println("Positive cover: Fds after deletion");
		fdm.posCoverTree.printDependencies(4);
		
		
		
		
	
		System.out.println("-----Insert Record: "+Arrays.toString(record4));
		
		records[2] = record4;
		
		fdm.updateNegCoverGivenInsertion(record4);
		System.out.println("Non fds after insertion");
		fdm.negCoverTree.printDependencies(4);
		System.out.println("Fds after insertion");
		fdm.posCoverTree.printDependencies(4);
		
		
		System.out.println("-----remove Record: "+Arrays.toString(record2));
		
		records[2] = record4;
		Instant start_2, end_2;
		start_2 = Instant.now();
		fdm.updateNegCoverGivenDeletion(record2);
		end_2 = Instant.now();
		System.out.println("Non fds after Deletion");
		fdm.negCoverTree.printDependencies(4);
		System.out.println("Fds after Deletion");
		fdm.posCoverTree.printDependencies(4);
		
		System.out.println("Deletion of record 3 took: "+Duration.between(start_1, end_1).toMillis());
		System.out.println("Deletion of record 2 took: "+Duration.between(start_2, end_2).toMillis());
		*/
    }

}
