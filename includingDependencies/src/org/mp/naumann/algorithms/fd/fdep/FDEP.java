package org.mp.naumann.algorithms.fd.fdep;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.structures.FDTree;
import org.mp.naumann.algorithms.fd.structures.FDTreeElement;
import org.mp.naumann.algorithms.fd.structures.OpenBitSetFD;
import org.mp.naumann.algorithms.fd.utils.ValueComparator;

import fr.dauphine.lamsade.khalid.dynast.FdepAlgorithm;

import java.util.BitSet;
import java.util.List;

class FDEP {


    private int numAttributes;
    private ValueComparator valueComparator;

    public FDEP(int numAttributes, ValueComparator valueComparator) {
        this.numAttributes = numAttributes;
        this.valueComparator = valueComparator;
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
        FDTree negCoverTree = new FDTree(this.numAttributes, -1);
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
        diffAttrs.set(0, this.numAttributes);
        diffAttrs.andNot(equalAttrs);

        negCoverTree.addFunctionalDependency(equalAttrs, diffAttrs);
    }

    private FDTree calculatePositiveCover(FDTree negCoverTree) {
        FDTree posCoverTree = new FDTree(this.numAttributes, -1);
        posCoverTree.addMostGeneralDependencies();
        OpenBitSet activePath = new OpenBitSet();
 
        this.calculatePositiveCover(posCoverTree, negCoverTree, activePath);

        return posCoverTree;
    }

    private void calculatePositiveCover(FDTree posCoverTree, FDTreeElement negCoverSubtree, OpenBitSet activePath) {
        OpenBitSet fds = negCoverSubtree.getFds();
        for (int rhs = fds.nextSetBit(0); rhs >= 0; rhs = fds.nextSetBit(rhs + 1))
            this.specializePositiveCover(posCoverTree, activePath, rhs);

        if (negCoverSubtree.getChildren() != null) {
            for (int attr = 0; attr < this.numAttributes; attr++) {
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
            for (int attr = this.numAttributes - 1; attr >= 0; attr--) {
                if (!lhs.get(attr) && (attr != rhs)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsFdOrGeneralization(specLhs, rhs))
                        posCoverTree.addFunctionalDependency(specLhs, rhs);
                    specLhs.clear(attr);
                }
            }
        }
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
		
		
		
        System.out.println("Negative FDs");
        List<OpenBitSetFD> neg_fds = negCoverTree.getFunctionalDependencies();
        for (int i=0; i< neg_fds.size();i++)
        	System.out.println(neg_fds.get(i).toString(4));
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
        neg_fds = negCoverTree.getFunctionalDependencies();
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
