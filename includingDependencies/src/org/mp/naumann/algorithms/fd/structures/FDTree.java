package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.database.data.ColumnIdentifier;

public class FDTree extends FDTreeElement {

    private int depth = 0;
    private int maxDepth;

    public FDTree(int numAttributes, int maxDepth) {
        super(numAttributes);
        this.maxDepth = maxDepth;
        this.children = new FDTreeElement[numAttributes];
    }

    public int getDepth() {
        return this.depth;
    }

    public int getMaxDepth() {
        return this.maxDepth;
    }

    @Override
    public String toString() {
        return "[" + this.depth + " depth, " + this.maxDepth + " maxDepth]";
    }

    public void trim(int newDepth) {
        this.trimRecursive(0, newDepth);
        this.depth = newDepth;
        this.maxDepth = newDepth;
    }

    public void addMostGeneralDependencies() {
        this.rhsAttributes.set(0, this.numAttributes);
        this.rhsFds.set(0, this.numAttributes);
    }

    public FDTreeElement addFunctionalDependency(OpenBitSet lhs, int rhs) {
        return addFunctionalDependency(lhs, this, n -> n.addRhsAttribute(rhs), n -> n.markFd(rhs), false);
    }
    
    public List<OpenBitSet> getGeneralizations(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> foundLhs = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet(this.numAttributes);
        int nextLhsAttr = lhs.nextSetBit(0);
        this.getGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
        return foundLhs;
    }
    
    public List<OpenBitSet> getNNMGeneralizations(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> foundLhs = new ArrayList<>();
        OpenBitSet currentLhs = lhs.clone();
        int nextLhsAttr = 0;
        this.getNNMGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
        return foundLhs;
    }
    
    void getNNMGeneralizations(OpenBitSet lhs, int rhs, int currentLhsAttr, OpenBitSet currentLhs,
            List<OpenBitSet> foundLhs) {
    	
    	int nextLhsAttr;
    	
    	FDTreeElement fde = this.findTreeElement(currentLhs);
		if (fde.hasRhsAttribute(rhs) && !currentLhs.equals(lhs)) {
			foundLhs.add(currentLhs);
			return;
		}
		else {
			if (currentLhs.equals(new OpenBitSet(this.numAttributes)))
				return;
			else
				while (currentLhsAttr >= 0) {
					currentLhs.clear(currentLhsAttr);
					this.getNNMGeneralizations(lhs,rhs,currentLhsAttr,currentLhs, foundLhs);
					currentLhs.set(currentLhsAttr);
					currentLhsAttr = lhs.nextSetBit(currentLhsAttr);
				}
		}	
    }

    public FDTreeElement addFunctionalDependency(OpenBitSet lhs, OpenBitSet rhs) {
        return addFunctionalDependency(lhs, this, n -> n.addRhsAttributes(rhs), n -> n.markFds(rhs), false);
    }

    public FDTreeElement addFunctionalDependencyGetIfNew(OpenBitSet lhs, int rhs) {
        return addFunctionalDependency(lhs, this, n -> n.addRhsAttribute(rhs), n -> n.markFd(rhs), true);
    }

    private FDTreeElement addFunctionalDependency(OpenBitSet lhs, FDTreeElement startNode,
                                                  Consumer<FDTreeElement> addRhsConsumer, Consumer<FDTreeElement> markFdConsumer,
                                                  boolean onlyNew) {
    	/*
    	System.out.println("Number of attributes: "+this.numAttributes);
    	System.out.println("Start Node Number of attributes: "+startNode.numAttributes);
    	System.out.println("lhs: "+BitSetUtils.toString(lhs));
    	*/
    	
        FDTreeElement currentNode = startNode;
        addRhsConsumer.accept(currentNode);
        int lhsLength = 0;
        boolean isNew = !onlyNew;
        for (int i = lhs.nextSetBit(0); i >= 0; i = lhs.nextSetBit(i + 1)) {
            lhsLength++;

            if (currentNode.getChildren() == null) {
                currentNode.setChildren(new FDTreeElement[this.numAttributes]);
                
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                isNew = true;
            } else if (currentNode.getChildren()[i] == null) {
                currentNode.getChildren()[i] = new FDTreeElement(this.numAttributes);
                isNew = true;
            }

            currentNode = currentNode.getChildren()[i];
            addRhsConsumer.accept(currentNode);
        }
        markFdConsumer.accept(currentNode);
        this.depth = Math.max(this.depth, lhsLength);
        return isNew ? currentNode : null;
    }

    public boolean containsFdOrGeneralization(OpenBitSet lhs, int rhs) {
        int nextLhsAttr = lhs.nextSetBit(0);
        return this.containsFdOrGeneralization(lhs, rhs, nextLhsAttr);
    }

    public List<OpenBitSet> getFdAndGeneralizations(OpenBitSet lhs, int rhs) {
        List<OpenBitSet> foundLhs = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet(this.numAttributes);
        int nextLhsAttr = lhs.nextSetBit(0);
        this.getFdAndGeneralizations(lhs, rhs, nextLhsAttr, currentLhs, foundLhs);
        return foundLhs;
    }
    


   
    public int removeFdAndGeneralizations(OpenBitSet lhs, int rhs) {
        OpenBitSet currentLhs = new OpenBitSet(this.numAttributes);
        int nextLhsAttr = lhs.nextSetBit(0);
        return this.removeFdFromGeneralizations(lhs, rhs, nextLhsAttr, currentLhs);
    }
    

    public FDTreeElement findTreeElement(OpenBitSet lhs) {
        FDTreeElement current = this;
        for (int lhsAttr = lhs.nextSetBit(0); lhsAttr >= 0; lhsAttr = lhs.nextSetBit(lhsAttr + 1)) {
            if (current.children != null && current.children[lhsAttr] != null) {
                current = current.children[lhsAttr];
            } else {
                return null;
            }
        }
        return current;
    }

    public boolean containsFd(OpenBitSet lhs, int rhs) {
        FDTreeElement fd = findTreeElement(lhs);
        if (fd != null) {
            return fd.isFd(rhs);
        }
        return false;
    }

    public List<FDTreeElementLhsPair> getLevel(int level) {
        List<FDTreeElementLhsPair> result = new ArrayList<>();
        OpenBitSet currentLhs = new OpenBitSet(this.numAttributes);
        int currentLevel = 0;
        this.getLevel(level, currentLevel, currentLhs, result);
        return result;
    }

    public List<FDTreeElementLhsPair> getLevel(int level, OpenBitSet lhs) {
        return getLevel(level).stream().filter(e -> BitSetUtils.isContained(lhs, e.getLhs())).collect(Collectors.toList());
    }

    public void removeFunctionalDependency(OpenBitSet lhs, int rhs) {
        int currentLhsAttr = lhs.nextSetBit(0);
        this.removeRecursive(lhs, rhs, currentLhsAttr);
    }

    public List<FunctionalDependency> getFunctionalDependencies(ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
        List<FunctionalDependency> functionalDependencies = new ArrayList<>();
        this.addFunctionalDependenciesInto(functionalDependencies, new OpenBitSet(this.numAttributes), columnIdentifiers, plis);
        return functionalDependencies;
    }

    public List<OpenBitSetFD> getFunctionalDependencies() {
        List<OpenBitSetFD> functionalDependencies = new ArrayList<>();
        this.addFunctionalDependenciesInto(functionalDependencies, new OpenBitSet(this.numAttributes));
        return functionalDependencies;
    }

    public int addFunctionalDependenciesInto(FunctionalDependencyResultReceiver resultReceiver, ObjectArrayList<ColumnIdentifier> columnIdentifiers, List<PositionListIndex> plis) {
        return this.addFunctionalDependenciesInto(resultReceiver, new OpenBitSet(this.numAttributes), columnIdentifiers, plis);
    }

	public void printDependencies(int num_attributes) {
		List<OpenBitSetFD> fds = getFunctionalDependencies();
		 
		System.out.println("Number of FDs: "+fds.size());
		
		for (int i=0; i<fds.size(); i++) {
			System.out.print(BitSetUtils.toString(fds.get(i).getLhs(), this.numAttributes));
			System.out.print(" -> ");
			System.out.println(fds.get(i).getRhs());
			//System.out.println(fds.get(i).toString(num_attributes));
		}
		
	}
	
}
