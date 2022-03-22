package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.database.data.ColumnCombination;
import org.mp.naumann.database.data.ColumnIdentifier;

public class ASTreeElement {

    public ASTreeElement[] children;
    OpenBitSet agreeset;
    OpenBitSet idx_children;
    int card;
    int numAttributes;
    
    
    ASTreeElement(int numAttributes) {
        this.numAttributes = numAttributes;
        this.agreeset = new OpenBitSet(numAttributes);
        this.idx_children = new OpenBitSet(numAttributes);
        this.card = 0;
    }

    public int getNumAttributes() {
        return this.numAttributes;
    }

    // children

    public ASTreeElement[] getChildren() {
        return this.children;
    }

    void setChildren(ASTreeElement[] children) {
        this.children = children;
    }
    
	public void reduceAS(OpenBitSet activePath,List<OpenBitSet> reducedAS, Map<OpenBitSet, Integer> hash_reducedAS, int min_attr, int as_id) {
		
		if (reducedAS.size() == 0) {
			return;
		}

		
		if (activePath.equals(reducedAS.get(as_id))) {
			
			this.card -= hash_reducedAS.get(activePath);
			as_id++;
			//reducedAS.remove(0);

		}
		

		if (this.children != null) {
			for (int i = this.children.length - 1; i>= min_attr; i--) {
				if (this.children[i] != null) {
					activePath.set(i);
					this.children[i].reduceAS(activePath, reducedAS, hash_reducedAS, i+1, as_id);
					activePath.clear(i);
				}
			}
		}
		
	}
 
    public boolean isAgreeSet(int i) {
        return (this.card > 0);
    }

    
    
    void getCardinalityOfSpecializations(OpenBitSet as, OpenBitSet activepath, Integer[] cardinalities){
    	OpenBitSet path;
    	long card_inter = OpenBitSet.intersectionCount(activepath, as);
    	//System.out.println("Intersection between "+BitSetUtils.toString(activepath,numAttributes)+" and "+BitSetUtils.toString(as,numAttributes)+ " is "+card_inter);
    	//System.out.println("as cardinlaity is "+as.cardinality());
    	if (card_inter == as.cardinality()) {
    		
    		if (this.card > 0) cardinalities[0] += this.card;
    		
    		for (int attr=0; attr < numAttributes ; attr++ ) {
    			if (idx_children.get(attr)) {
    				this.children[attr].getCardinalityOfChildren(cardinalities);
    			}	
    		}
    	}
    	else if (card_inter == activepath.cardinality()) {
    		for (int attr=0; attr < numAttributes ; attr++ ) {
    			if (idx_children.get(attr)) {
    				path = activepath.clone();
    				path.set(attr);
    				this.children[attr].getCardinalityOfSpecializations(as,path,cardinalities);
    			}	
    		}
    	}
    	
    		
    }
    
    void getCardinalityOfChildren(Integer[] cardinalities){	
    	
    	//System.out.println("start getCardinalityOfChildren");
		if (this.card > 0) {
			cardinalities[0] += this.card;
		}
		
		
		for (int attr=0; attr < numAttributes ; attr++ ) {
			if (idx_children.get(attr)) {
				this.children[attr].getCardinalityOfChildren(cardinalities);
			}	
		}

    		
    }
    
    
	public void addAgreeSet(OpenBitSet as, int card, int currAtt, OpenBitSet activepath) {
		
		//System.out.println("active path: "+BitSetUtils.toString(activepath, numAttributes));
		if (activepath.equals(as)) {
			this.card += card;
			//System.out.println("Added "+BitSetUtils.toString(activepath,numAttributes)+" with card "+card);

		}
		else {
			int nextAtt = as.nextSetBit(currAtt+1);
			//System.out.println("nextAtt: "+nextAtt);
			if (children == null)
				this.children = new ASTreeElement[numAttributes];
			if (children[nextAtt] == null) {
				this.children[nextAtt] = new ASTreeElement(numAttributes);
				idx_children.set(nextAtt);
			}
			activepath.set(nextAtt);
			children[nextAtt].addAgreeSet(as, card, nextAtt, activepath);

		}
	}
	
	protected void reduceCardinality(OpenBitSet as, int cardinality, int currAttr) {
				
		int nextAttr = as.nextSetBit(currAttr+1);
			
		if (nextAttr >= 0) {
			this.children[nextAttr].reduceCardinality(as, cardinality, nextAttr);
		} else
			this.card -= cardinality;
		
	}
	
	protected void pruneASTreeElement(int attr) {
		
		for (int currAttr = attr; currAttr < numAttributes; currAttr++) {
			if (this.idx_children.get(currAttr)) {
				if (!this.children[currAttr].hasChildrenWithCardinality(currAttr)) {
					idx_children.clear(currAttr);
					this.children[currAttr] = null;
				}
				else {
					if (currAttr < (numAttributes -1))
						this.children[currAttr].pruneASTreeElement(currAttr+1);
				}
			}
		}
	}
	
	protected boolean hasChildrenWithCardinality(int currAttr) {
		
		boolean found = false;
		if (this.card > 0) {
			found = true;
		}
		else {
			if (currAttr < numAttributes -1) {
				for (int attr = currAttr +1 ; attr< numAttributes; attr++) {
					if (this.idx_children.get(attr))
							if (this.children[attr].hasChildrenWithCardinality(attr)) {
								found = true;
								break;
							}
								
				}
				
			}
		}
		
		return found;
		
	}
	
	public void printTree(OpenBitSet activepath, String level) {
		
		OpenBitSet path;
		System.out.println(level+BitSetUtils.toString(activepath,numAttributes)+ "	" + this.card);
		level = level + "-";
		
		for(int i=0; i <numAttributes; i++) {
			if (idx_children.get(i)) {
				path = activepath.clone();
				path.set(i);
				this.children[i].printTree(path, level);
			}	
		}
		
		
	}
    	
}
