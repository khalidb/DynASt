package org.mp.naumann.algorithms.fd.structures;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.lucene.util.OpenBitSet;
import org.mp.naumann.algorithms.fd.FunctionalDependency;
import org.mp.naumann.algorithms.fd.FunctionalDependencyResultReceiver;
import org.mp.naumann.algorithms.fd.hyfd.PositionListIndex;
import org.mp.naumann.algorithms.fd.utils.BitSetUtils;
import org.mp.naumann.database.data.ColumnIdentifier;

import fr.dauphine.lamsade.khalid.dynast.util.SortOpenBitSet;

public class ASTree extends ASTreeElement {

	
	
	public ASTree(int numAttributes){
		super(numAttributes);
		this.children = new ASTreeElement[this.numAttributes];
	}

	public void addAgreeSets(Map<OpenBitSet,Integer> bitsetmap) {	
		
		//List<OpenBitSet> bitsets = new ArrayList<OpenBitSet>(bitsetmap.keySet());
		//Collections.sort(bitsets,new SortOpenBitSet());
		//Collections.sort(bitsets,new SortOpenBitSet().reversed());
		//Collections.sort(bitsets, Collections.reverseOrder());
		
		
    	for (OpenBitSet name: bitsetmap.keySet()){
    	//for (OpenBitSet name: bitsets){	
    		//System.out.println("numAttributes "+this.numAttributes);
            String key = BitSetUtils.toString(name,numAttributes);
            Integer value = bitsetmap.get(name);  
            //System.out.println("Need to add the agreeset "+key + " with card " + value);  
            this.addAgreeSet(name, value);	
    	} 
		
	}
	
	public void removeAgreeSets(Map<OpenBitSet,Integer> bitsetmap) {
		
		for (Map.Entry<OpenBitSet,Integer> entry : bitsetmap.entrySet()) {
			//System.out.println("Bitset to remove");
			//System.out.println(BitSetUtils.toString(entry.getKey(),this.numAttributes));
			//System.out.println("card: "+entry.getValue());
			this.reduceCardinality(entry.getKey(), entry.getValue());	
		}
		
	}
	
	public void addAgreeSet(OpenBitSet as, int card) {
		//System.out.println("####### Adding: "+BitSetUtils.toString(as,numAttributes));
		
		OpenBitSet activepath = new OpenBitSet(numAttributes);
		if (as.equals(activepath)) {
			this.card += card;
			return;
		}
		if (as.get(0)) {
			activepath.set(0);
			//System.out.println("idx_children: "+BitSetUtils.toString(idx_children,numAttributes));
			//System.out.println("this.children: "+this.children);
			if (this.children[0] == null) {
				this.children[0] = new ASTreeElement(numAttributes);
				idx_children.set(0);
			}
			//System.out.println("idx_children: "+BitSetUtils.toString(idx_children,numAttributes));
			this.children[0].addAgreeSet(as, card, 0, activepath);
		}
		else
			this.addAgreeSet(as, card, 0, activepath);	
	}
	
	
	public void printTree() {
		
		OpenBitSet activepath = new OpenBitSet(numAttributes);
		this.printTree(activepath, "");
		
	}
	
	public int getCardinalityOfSpecializations(OpenBitSet as) {
		Integer[] cardinalities = new Integer[1];
		cardinalities[0]= 0;
		OpenBitSet activepath = new OpenBitSet(numAttributes);
		this.getCardinalityOfSpecializations(as, activepath, cardinalities);
		return  cardinalities[0];
		
	}
	
	public void reduceCardinality(OpenBitSet as, int cardinality) {
		
		int currAttr = 0;
		int nextAttr = as.nextSetBit(currAttr);
		
		if (nextAttr >= 0) {
			/*
			System.out.println("Next attributes: "+ nextAttr);
			System.out.println("Open bit set AS");
			System.out.println(BitSetUtils.toString(as,this.numAttributes));
			System.out.println("Children");
			System.out.println(this.children);
			*/
			try {
				this.children[nextAttr].reduceCardinality(as, cardinality, nextAttr);
			} catch (Exception e) {
				//ignore
				//System.err.println("Did not find a child");
			}
			
		} else
			this.card -= cardinality;
			
	}
	
	public void pruneASTree() {
		//System.out.println("Print AS Tree");
		this.pruneASTreeElement(0);
	}
	
	
	
	/*
	public void pruneASTree() {
		
		ASTreeElement currElem = null;
		
		if (this.card == 0)
			currElem = this;
		
		for (int currAttr =0; currAttr < numAttributes; currAttr++) {
			if (this.idx_children.get(currAttr)) {
				if (this.children[currAttr].hasNoChildrenWithCardinality()) {
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
	*/
	
	public int getBeforeSet(OpenBitSet bs, int pos) {
		if (bs.get(pos))
			return pos;
		else {
			pos--;
			return getBeforeSet(bs, pos);
		}	
	}
	
	public void reduceAS(List<OpenBitSet> reducedAS, Map<OpenBitSet, Integer> hash_reducedAS) {
		
		//Collections.sort(reducedAS,new SortOpenBitSet());
		
		//System.out.println("Order reducedAS");
		//for (OpenBitSet bs: reducedAS) {
		//	System.out.println(BitSetUtils.toString(bs,numAttributes));
		//}
		
		OpenBitSet activePath = new OpenBitSet(numAttributes);
		
		int min_attr = 0;
		
		this.reduceAS(activePath, reducedAS, hash_reducedAS, min_attr, 0);
		
	}
	

	
	public static void main(String[] args) {
		
		
		
		
		int numAttributes = 4;
		ASTree astree = new ASTree(numAttributes);
		
		
		// 0000 3
		OpenBitSet as = new OpenBitSet(numAttributes);
		int card = 3;
		
		astree.addAgreeSet(as, card);
		
		// 0010 2
		OpenBitSet as1 = new OpenBitSet(numAttributes);
		as1.set(2);
		astree.addAgreeSet(as1, 2);
		
		// 0001 4
		OpenBitSet as2 = new OpenBitSet(numAttributes);
		as2.set(3);
		astree.addAgreeSet(as2, 4);
		
		// 0011 4
		OpenBitSet as3 = new OpenBitSet(numAttributes);
		as3.set(2);
		as3.set(3);
		astree.addAgreeSet(as3, 4);
		
		// 0111 5
		OpenBitSet as4 = new OpenBitSet(numAttributes);
		as4.set(1);
		as4.set(2);
		as4.set(3);
		astree.addAgreeSet(as4, 5);
		
		//astree.printTree();
		
		// 1011 6
		OpenBitSet as5 = new OpenBitSet(numAttributes);
		as5.set(0);
		as5.set(2);
		as5.set(3);
		astree.addAgreeSet(as5, 6);
		
		// 1010 7
		OpenBitSet as6 = new OpenBitSet(numAttributes);
		as6.set(0);
		as6.set(2);
		astree.addAgreeSet(as6, 7);
		
		// 1000 10
		OpenBitSet as7 = new OpenBitSet(numAttributes);
		as7.set(0);
		astree.addAgreeSet(as7, 10);
		
		System.out.println("as5: "+BitSetUtils.toString(as5,numAttributes));
		
		
		
		astree.printTree();
		
		
		System.out.println("Get cardinality of specializations of 1000");
		OpenBitSet agreeset = new OpenBitSet(numAttributes);
		agreeset.fastSet(2);
		agreeset.fastSet(3);
		System.out.println(astree.getCardinalityOfSpecializations(agreeset));
		
		System.out.println("Reduce the cardinlaity of the agreeset 0001 with 3");
		OpenBitSet as8 = new OpenBitSet(numAttributes);
		//as8.set(0);
		//as8.set(2);
		as8.set(3);
		//astree.reduceCardinality(as8, 3);
		
		
		System.out.println("Reduce the cardinlaity of the agreeset 0111 with 2");
		OpenBitSet as9 = new OpenBitSet(numAttributes);
		as9.set(1);
		as9.set(2);
		as9.set(3);
		//astree.reduceCardinality(as9, 2);
		
		astree.printTree();
		
		Map<OpenBitSet, Integer> hash_reducedAS = new HashMap<OpenBitSet,Integer>();
		hash_reducedAS.put(as8, 3);
		hash_reducedAS.put(as9, 2);
		
		List<OpenBitSet> reducedAS = new ArrayList<OpenBitSet>();
		reducedAS.add(as9);
		reducedAS.add(as8);
		
		System.out.println("Before ordering reducedAS");
		for (OpenBitSet bs: reducedAS) {
			System.out.println(BitSetUtils.toString(bs,numAttributes));
		}
		
		astree.reduceAS(reducedAS, hash_reducedAS);
		
		//System.out.println("Prune ASTree");
		//astree.pruneASTree();
		
		System.out.println("AStree after reducing the cardinalities");
		astree.printTree();
	
		
	}
	

}
