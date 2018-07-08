package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.customtypes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class BlockScore implements Comparable<BlockScore> {
	
	private String key;
	private double score;
	public BlockScore(String blockId, double zScore) {
		super();
		this.key = blockId;
		this.score = zScore;
	}
	public String getBlockId() {
		return key;
	}
	public void setBlockId(String blockId) {
		this.key = blockId;
	}
	public double getzScore() {
		return score;
	}
	public void setzScore(double zScore) {
		this.score = zScore;
	}
	
	@Override
	public int compareTo(BlockScore o) {
		double diff = score - o.getzScore();
			if (diff < 0)
				return 1;
			if (diff>0)
				return -1;
			if (key.equals(o.key)) //means the same block id with different zScore values, return the maximum
				return (int)Math.signum(diff);
			return 0;
	}

	@Override
	public boolean equals(Object obj) {
		BlockScore objB = (BlockScore)obj;
		return (this.key.equals(objB.key) && this.score == objB.score);
	}
	
	@Override
	public int hashCode() {
		int v = (int)score;
		if (v==0)
			v = 1;
		return key.hashCode() >> 13 * v >> 13;
	}
	
	@Override
	public String toString() {
		return key+"_"+score;
	}
	
	public static void main(String[] args) {
		List<BlockScore> set = new ArrayList<BlockScore>();
		BlockScore st = new BlockScore("tempId", 9.9);
		set.add(new BlockScore("1", 0.5));
		set.add(new BlockScore("2", 0.5)); 
		set.add(new BlockScore("3", 2.5));
		set.add(new BlockScore("4", 5.5));
		set.add(new BlockScore("5", 10));
		set.add(new BlockScore("5", 1.5));
		
		set.add(st);
		
		Collections.sort(set);
		System.out.println(set);
		Collections.sort(set);
		//System.out.println(set);
		BlockScore st1 = set.get(0);
		st1.setzScore(9.99);
		
		Stack<BlockScore> stack = new Stack<BlockScore>();
		stack.add(new BlockScore("1", 0.5));
		stack.add(new BlockScore("2", 0.5)); 
		stack.add(new BlockScore("3", 2.5));
		stack.add(new BlockScore("4", 5.5));
		stack.add(new BlockScore("5", 1.5));
		
		System.out.println("Contains " + stack.contains(new BlockScore("5",2.5)));
		//stack.add(st);
		Collections.sort(stack);
		//System.out.println("stack " + stack);
		
		Set<BlockScore> hashSet = new HashSet<>();
		hashSet.add(new BlockScore("1", 0.5));
		hashSet.add(new BlockScore("1", 0.5));
		//System.out.println(hashSet);
	}


}
class BlockScoreComparator implements Comparator<BlockScore> {

	@Override
	public int compare(BlockScore o1, BlockScore o2) {
		if (o1.getzScore() < o2.getzScore())
			return 1;
		if (o1.getzScore() > o2.getzScore())
			return -1;
		return 0;
	}
	
}
