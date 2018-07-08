package org.apache.hadoop.yarn.server.resourcemanager.scheduler.custom;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;

/***
 * Some helper methods for the scheduler.
 * @author Yehia Elshater
 *
 */
public class SchedulerCustomUtils {
	
	public static FSSchedulerNode retrieveSchedulerNodeByHostName (Map<NodeId , FSSchedulerNode> nodes , String hostname) {
		for (NodeId key : nodes.keySet()) {
			if(key.getHost().equals(hostname)) {
				return nodes.get(key);
			}
		}
		return null;
	}
	
	public static NodeId retrieveSchedulerNodeIdByHostName (Map<NodeId , FSSchedulerNode> nodes , String hostname) {
		for (NodeId key : nodes.keySet()) {
			if(key.getHost().equals(hostname)) {
				return key;
			}
		}
		return null;
	}

	/***
	 * Selects the node with minimum score (i.e. with minimum number of blocks - the most under-utilized)
	 * TODO: Should be re-factored to maintain a min heap (e.g. TreeMap) to keep the minimum node in O(1). Currently is O(cluster_nodes)
	 * 
	 * @author Yehia Elshater
	 * @param nodeScoresMap
	 * @param nodes
	 * @return
	 */
	public static FSSchedulerNode selectMinScoreNode(
			ConcurrentHashMap<NodeId, Integer> nodeScoresMap,
			Map<NodeId, FSSchedulerNode> nodes, Set<NodeId> blackNodesList) {
		FSSchedulerNode minNode = null;
		TreeMap<Integer , List<NodeId>> sortedNodes = new TreeMap<Integer, List<NodeId>>();
		for (NodeId node : nodeScoresMap.keySet()) {
			List<NodeId> nodeList = sortedNodes.get(nodeScoresMap.get(node));
			if (nodeList !=null && !nodeList.isEmpty()) { //This means there are two or more nodes with the same score
				nodeList.add(node);
			}
			if (nodeList == null) {
				nodeList = new ArrayList<NodeId>();
				nodeList.add(node);
			}
			sortedNodes.put(nodeScoresMap.get(node), nodeList);
		}
		while (minNode == null) {
			for (List<NodeId> nodeList : sortedNodes.values()) {
				for (NodeId mNode : nodeList) {
					if (!blackNodesList.contains(mNode)) {
						minNode = nodes.get(mNode);
						break;
					}
				}
				if (minNode !=null)
					break;
			}
		}
		return minNode;
	}
	
	public static void main(String[] args) {
		TreeMap<Integer, String> m = new TreeMap<Integer, String>();
		m.put(4, "k1");
		m.put(2, "k2");
		m.put(1, "k3");
		m.put(1, "k4");
		System.out.println(m.values().iterator().next());
		NodeSelection ns = NodeSelection.RANDOM;
		System.out.println(ns);
	}
	
	public enum NodeSelection {
		RANDOM,
		MIN
	}

}
