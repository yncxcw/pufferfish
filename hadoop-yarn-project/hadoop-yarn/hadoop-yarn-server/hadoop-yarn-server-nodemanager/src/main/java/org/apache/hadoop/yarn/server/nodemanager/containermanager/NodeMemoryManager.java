package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;


public class NodeMemoryManager {
	
	
	private static final Log LOG = LogFactory.getLog(NodeMemoryManager.class);
	
	 //TODO add log for debugging
	 //TODO for memory usage over 95%, NodeMemoryManager should reclaim memory actively
	 //context of nodemanager
	 final Context context;
	 
	 //node total configured memory
	 int nodeTotal;
	 
	 //at x% percent of memory is used, we will stop ballooning
	 final double STOP_BALLOON_LIMIT;
	 
	 final double RECLAIM_BALLOON_LIMIT;
	 
	 final double CONTAINER_BALLOON_RATIO;
	 
	 final int DEFAULT_NODE_SIZE;
	 
	 final long SWAP_KEEP_TIME;
	 //nodeCurrentUsedMemory
	 long nodeCurrentUsed;

	//nodeCurrentAssignedMemory
	long nodeCurrentAssigned;

	 //used to do moving avarage ##of 5
	 Map<ContainerId,Long> containerToMemoryUsage;
	 
	 //used to record the statics of swapping container;
	 Map<ContainerId,Long> containerToSwap;
	 
	 //used to lock when read
	 Lock readLock;
	 
	 //used to lock when write
	 Lock writeLock;
	 
	 public NodeMemoryManager(Context context,Configuration conf){
		 this.DEFAULT_NODE_SIZE = 128*1024;
		 this.context   = context;
		 this.nodeTotal = conf.getInt(
			      YarnConfiguration.NM_PMEM_MB, YarnConfiguration.DEFAULT_NM_PMEM_MB
			                         );
		 this.containerToMemoryUsage  = new HashMap<ContainerId,Long>();
		 this.containerToSwap         = new HashMap<ContainerId,Long>();
		 //TODO add configuration
		 this.STOP_BALLOON_LIMIT      = conf.getDouble(YarnConfiguration.RATIO_STOP_BALLON_LIMIT,
				                                       YarnConfiguration.DEFAULT_RATIO_STOP_BALLON_LIMIT);
		 
		 this.CONTAINER_BALLOON_RATIO = conf.getDouble(YarnConfiguration.RATIO_CONTAINER_BALLON,
				                                       YarnConfiguration.DEFAULT_RATIO_CONTAINER_BALLON);
		 
		 this.RECLAIM_BALLOON_LIMIT   = conf.getDouble(YarnConfiguration.RATIO_RECLAIM_BALLOON_LIMIT,
				                                       YarnConfiguration.DEFAULT_RATIO__RECLAIM_BALLOON_LIMIT);
		 
		 //keep for 2 minute
		 this.SWAP_KEEP_TIME          = 150;
		 ReadWriteLock readWriteLock  = new ReentrantReadWriteLock();
		 this.readLock  = readWriteLock.readLock();
		 this.writeLock = readWriteLock.writeLock();
	 }
	  
	 
	 
	 private void updateMetrics(){
		 try {
			 this.writeLock.lock();
			 //recompute current used
			 nodeCurrentUsed                   = 0;
			 nodeCurrentAssigned               = 0;
			 Set<ContainerId> containerIds     = this.context.getContainers().keySet();
			 //we delete out of date containercontainerToMemoryUsage
			 Iterator<Entry<ContainerId, Long>> it = this.containerToMemoryUsage.entrySet().iterator();
			 while(it.hasNext()){
				 Map.Entry<ContainerId, Long> entry=it.next();
				 if(!containerIds.contains(entry.getKey())){
					 it.remove();
				 }
			 }
			 
			 //we delete out of data containersToSwap
			 Iterator<Entry<ContainerId, Long>> is = this.containerToSwap.entrySet().iterator();
			 while(is.hasNext()){
				 Map.Entry<ContainerId,Long> entry = is.next();
				 if(!containerIds.contains(entry.getKey())){
					 is.remove();
				 }
			 }
			
			 //we update the newly memory consumption
			 for(ContainerId containerId : containerIds){
				 Container container     = this.context.getContainers().get(containerId);
				 long currentUsed        = container.getContainerMonitor().getCurrentUsedMemory();
				 long currentAssigned    = container.getContainerMonitor().getCurrentLimitedMemory();
				 //update contaienr memory usage map
				 this.containerToMemoryUsage.put(containerId, currentUsed);
				 //accumulated host memory usage
				 this.nodeCurrentUsed+=currentUsed;
				 this.nodeCurrentAssigned+=currentAssigned;
			 }
			 
			 //we update newly container swapping
			 for(ContainerId containerId : containerIds){
				 Container container     = this.context.getContainers().get(containerId);
			     //only keep flexible container
				 if(!container.isFlexble()){
			    	 continue;
			     }
				 
				//add to swapping group
				 if(container.getContainerMonitor().getIsSwapping()){
	                 //LOG.info("add swapping container"+container.getContainerId());
	            	 this.containerToSwap.put(containerId, SWAP_KEEP_TIME);
	             
	             //update its waiting time if not swap this round
				 }else if(this.containerToSwap.containsKey(containerId)){
					 long currentWaitTime = this.containerToSwap.get(containerId);
					 currentWaitTime--;
					 this.containerToSwap.put(containerId, currentWaitTime);
					 if(this.containerToSwap.get(containerId) <0){
						 this.containerToSwap.remove(containerId);
						 LOG.info(containerId+" is removed");
					 }
				 } 
			 }
		 }finally{
			 this.writeLock.unlock();
		 }
	 }
	 //called in ContainerMonitor preodically to balloon the contaier out
	 //of its demand
	 public void MemoryBalloon(){
	     //LOG.info("memory balloon called");
		 List<Container>  swappingContainer= new ArrayList<Container>();
		 
		 this.updateMetrics();
		 
		 for(Entry<ContainerId, Long> entry: this.containerToSwap.entrySet()){
			 ContainerId containerId=entry.getKey();
			 swappingContainer.add(this.context.getContainers().get(containerId));
		 }
		 
		 //sort swapping container by its starting time
		 Collections.sort(swappingContainer, new Comparator<Container>() {
		        @Override
		        public int compare(final Container object1, final Container object2) {
		        return Long.compare(object1.getLaunchStartTime(), object2.getLaunchStartTime());
		        }
		  } );
		 //out of the limit, we do nothing, since ContainerImpl will throttle the cpu
		 //usage for this container
		 double usage    = nodeCurrentUsed*1.0/nodeTotal*1.0;
		 double assignage= nodeCurrentAssigned*1.0/nodeTotal*1.0;
		 LOG.info("balloon usage  and assignage:  "+usage+"  "+assignage);
		 if(assignage > RECLAIM_BALLOON_LIMIT){
			
			 int memoryClaimed=(int)((assignage-RECLAIM_BALLOON_LIMIT)*nodeTotal);
			 LOG.info("out of limit reclaim: "+memoryClaimed);
			 this.MemoryReclaim(memoryClaimed);
			 return;
			 
		 }else if( assignage > STOP_BALLOON_LIMIT && assignage < RECLAIM_BALLOON_LIMIT){
			 LOG.info("stop ballooning at assign usage"+assignage);
			 return;
		 }
		 //TODO test if ordered right
		 double balloonRatio = CONTAINER_BALLOON_RATIO;
		 //If we have available memory, we will choose memory hungry container to balloon
		 //earliest balloon first
		 
		  int swappingSize=0;
		  if(swappingContainer.size() > 0){
			 // LOG.info("swapping container size "+swappingContainer.size());
		  }
		  for(Container cnt : swappingContainer){
			    //compute new memory after balloon
			    //LOG.info("cached swapping container: "+cnt.getContainerId()+"  ratio:"+balloonRatio);
		       if(cnt.getContainerMonitor().getIsSwapping()){
		    	   swappingSize++;
			       int oldMemory     = (int) cnt.getContainerMonitor().getCurrentLimitedMemory();
			       int newMemory     = (int) (oldMemory*balloonRatio);
				   int available     = (int) (nodeTotal*STOP_BALLOON_LIMIT-nodeCurrentAssigned);
				   if(available <=0){
					   LOG.info("balloon error: "+ available);
					   return;
				   }
				   if(newMemory >= available){
					   newMemory = available;
					   LOG.info("available: "+ available + " newMemory: "+newMemory+" Limit: "+nodeTotal*STOP_BALLOON_LIMIT);
				   }
			       long newCntMemory = oldMemory+newMemory;

			        //LOG.info("### container "+cnt.getContainerId()+" ratio "+balloonRatio+" from "+oldMemory+" to "+newCntMemory+" ###");
			        cnt.getContainerMonitor().setConfiguredMemory(newCntMemory);
			        nodeCurrentAssigned+=newMemory;

		        }
			    balloonRatio/=8;
			    
		  }
		  if(swappingSize > 0){
		      //LOG.info("swapping size: "+swappingSize);
		  }
 }
	 
 public void MemoryReclaim(int requestSize){
    LOG.info("memory reclaim called, current assigned: "+nodeCurrentAssigned+"  current used: "+nodeCurrentUsed+" request: "+requestSize);
    LOG.info("limit: "+nodeTotal*RECLAIM_BALLOON_LIMIT);
	 //update metrics
	this.updateMetrics();
	 //we bypass memory reclaim
	 if(nodeCurrentAssigned + requestSize < nodeTotal*RECLAIM_BALLOON_LIMIT){
		 return;
	 }

	 requestSize = (int)(nodeCurrentAssigned + requestSize-nodeTotal*RECLAIM_BALLOON_LIMIT);

	 LOG.info("new reclaim: "+requestSize);
	 
	 //Find all ballooned but not swapped containers
	 List<Container> bcontainers = new ArrayList<Container>();
	 //Find all ballooned and swapped containers
	 List<Container> scontainers = new ArrayList<Container>();
	 for(ContainerId cntId: containerToMemoryUsage.keySet()){
		 Container container = (Container) this.context.getContainers().get(cntId);
		 if(container.isFlexble() && (containerToMemoryUsage.get(cntId) >
		                          container.getResource().getMemory())){
			 if(container.getContainerMonitor().getIsSwapping())
				 scontainers.add(container);
			 else
				 bcontainers.add(container);
		 }
	 }
	 
	 //First, reclaim memory from swapped container
	 if(scontainers.size() > 0){

	    int thisRound = requestSize/scontainers.size();
	    Iterator<Container> it=scontainers.iterator();
	    while(it.hasNext()){
		    long claimedSize =it.next().getContainerMonitor().
				         reclaimMemory(thisRound);

		    requestSize-=claimedSize;

			if(requestSize <= 10){
				break;
			}
	  }


	 if(requestSize<=10)
		 return;
	 }
	 
	 //Second, reclaim memory from latest balloon container
	 //sort swapping container by its starting time
	 Collections.sort(bcontainers, new Comparator<Container>() {
	        @Override
	        public int compare(final Container object1, final Container object2) {
	        return Long.compare(object1.getLaunchStartTime(), object2.getLaunchStartTime());
	        }
	  } );
	 

	 if(bcontainers.size() > 0) {
		 for (int i = bcontainers.size() - 1; i >= 0; i--) {
			 //we choose container ordered by its submission time
			 //by doing so, we restrict the affect of swapness to 
			 //as less container as possible
			 long claimedSize = bcontainers.get(i).
					 getContainerMonitor().reclaimMemory(requestSize);
			 requestSize -= claimedSize;

			 if (requestSize <= 10) {
				 break;
			 }
		 }

	 }
		 

	return; 
 }	 
 
 
 public long getCurrentActualMemory(){
	 
	 return this.nodeCurrentUsed;
 }
	 
}
