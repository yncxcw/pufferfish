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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

public class NodeMemoryManager {
	
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
	 
	 //nodeCurrentUsedMemory
	 int nodeCurrentUsed;

	 //used to do moving avarage ##of 5
	 Map<ContainerId,Integer> containerToMemoryUsage;
	 
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
		 this.containerToMemoryUsage  = new HashMap<ContainerId,Integer>();
		 //TODO add configuration
		 this.STOP_BALLOON_LIMIT      = 0.9;
		 this.CONTAINER_BALLOON_RATIO = 0.5;
		 this.RECLAIM_BALLOON_LIMIT   = 0.95;
		 
		 ReadWriteLock readWriteLock  = new ReentrantReadWriteLock();
		 this.readLock  = readWriteLock.readLock();
		 this.writeLock = readWriteLock.writeLock();
	 }
	  
	 //called in ContainerMonitor preodically to balloon the contaier out
	 //of its demand
	 public void MemoryBalloon(){
     try {
		 this.writeLock.lock();
		 //recompute current used
		 nodeCurrentUsed                  = 0;
		 List<Container> containers       = (List<Container>) this.context.getContainers().values();
		 List<Container> swappingContainer= new ArrayList<Container>();
		 //we delete out of date container
		 for(ContainerId containerId : this.containerToMemoryUsage.keySet()){
			 if(!containers.contains(containerId)){
				 this.containerToMemoryUsage.remove(containerId);
			 }
		 }
		 //we update the newly memory consumption
		 for(Container container : containers){
			 ContainerId containerId = container.getContainerId();
			 int currentUsed = container.getContainerMonitor().getCurrentUsedMemory();
			 //let it go, if this container is not running yet
			 if(currentUsed == 0){
				continue;
			 }
			 //update contaienr memory usage map
			 containerToMemoryUsage.put(containerId, currentUsed);
			 //add to swapping group
			 if(container.getContainerMonitor().getIsSwapping()){
				Application app = (Application) context.getApplications().get(
                         container.getContainerId().
                         getApplicationAttemptId().
                         getApplicationId()
                         );
               if(!app.getIsFlexible()){
                   continue;
                }
				swappingContainer.add(container);
			 }
			 nodeCurrentUsed+=currentUsed;
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
		 double usage = nodeCurrentUsed*1.0/nodeTotal*1.0;
		 if(usage > RECLAIM_BALLOON_LIMIT){
			 int memoryClaimed=(int)(usage-RECLAIM_BALLOON_LIMIT)*nodeTotal;
			 this.MemoryReclaim(memoryClaimed);
			 return;
			 
		 }else if( usage > STOP_BALLOON_LIMIT && usage < RECLAIM_BALLOON_LIMIT){
			 return;
		 }
		 //TODO test if ordered right
		 double balloonRatio = CONTAINER_BALLOON_RATIO;
		 //If we have available memory, we will choose memory hungry container to balloon
		 //earliest balloon first
		  for(Container cnt : swappingContainer){
			    //compute new memory after balloon
			    int newMemory    = (int) (cnt.getContainerMonitor().getConfiguredMemory()*balloonRatio);
			    int currentUsage = nodeCurrentUsed+newMemory;
			    if(currentUsage*1.0/nodeTotal*1.0 > STOP_BALLOON_LIMIT){
			    	break;
			    }
			    cnt.getContainerMonitor().setConfiguredMemory(newMemory);
			    nodeCurrentUsed+=newMemory;
			    balloonRatio/=2;
		  }
	 }finally{
		 this.writeLock.unlock();
	 }
	
 }
	 
 public void MemoryReclaim(int requestSize){
 try {
	this.readLock.lock();
	 //we bypass memory reclaim
	 if(nodeCurrentUsed + requestSize < nodeTotal*RECLAIM_BALLOON_LIMIT){
		 return;
	 }
	 
	 //Find all ballooned but not swapped containers
	 List<Container> bcontainers = new ArrayList<Container>();
	 //Find all ballooned and swapped containers
	 List<Container> scontainers = new ArrayList<Container>();
	 for(ContainerId cntId: containerToMemoryUsage.keySet()){
		 Container container = (Container) this.context.getContainers().get(cntId);
		 if(container.getContainerMonitor().getCurrentUsedMemory() > 
		                          container.getResource().getMemory()){
			 if(container.getContainerMonitor().getIsSwapping())
				 scontainers.add(container);
			 else
				 bcontainers.add(container);
		 }
	 }
	 
	 //First, reclaim memory from swapped container
	 if(scontainers.size() > 0){
	 while(true){
	    int thisRound = requestSize/scontainers.size();
	    Iterator<Container> it=scontainers.iterator();
	    while(it.hasNext()){
		    int claimedSize =it.next().getContainerMonitor().
				         reclaimMemory(thisRound);
		    
		    //no more memory for reclaiming for this container
		    if(claimedSize < thisRound){
		    	it.remove();
		    }
		    requestSize-=thisRound;
	  }
	    if(scontainers.size() == 0 || requestSize <=10){
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
	 
	 while(requestSize > 10){
		 for(int i=bcontainers.size()-1;i>=0;i--){
			 //we choose container ordered by its submission time
			 //by doing so, we restrict the affect of swapness to 
			 //as less container as possible
			 int claimedSize = bcontainers.get(i).
					             getContainerMonitor().reclaimMemory(requestSize);
			 requestSize-=claimedSize;	 
		 }
		 
	 }
	}finally{
		 this.readLock.unlock();
	}
	return; 
 }	 
	 
}
