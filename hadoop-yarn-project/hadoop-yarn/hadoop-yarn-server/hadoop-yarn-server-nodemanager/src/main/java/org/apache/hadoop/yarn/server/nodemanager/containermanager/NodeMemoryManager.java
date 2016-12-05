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
	 
	 //nodeCurrentUsedMemory
	 int nodeCurrentUsed;

	 //used to do moving avarage ##of 5
	 Map<ContainerId,Long> containerToMemoryUsage;
	 
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
		 //TODO add configuration
		 this.STOP_BALLOON_LIMIT      = conf.getDouble(YarnConfiguration.RATIO_STOP_BALLON_LIMIT,
				                                       YarnConfiguration.DEFAULT_RATIO_STOP_BALLON_LIMIT);
		 
		 this.CONTAINER_BALLOON_RATIO = conf.getDouble(YarnConfiguration.RATIO_CONTAINER_BALLON,
				                                       YarnConfiguration.DEFAULT_RATIO_CONTAINER_BALLON);
		 
		 this.RECLAIM_BALLOON_LIMIT   = conf.getDouble(YarnConfiguration.RATIO_RECLAIM_BALLOON_LIMIT,
				                                       YarnConfiguration.DEFAULT_RATIO__RECLAIM_BALLOON_LIMIT);
		 
		 ReadWriteLock readWriteLock  = new ReentrantReadWriteLock();
		 this.readLock  = readWriteLock.readLock();
		 this.writeLock = readWriteLock.writeLock();
	 }
	  
	 //called in ContainerMonitor preodically to balloon the contaier out
	 //of its demand
	 public void MemoryBalloon(){
	 //LOG.info("memory balloon called");
     try {
		 this.writeLock.lock();
		 //recompute current used
		 nodeCurrentUsed                  = 0;
		 Set<ContainerId> containerIds    = this.context.getContainers().keySet();
		 List<Container>  swappingContainer= new ArrayList<Container>();
		 //we delete out of date containercontainerToMemoryUsage
		 Iterator<Entry<ContainerId, Long>> it = this.containerToMemoryUsage.entrySet().iterator();
		 
		 while(it.hasNext()){
			 Map.Entry<ContainerId, Long> entry=it.next();
			 if(!containerIds.contains(entry.getKey())){
				 it.remove();
			 }
		 }
		
		 //we update the newly memory consumption
		 for(ContainerId containerId : containerIds){
			 Container container     = this.context.getContainers().get(containerId);
			 long currentUsed        = container.getContainerMonitor().getCurrentUsedMemory();
			 //let it go, if this container is not running yet
			 if(currentUsed == 0){
				continue;
			 }
			 //update contaienr memory usage map
			 containerToMemoryUsage.put(containerId, currentUsed);
			 //add to swapping group
			 if(container.getContainerMonitor().getIsSwapping() && container.isFlexble()){
			
                   //LOG.info("add swapping container"+container.getContainerId());
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
			 LOG.info("out of limit reclaim: "+memoryClaimed);
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
			    int newMemory    = (int) (cnt.getContainerMonitor().getCurrentLimitedMemory()*balloonRatio);
			    long newCntMemory = cnt.getContainerMonitor().getCurrentLimitedMemory()+newMemory;
			    int currentUsage = nodeCurrentUsed+newMemory;
			    if(currentUsage*1.0/nodeTotal*1.0 > STOP_BALLOON_LIMIT){
			    	break;
			    }
			    LOG.info("### container"+cnt.getContainerId()+"ratio "+balloonRatio+"balloon to"+newCntMemory+"###");
			    cnt.getContainerMonitor().setConfiguredMemory(newCntMemory);
			    nodeCurrentUsed+=newMemory;
			    balloonRatio/=2;
		  }
	 }finally{
		 this.writeLock.unlock();
	 }
	
 }
	 
 public void MemoryReclaim(int requestSize){
 LOG.info("memory reclaim called");
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
		    long claimedSize =it.next().getContainerMonitor().
				         reclaimMemory(thisRound);
		    
		    //no more memory for reclaiming for this container
		    if(claimedSize < thisRound){
		    	it.remove();
		    }
		    requestSize-=claimedSize;
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
			 long claimedSize = bcontainers.get(i).
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
