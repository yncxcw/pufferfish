package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

public enum ContainerMemoryState {
	   /**
	    *  Running mode
	   */
	  RUNNING, 
	  
	  /** 
	   *   Suspending mode
	   */
	  SUSPENDING, 
	  
	  /** 
	   *   Recoverying mode
	   */
	  RECOVERYING

}
