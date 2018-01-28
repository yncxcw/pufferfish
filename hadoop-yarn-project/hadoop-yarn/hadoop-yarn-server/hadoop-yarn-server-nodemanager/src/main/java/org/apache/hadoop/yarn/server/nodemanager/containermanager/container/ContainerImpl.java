/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServicesEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.LocalResourceRequest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationCleanupEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.ContainerLocalizationRequestEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerContainerFinishedEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStartMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainerStopMonitoringEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.SystemClock;

public class ContainerImpl implements Container {

  private final Lock readLock;
  private final Lock writeLock;
  private final Dispatcher dispatcher;
  private final NMStateStoreService stateStore;
  private final Credentials credentials;
  private final NodeManagerMetrics metrics;
  private final ContainerLaunchContext launchContext;
  private final ContainerTokenIdentifier containerTokenIdentifier;
  private final ContainerId containerId;
  private final Resource resource;
  private final String user;
  private int exitCode = ContainerExitStatus.INVALID;
  private final StringBuilder diagnostics;
  private boolean wasLaunched;
  private long containerLaunchStartTime=-1;
  private static Clock clock = new SystemClock();
  private final int virtualCores;
  
  public ContainerMonitor containerMonitor; 

  /** The NM-wide configuration - not specific to this container */
  private final Configuration daemonConf;

  private static final Log LOG = LogFactory.getLog(ContainerImpl.class);
  private final Map<LocalResourceRequest,List<String>> pendingResources =
    new HashMap<LocalResourceRequest,List<String>>();
  private final Map<Path,List<String>> localizedResources =
    new HashMap<Path,List<String>>();
  private final List<LocalResourceRequest> publicRsrcs =
    new ArrayList<LocalResourceRequest>();
  private final List<LocalResourceRequest> privateRsrcs =
    new ArrayList<LocalResourceRequest>();
  private final List<LocalResourceRequest> appRsrcs =
    new ArrayList<LocalResourceRequest>();
  private final Map<LocalResourceRequest, Path> resourcesToBeUploaded =
      new ConcurrentHashMap<LocalResourceRequest, Path>();
  private final Map<LocalResourceRequest, Boolean> resourcesUploadPolicies =
      new ConcurrentHashMap<LocalResourceRequest, Boolean>();

  // whether container has been recovered after a restart
  private RecoveredContainerStatus recoveredStatus =
      RecoveredContainerStatus.REQUESTED;
  // whether container was marked as killed after recovery
  private boolean recoveredAsKilled = false;
  private Context context;
  
  private boolean isFlexible;

  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier, Context context) {
    this.daemonConf = conf;
    this.dispatcher = dispatcher;
    this.stateStore = context.getNMStateStore();
    this.launchContext = launchContext;
    this.containerTokenIdentifier = containerTokenIdentifier;
    this.containerId = containerTokenIdentifier.getContainerID();
    this.resource = containerTokenIdentifier.getResource();
    this.diagnostics = new StringBuilder();
    this.credentials = creds;
    this.metrics = metrics;
    user = containerTokenIdentifier.getApplicationSubmitter();
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.context = context;
    this.isFlexible=false;
    this.containerMonitor = new ContainerMonitor(isFlexible);
    this.virtualCores =
            conf.getInt(
                YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);
    
    stateMachine = stateMachineFactory.make(this);
  }

  // constructor for a recovered container
  public ContainerImpl(Configuration conf, Dispatcher dispatcher,
      ContainerLaunchContext launchContext, Credentials creds,
      NodeManagerMetrics metrics,
      ContainerTokenIdentifier containerTokenIdentifier,
      RecoveredContainerStatus recoveredStatus, int exitCode,
      String diagnostics, boolean wasKilled, Context context) {
    this(conf, dispatcher, launchContext, creds, metrics,
        containerTokenIdentifier, context);
    this.recoveredStatus = recoveredStatus;
    this.exitCode = exitCode;
    this.recoveredAsKilled = wasKilled;
    this.diagnostics.append(diagnostics);
  }

  @Override
  public void setFlexible(){
	  this.isFlexible = true;
  }
  
  @Override
  public boolean isFlexble(){
	  return this.isFlexible;
  }
  
  private static final ContainerDiagnosticsUpdateTransition UPDATE_DIAGNOSTICS_TRANSITION =
      new ContainerDiagnosticsUpdateTransition();

  // State Machine for each container.
  private static StateMachineFactory
           <ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>
        stateMachineFactory =
      new StateMachineFactory<ContainerImpl, ContainerState, ContainerEventType, ContainerEvent>(ContainerState.NEW)
    // From NEW State
    .addTransition(ContainerState.NEW,
        EnumSet.of(ContainerState.LOCALIZING,
            ContainerState.LOCALIZED,
            ContainerState.LOCALIZATION_FAILED,
            ContainerState.DONE),
        ContainerEventType.INIT_CONTAINER, new RequestResourcesTransition())
    .addTransition(ContainerState.NEW, ContainerState.NEW,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.NEW, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER, new KillOnNewTransition())

    // From LOCALIZING State
    .addTransition(ContainerState.LOCALIZING,
        EnumSet.of(ContainerState.LOCALIZING, ContainerState.LOCALIZED),
        ContainerEventType.RESOURCE_LOCALIZED, new LocalizedTransition())
    .addTransition(ContainerState.LOCALIZING,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED,
        new ResourceFailedTransition())
    .addTransition(ContainerState.LOCALIZING, ContainerState.LOCALIZING,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER,
        new KillDuringLocalizationTransition())

    // From LOCALIZATION_FAILED State
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        new LocalizationFailedToDoneTransition())
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    // container not launched so kill is a no-op
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.KILL_CONTAINER)
    // container cleanup triggers a release of all resources
    // regardless of whether they were localized or not
    // LocalizedResource handles release event in all states
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_LOCALIZED)
    .addTransition(ContainerState.LOCALIZATION_FAILED,
        ContainerState.LOCALIZATION_FAILED,
        ContainerEventType.RESOURCE_FAILED)

    // From LOCALIZED State
    .addTransition(ContainerState.LOCALIZED, ContainerState.RUNNING,
        ContainerEventType.CONTAINER_LAUNCHED, new LaunchTransition())
    .addTransition(ContainerState.LOCALIZED, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.LOCALIZED, ContainerState.LOCALIZED,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.LOCALIZED, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())

    // From RUNNING State
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(true))
    .addTransition(ContainerState.RUNNING,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(true))
    .addTransition(ContainerState.RUNNING, ContainerState.RUNNING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.RUNNING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER, new KillTransition())
    .addTransition(ContainerState.RUNNING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new KilledExternallyTransition()) 

    // From CONTAINER_EXITED_WITH_SUCCESS State
    .addTransition(ContainerState.EXITED_WITH_SUCCESS, ContainerState.DONE,
        ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
        new ExitedWithSuccessToDoneTransition())
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_SUCCESS,
        ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.KILL_CONTAINER)

    // From EXITED_WITH_FAILURE State
    .addTransition(ContainerState.EXITED_WITH_FAILURE, ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            new ExitedWithFailureToDoneTransition())
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
        ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.EXITED_WITH_FAILURE,
                   ContainerState.EXITED_WITH_FAILURE,
                   ContainerEventType.KILL_CONTAINER)

    // From KILLING State.
    .addTransition(ContainerState.KILLING,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.CONTAINER_KILLED_ON_REQUEST,
        new ContainerKilledTransition())
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.RESOURCE_LOCALIZED,
        new LocalizedResourceDuringKillTransition())
    .addTransition(ContainerState.KILLING, 
        ContainerState.KILLING, 
        ContainerEventType.RESOURCE_FAILED)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.KILLING, ContainerState.KILLING,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_SUCCESS,
        ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
        new ExitedWithSuccessTransition(false))
    .addTransition(ContainerState.KILLING, ContainerState.EXITED_WITH_FAILURE,
        ContainerEventType.CONTAINER_EXITED_WITH_FAILURE,
        new ExitedWithFailureTransition(false))
    .addTransition(ContainerState.KILLING,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            new KillingToDoneTransition())
    // Handle a launched container during killing stage is a no-op
    // as cleanup container is always handled after launch container event
    // in the container launcher
    .addTransition(ContainerState.KILLING,
        ContainerState.KILLING,
        ContainerEventType.CONTAINER_LAUNCHED)

    // From CONTAINER_CLEANEDUP_AFTER_KILL State.
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
            ContainerState.DONE,
            ContainerEventType.CONTAINER_RESOURCES_CLEANEDUP,
            new ContainerCleanedupAfterKillToDoneTransition())
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
        UPDATE_DIAGNOSTICS_TRANSITION)
    .addTransition(ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        ContainerState.CONTAINER_CLEANEDUP_AFTER_KILL,
        EnumSet.of(ContainerEventType.KILL_CONTAINER,
            ContainerEventType.RESOURCE_FAILED,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE))

    // From DONE
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.KILL_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        ContainerEventType.INIT_CONTAINER)
    .addTransition(ContainerState.DONE, ContainerState.DONE,
       ContainerEventType.UPDATE_DIAGNOSTICS_MSG,
       UPDATE_DIAGNOSTICS_TRANSITION)
    // This transition may result when
    // we notify container of failed localization if localizer thread (for
    // that container) fails for some reason
    .addTransition(ContainerState.DONE, ContainerState.DONE,
        EnumSet.of(ContainerEventType.RESOURCE_FAILED,
            ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS,
            ContainerEventType.CONTAINER_EXITED_WITH_FAILURE))

    // create the topology tables
    .installTopology();

  private final StateMachine<ContainerState, ContainerEventType, ContainerEvent>
    stateMachine;

  public org.apache.hadoop.yarn.api.records.ContainerState getCurrentState() {
    switch (stateMachine.getCurrentState()) {
    case NEW:
    case LOCALIZING:
    case LOCALIZATION_FAILED:
    case LOCALIZED:
    case RUNNING:
    case EXITED_WITH_SUCCESS:
    case EXITED_WITH_FAILURE:
    case KILLING:
    case CONTAINER_CLEANEDUP_AFTER_KILL:
    case CONTAINER_RESOURCES_CLEANINGUP:
      return org.apache.hadoop.yarn.api.records.ContainerState.RUNNING;
    case DONE:
    default:
      return org.apache.hadoop.yarn.api.records.ContainerState.COMPLETE;
    }
  }
  
  @Override
  public long getLaunchStartTime(){
	  this.readLock.lock();
	    try {
	      return this.containerLaunchStartTime;
	    } finally {
	      this.readLock.unlock();
	    }
  }

  @Override
  public String getUser() {
    this.readLock.lock();
    try {
      return this.user;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Map<Path,List<String>> getLocalizedResources() {
    this.readLock.lock();
    try {
      if (ContainerState.LOCALIZED == getContainerState()) {
        return localizedResources;
      } else {
        return null;
      }
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Credentials getCredentials() {
    this.readLock.lock();
    try {
      return credentials;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerState getContainerState() {
    this.readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerLaunchContext getLaunchContext() {
    this.readLock.lock();
    try {
      return launchContext;
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerStatus cloneAndGetContainerStatus() {
    this.readLock.lock();
    try {
      return BuilderUtils.newContainerStatus(this.containerId,
        getCurrentState(), diagnostics.toString(), exitCode);
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public NMContainerStatus getNMContainerStatus() {
    this.readLock.lock();
    try {
      return NMContainerStatus.newInstance(this.containerId, getCurrentState(),
        getResource(), diagnostics.toString(), exitCode,
        containerTokenIdentifier.getPriority(),
        containerTokenIdentifier.getCreationTime());
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public Resource getResource() {
    return this.resource;
  }

  @Override
  public ContainerTokenIdentifier getContainerTokenIdentifier() {
    this.readLock.lock();
    try {
      return this.containerTokenIdentifier;
    } finally {
      this.readLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private void sendFinishedEvents() {
    // Inform the application
    @SuppressWarnings("rawtypes")
    EventHandler eventHandler = dispatcher.getEventHandler();
    eventHandler.handle(new ApplicationContainerFinishedEvent(containerId));
    // Remove the container from the resource-monitor
    eventHandler.handle(new ContainerStopMonitoringEvent(containerId));
    // Tell the logService too
    eventHandler.handle(new LogHandlerContainerFinishedEvent(
      containerId, exitCode));
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  private void sendLaunchEvent() {
    ContainersLauncherEventType launcherEvent =
        ContainersLauncherEventType.LAUNCH_CONTAINER;
    if (recoveredStatus == RecoveredContainerStatus.LAUNCHED) {
      // try to recover a container that was previously launched
      launcherEvent = ContainersLauncherEventType.RECOVER_CONTAINER;
    }
    containerLaunchStartTime = clock.getTime();
    dispatcher.getEventHandler().handle(
        new ContainersLauncherEvent(this, launcherEvent));
  }

  // Inform the ContainersMonitor to start monitoring the container's
  // resource usage.
  @SuppressWarnings("unchecked") // dispatcher not typed
  private void sendContainerMonitorStartEvent() {
      long pmemBytes = getResource().getMemory() * 1024 * 1024L;
      float pmemRatio = daemonConf.getFloat(
          YarnConfiguration.NM_VMEM_PMEM_RATIO,
          YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
      long vmemBytes = (long) (pmemRatio * pmemBytes);
      int cpuVcores = getResource().getVirtualCores();

      dispatcher.getEventHandler().handle(
          new ContainerStartMonitoringEvent(containerId,
              vmemBytes, pmemBytes, cpuVcores));
  }

  private void addDiagnostics(String... diags) {
    for (String s : diags) {
      this.diagnostics.append(s);
    }
    try {
      stateStore.storeContainerDiagnostics(containerId, diagnostics);
    } catch (IOException e) {
      LOG.warn("Unable to update diagnostics in state store for "
          + containerId, e);
    }
  }

  @SuppressWarnings("unchecked") // dispatcher not typed
  public void cleanup() {
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrc =
      new HashMap<LocalResourceVisibility, 
                  Collection<LocalResourceRequest>>();
    if (!publicRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PUBLIC, publicRsrcs);
    }
    if (!privateRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.PRIVATE, privateRsrcs);
    }
    if (!appRsrcs.isEmpty()) {
      rsrc.put(LocalResourceVisibility.APPLICATION, appRsrcs);
    }
    dispatcher.getEventHandler().handle(
        new ContainerLocalizationCleanupEvent(this, rsrc));
  }

  static class ContainerTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Just drain the event and change the state.
    }

  }
  
  
  public static class ContainerMemoryEvent{
	  
	  //0 for balloon, 1 for resume. -1 for null
	  public int type;
	  
	  //ballooned value, or resumed value
	  public int value;
	  
	  public ContainerMemoryEvent(int type, int value){
		  
		  this.type = type;
		  this.value= value;
	  }
	  
	  
	  public int getType(){
		  
		  return type;
	  }
	  
	  public int getValue(){
		  
		  return value;
	  }
	 
  }
  public class ContainerMonitor extends Thread{
	  
	    private ContainerMemoryState memoryState;
	  
		private String memoryPath;
		
		private String cpuPath;
		
		private String cpuSetPath;

		private String name;
		
		private String dockerId=null;
		
		private Queue<ContainerMemoryEvent> cmeQueue;
		
		private long currentUsedMemory;
		
		private long currentUsedSwap;

		private long limitedMemory;
		
		private boolean isRunning;

		private double SLACK_FACTOR;
		
		private boolean balloonedBefore;
		
		private boolean isBallooningWindow;
		
		private final ReadWriteLock stateLock;
		
		
		public ContainerMonitor(boolean isFlexible){
			
			//YARN container id
			this.name = containerId.toString();
			LOG.info("container id:"+this.name);
			//in terms of M
			this.currentUsedMemory= 0;
			this.currentUsedSwap  = 0;
			this.limitedMemory    = 0;
			//current memory state
			this.memoryState=ContainerMemoryState.RUNNING;
			//synchronized queue for insert to balloonned or reclaimed value
			this.cmeQueue  =new LinkedList<ContainerMemoryEvent>();
			this.SLACK_FACTOR=1.1;
			//record if it is in ballooning window
			this.isBallooningWindow=false;
			//record if it has ever ballooned before
			this.balloonedBefore=false;
			//for accessing control over memory state
		    this.stateLock = new ReentrantReadWriteLock();
			
		    
		}
		
		public synchronized void setBallooningWindow(boolean isInWindow){
			this.stateLock.writeLock().lock();
			try{
			this.isBallooningWindow=isInWindow;
			}finally{
				this.stateLock.writeLock().unlock();
			}
		}
		
		public synchronized boolean getBallooningWindow(){
			this.stateLock.readLock().lock();
			try{
			return this.isBallooningWindow;
			}finally{
				this.stateLock.readLock().unlock();
			}
		}
		
		//put into the synchronized queue
		public synchronized void putContainerMemoryEvent(ContainerMemoryEvent event){
			   cmeQueue.add(event);
		}
		
		public void setMemorySate(ContainerMemoryState memoryState){
			this.stateLock.writeLock().lock();
			try{
				this.memoryState=memoryState;
			}finally{
			  this.stateLock.writeLock().unlock();	
			}
		}
		
		
		public ContainerMemoryState getMemoryState(){
			this.stateLock.readLock().lock();
			try{
				return memoryState;
			}finally{
				this.stateLock.readLock().unlock();
			}
		}
		
		//poll from the synchronized queue
		public synchronized ContainerMemoryEvent getContainerMemoryEvent(){
			   //LOG.info("event queue length: "+cmeQueue.size());
			   if(cmeQueue.isEmpty()){
				   //LOG.info("empty event");
				   return new ContainerMemoryEvent(-1,0);
			   }
			   
			   
			   ContainerMemoryEvent topEvent=cmeQueue.poll();
			   //LOG.info("top event: "+topEvent.type+" value: "+topEvent.value);
			   int average = topEvent.getValue();
			   int count   = 1;
			   //if there are any other event, merge same types of event
			   if(cmeQueue.size() > 0){
			   while(topEvent.getType()==cmeQueue.peek().getType()){
				   ContainerMemoryEvent tempEvent=cmeQueue.poll();
				   average += tempEvent.getValue();
				   count++;
				   if(cmeQueue.isEmpty()){
					   break;
				   }
			   }
			   LOG.info(this.name+" memory event found "+count);
			   topEvent.value=(int)(average/count);
			   
			   }
			   cmeQueue.clear();
			
			
			return topEvent;
		}
		
		
		@Override
		public void run(){
			try {
				//wait untill the container is launched
				Thread.sleep(15000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			//get its docker id
			String[] containerIdCommands={"docker","inspect","--format={{.Id}}",name};
		    dockerId = runDockerUpdateCommand(containerIdCommands);
		    //initial the memory path
		    LOG.info(name+" get dockerId "+dockerId);
		    memoryPath= "/sys/fs/cgroup/memory/docker/"+dockerId+"/";
		    cpuPath   = "/sys/fs/cgroup/cpu/docker/"+dockerId+"/";
		    cpuSetPath= "/sys/fs/cgroup/cpuset/docker/"+dockerId+"/";
		    
			isRunning = true;
			//int count = 3;
			//int index = 0;
			while(stateMachine.getCurrentState() == ContainerState.RUNNING && isRunning){
				//update necessary metrics
				updateCgroupValues();
				ContainerMemoryEvent event = getContainerMemoryEvent();
				//ContainerMemoryState nextState;
				if(isFlexible)
			      LOG.info("$$$  "+this.name+" "+memoryState);
				else
			      LOG.info("$$$  "+this.name+" "+this.currentUsedMemory+" "+this.currentUsedSwap+" "+this.limitedMemory+"  $$$");
				//printCPUQuota();
				//printCPUSet();
				switch(memoryState){
				
				   case  RUNNING:
					     //for debug
					     if(event.getType()==0){
					    	 LOG.info("wrongstate: running receive balloon: "+this.name);
					     }
					     //process reclaim
					     if(event.getType()==1){
					    	 //suspend cpu for memory management
					    	 LOG.info(this.name+" reclaim");
					    	 suspendCpus();
					    	 if(updateConfiguredMemory(event.getValue())){
					    		 memoryState = ContainerMemoryState.SUSPENDING;
					    		
					    	 }
					    	 else{
					    		 resumeCpus();
					    	     memoryState=ContainerMemoryState.RUNNING;
					    	 }
					    	 continue;
					    	
					     }
					     //process shrink
					     else if(getIsSlack()){				    	 
					    	shrinkSlack();
					    	memoryState=ContainerMemoryState.RUNNING;
					    	continue;
					     }
					     //process out of memory
					     else if(getIsOutofMemory()){
					    	suspendCpus();
					    	memoryState=ContainerMemoryState.SUSPENDING;
					    	continue;
					     }
					   
					     break;
				   case  SUSPENDING:
					  
					     //process reclaim
					     if(event.getType() == 1){
					    	LOG.info(this.name+" reclaim");
					    	updateConfiguredMemory(event.getValue());
					    	memoryState=ContainerMemoryState.SUSPENDING;
					    	continue;
					     }
					     //process balloon
					     else if(event.getType()==0){
					    	LOG.info(this.name+" balloon"); 
					    	updateConfiguredMemory(event.getValue()); 
					    	memoryState=ContainerMemoryState.SUSPENDING;
					    	balloonedBefore=true;
					    	continue;
					     }
					     else if(getIsSwapping()){
					    	resumeCpuQuota();
					    	memoryState=ContainerMemoryState.RECOVERYING;
					    	continue;
					     }
					   
					     break;
					   
					   
				   case  RECOVERYING:
					    //for debug
					     if(event.getType()==0){
					    	 LOG.info("wrongstate: recoverying receive balloon: "+this.name);
					     }
					     //process reclaim 
					     if(event.getType()==1){
					    	 LOG.info(this.name+" reclaim"); 
					    	suspendCpus();
					    	if(updateConfiguredMemory(event.getValue())){
					    		 memoryState = ContainerMemoryState.SUSPENDING;
					    		
					    	}else{
						         resumeCpuQuota();
						         memoryState=ContainerMemoryState.RECOVERYING;
					    	}
					    	continue;
					     }else if(getIsOutofMemory()){
					    	 suspendCpus();
						     memoryState=ContainerMemoryState.SUSPENDING;
						     continue;
					     }
					     //process finish swapping
					     else if(getIsNoneSwapping()){
					    	 resumeCpus();
					    	 memoryState=ContainerMemoryState.RUNNING;
					    	 continue;
					     }
					     break;
				}
					
			
				//if we come here it means we need to sleep for 2s
				 try {
					    Thread.sleep(1000);
					} catch (InterruptedException e) {
					    e.printStackTrace();
				 }
				 
			}
			isRunning = false;
			memoryState = ContainerMemoryState.FINISH;
			LOG.info(this.name+"final current state: "
					+ stateMachine.getCurrentState());
		}
		
		
		//private void 
	
		private void updateCgroupValues(){
			
			getCurrentLimitedMemory();
			//get current used memory
			getCurrentUsedMemory();
			//get current used swap
			getCurrentUsedSwap();
		}
		
		private void suspendCpuQuota(){
			DockerCommandCpuQuota(1000);
		}
		
		private void resumeCpuQuota(){
			DockerCommandCpuQuota(-1);
		}
		
		private void suspendCpus(){
			
		   DockerCommandCpuQuota(1000);
		   Set<Integer> cpus=new HashSet<Integer>();
		   cpus.add(0);
		   DockerCommandCpuMap(cpus);
			
		}
		
		private void resumeCpus(){
			DockerCommandCpuQuota(-1);
			Set<Integer> cpus=new HashSet<Integer>();
			for(int i=0;i<virtualCores;i++){
				cpus.add(i);
			}
			DockerCommandCpuMap(cpus);	
			
			
		}
		
		
		
		//should return to suspending state
		//if memory < currentUsed
		//return true
		//else
		//return false
		private boolean updateConfiguredMemory(long memory){
			
	        LOG.info("update container memory: "+name+" old: "+limitedMemory+"new: "+memory);
	        if(memory > currentUsedMemory){
	        	DockerCommandMemory(memory);
	        	LOG.info("balloon-nswap: "+this.name+" from "+this.limitedMemory+"to: "+memory);
	        	return false;
	        }else{
                if(memory < currentUsedMemory*0.5){
                    //LOG.info("delay shrink");
                    memory = (long)(currentUsedMemory*0.5);
                    
                }
                
                long startTime=System.currentTimeMillis();   //current time
                long size = currentUsedMemory - memory;
	        	while(memory < currentUsedMemory){
	        		if(currentUsedMemory - 512 > memory)
	        			currentUsedMemory = currentUsedMemory - 512;
	        		DockerCommandMemory(currentUsedMemory);
                    //LOG.info("shrink-swap "+this.name+" from "+this.limitedMemory+" to "+this.currentUsedMemory);

                }
	        	DockerCommandMemory(memory);
	        	long endTime=System.currentTimeMillis();     //end time
	        	//LOG.info("shringk-swap "+this.name+" time: "+(endTime-startTime)+"size: "+size);
                return true;
	        }
	        
	        
	        
		}
		
		
		private void DockerCommandCpuMap(Set<Integer> cpus){
			if(!isFlexible){
	    		  return;
	    	  }
			  List<String> commandPrefix = new ArrayList<String>();
			  commandPrefix.add("docker");
			  commandPrefix.add("update");
			  List<String> commandCores = new ArrayList<String>();
			  commandCores.addAll(commandPrefix);
			  commandCores.add("--cpuset-cpus");
			  int index = 0;
			  String  coresStr=new String();
			  for(Integer core : cpus){
				  coresStr=coresStr+core.toString();
				  index++;
				  if(index < cpus.size()){
					  coresStr=coresStr+",";
				  }
			  }
			  
			  commandCores.add(coresStr);
			  commandCores.add(containerId.toString());
			  String[] commandArrayCores = commandCores.toArray(new String[commandCores.size()]);
			  this.runDockerUpdateCommand(commandArrayCores);
			  
			
		}
		
	    private void DockerCommandCpuQuota(Integer quota){
	    	  if(!isFlexible){
	    		  return;
	    	  }
			  List<String> commandPrefix = new ArrayList<String>();
			  commandPrefix.add("docker");
			  commandPrefix.add("update");
			  List<String> commandQuota = new ArrayList<String>();
			  commandQuota.addAll(commandPrefix);
			  commandQuota.add("--cpu-quota");
			  commandQuota.add(quota.toString());
			  commandQuota.add(containerId.toString());
			  String[] commandArrayQuota = commandQuota.toArray(new String[commandQuota.size()]);
			  this.runDockerUpdateCommand(commandArrayQuota);
			  
		  }
		 private void DockerCommandMemory(Long memory){
			 if(!isFlexible){
	    		  return;
	    	  }
              Long memory_swap=memory+131072;
			  List<String> commandPrefix = new ArrayList<String>();
			  commandPrefix.add("docker");
			  commandPrefix.add("update");
			  List<String> commandMemory = new ArrayList<String>();
			  commandMemory.addAll(commandPrefix);
			  commandMemory.add("--memory");
			  commandMemory.add(memory.toString()+"m");
              commandMemory.add("--memory-swap");
			  commandMemory.add(memory_swap.toString()+"m");
			  commandMemory.add(containerId.toString());
			  String[] commandArrayMemory = commandMemory.toArray(new String[commandMemory.size()]);
			  runDockerUpdateCommand(commandArrayMemory);
			  
			  
		   }
		 
		 
		 public void DockerCommandkill(){
			if(!isFlexible){
		    		  return;
		     }
			List<String> commandPrefix = new ArrayList<String>();
			commandPrefix.add("docker");
			commandPrefix.add("kill");
			commandPrefix.add(dockerId);
			String[] commandkill = commandPrefix.toArray(new String[commandPrefix.size()]);
			LOG.info("run docker kill: "+containerId);
			runDockerUpdateCommandNoLock(commandkill);
				
		 }
		
		
		 
		public void printCPUQuota(){
			if(!isRunning)
				return;
			
			String path=cpuPath+"cpu.cfs_quota_us";
			List<String> readlines=readFileLines(path);
			String cpuQuota=null;
			if(readlines!=null){
			  cpuQuota = readlines.get(0);
			//LOG.info("get limited memory:"+name+"  "+limitedMemory);
			}
			
			LOG.info("Container quota "+containerId+" "+cpuQuota);
		} 
		 
		public void printCPUSet(){
			if(!isRunning)
				return;
			
			String path=cpuSetPath+"cpuset.cpus";
			List<String> readlines=readFileLines(path);
			String cpuSet=null;
			if(readlines!=null){
			  cpuSet = readlines.get(0);
			//LOG.info("get limited memory:"+name+"  "+limitedMemory);
			}
			
			LOG.info("Container set  "+containerId+" "+cpuSet);
		}
		
		//pull by monitor, in termes of M
		public long getCurrentLimitedMemory(){
			if(!isRunning)
				return 0;
			String path=memoryPath+"memory.limit_in_bytes";
			List<String> readlines=readFileLines(path);
			if(readlines!=null){
			  limitedMemory = Long.parseLong(readFileLines(path).get(0))/(1024*1024);
			//LOG.info("get limited memory:"+name+"  "+limitedMemory);
			}
			return limitedMemory;
		}
		
		//pulled by nodemanager, in termes of M
		private long getCurrentUsedSwap(){
			if(!isRunning)
				return 0;
			
			String path=memoryPath+"memory.stat";
			List<String> readlines=readFileLines(path);
			if(readlines!=null){
			String SwapString=readlines.get(6);
			String SwapString1=SwapString.split("\\s++")[1];
			currentUsedSwap=Long.parseLong(SwapString1)/(1024*1024);
			//LOG.info("get swap memory:"+name+"  "+currentUsedSwap);
			}
			return currentUsedSwap;
		}
				
		
		//pulled by nodemanager, in termes of M
		public long getCurrentUsedMemory(){
			if(!isRunning)
				return 0;
			
			String path=memoryPath+"memory.usage_in_bytes";
			List<String> readlines=readFileLines(path);
			if(readlines!=null){
			currentUsedMemory=(int)(Long.parseLong(readlines.get(0))/(1024*1024));
			//LOG.info("get used memory:"+name+"  "+currentUsedMemory);
			}
			return currentUsedMemory;
		}
		
       public boolean getIsOutofMemory(){
			
			if(!isRunning)
				return false;
			
			updateCgroupValues();
		    
			if(currentUsedMemory+currentUsedSwap>limitedMemory){
				
			LOG.info("out of memory contianer detected: "+this.name+" "+currentUsedMemory
					+" "+currentUsedSwap+" "+limitedMemory);
				return true;
			}else{
				
				return false;
			}
		}
		
		//pulled by nodemanager
		public boolean getIsSwapping(){
			
			if(!isRunning)
				return false;
			
			updateCgroupValues();
		    
			if(!getIsOutofMemory()&&currentUsedSwap > 0){
				
			//LOG.info("swapping container detected: "+this.name);
				
			    return true;
			}else{
				return false;
			}
		}
		
		//get is it is none swapping()
		public boolean getIsNoneSwapping(){
			
			if(!isRunning)
				return false;
			updateCgroupValues();
			
			if(!getIsOutofMemory()&&currentUsedSwap <= (long)(0.2*currentUsedMemory) ){
		    LOG.info("non-swapping container detected: "+this.name);
				return true;
			}else{
				return false;
			}
			
		}
		
		
		
		
		
		public boolean getIsSlack(){
			
			if(!isRunning)
				return false;
			
			if(!balloonedBefore)
				return false;
			
			if(getBallooningWindow()){
				return false;
			}
			
			updateCgroupValues();
			
			if(limitedMemory > (currentUsedMemory+currentUsedSwap)*SLACK_FACTOR){
				
				LOG.info("slacking contianer detected: "+this.name);
				
				return true;
			}else{
				return false;
			}
		}
		
		
		public void shrinkSlack(){
			
			int targetSize = (int)((currentUsedMemory+currentUsedSwap)*SLACK_FACTOR);
			
			if(targetSize > 0)
			    updateConfiguredMemory(targetSize);
		}
		
		
		
		//recalim $claimSize MB memory from used container
		//usually called when this container is swapping
		//as much as its original allocated memory
		//@return how much memory is reclaimed
		public long reclaimMemory(int claimSize){
		    if(!isRunning)
		    	//TODO this is tricky, we should return current used to show that
		    	//all memory is released
		    	return 0;
		    long left=Math.max(getResource().getMemory(),limitedMemory-claimSize);
		    long reclaimed=limitedMemory-left;
            //put to event chanllel
		    putContainerMemoryEvent(new ContainerMemoryEvent(1,(int)left));
            LOG.info(name +" creclaim  reclaimed: "+reclaimed);
            LOG.info(name +" creclaim  left: "+left);
		    return reclaimed;
		}
		
		
		private String runDockerUpdateCommandNoLock(String[] command){
			//do nothing, if this container is not flexible 
			 String commandString=new String();
			 for(String c : command){
				 commandString += c;
				 commandString += " ";
			 }
			
			 ShellCommandExecutor shExec = null; 
			 int count = 1;
			 while(count < 100){
				
            if(!(stateMachine.getCurrentState() == ContainerState.RUNNING || stateMachine.getCurrentState() == ContainerState.KILLING)){
               
                break;
             } 
            boolean error=false;
			 //we try 10 times if fails due to device busy 
		     try { 
				  shExec = new ShellCommandExecutor(command);
				  shExec.execute();
				 
			    } catch (IOException e) {
			      int exitCode = shExec.getExitCode();
			      LOG.warn("Exception from Docker update with container ID: "
			            + name + " and exit code: " + exitCode, e); 
			      count++;
                LOG.info("tries for "+count);
                error=true;
			    } finally {
			      if (shExec != null) {
			        shExec.close();
			      }
			      LOG.info("finish docker commands: "+commandString);
			      //sleep for 100 mill seconds,before release lock
			      try{
			      Thread.sleep(500);
			      }catch(InterruptedException e1){
			    	  e1.printStackTrace();
			      }
			    }
		     
		     if(error){
		    	//sleep and retries
		    	try {
					Thread.sleep(100*count);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				continue;
		     }
		     
		      
		        break; 
			 }
			
			
			 return shExec.getOutput().trim();
			
		}
		private String runDockerUpdateCommand(String[] command){
			//do nothing, if this container is not flexible 
			 String commandString=new String();
			 for(String c : command){
				 commandString += c;
				 commandString += " ";
			 }
			
			 ShellCommandExecutor shExec = null; 
			 int count = 1;
			 while(count < 100){
				
             if(!(stateMachine.getCurrentState() == ContainerState.RUNNING || stateMachine.getCurrentState() == ContainerState.KILLING)){
                
                 break;
              } 
             boolean error=false;
             //add lock before execution
             context.getNodeMemoryManager().getDockerLock().lock();;
             LOG.info("run docker commands: "+commandString);
			 //we try 10 times if fails due to device busy 
		     try { 
				  shExec = new ShellCommandExecutor(command);
				  shExec.execute();
				 
			    } catch (IOException e) {
			      int exitCode = shExec.getExitCode();
			      LOG.warn("Exception from Docker update with container ID: "
			            + name + " and exit code: " + exitCode, e); 
			      count++;
                 LOG.info("tries for "+count);
                 error=true;
			    } finally {
			      if (shExec != null) {
			        shExec.close();
			      }
			      LOG.info("finish docker commands: "+commandString);
			      //sleep for 100 mill seconds,before release lock
			      try{
			      Thread.sleep(500);
			      }catch(InterruptedException e1){
			    	  e1.printStackTrace();
			      }
			      context.getNodeMemoryManager().getDockerLock().unlock();
			    }
		     
		     if(error){
		    	//sleep and retries
		    	try {
					Thread.sleep(100*count);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				continue;
		     }
		     
		      
		        break; 
			 }
			
			
			 return shExec.getOutput().trim();
		   }
		
		private List<String> readFileLines(String path){
			ArrayList<String> results= new ArrayList<String>();
			File file = new File(path);
		    BufferedReader reader = null;
		    boolean isError=false;
		    //LOG.info("try to read"+path);
		    try {
		        reader = new BufferedReader(new FileReader(file));
		        String tempString = null;
		        int line = 1;
		        while ((tempString = reader.readLine()) != null) {
		               results.add(tempString);
		               line++;
		            }
		            reader.close();
		        } catch (IOException e) {
		        	LOG.info("file error: "+e.toString());
		        	//if we come to here, then means read file causes errors;
		        	//if reports this errors mission errors, it means this containers
		        	//has terminated, but nodemanger did not delete it yet. we stop monitoring
		        	//here
		        	if(e.toString().contains("FileNotFoundException")){
		        	  isRunning=false;	
		        	}
		        	isError=true;
		        } finally {
		            if (reader != null) {
		                try {
		                    reader.close();
		                } catch (IOException e1) {
		                }
		            }
		        }
		    
			if(!isError){
			return results;
			}else{
		    return null;
			}
		}
	}
  

  /**
   * State transition when a NEW container receives the INIT_CONTAINER
   * message.
   * 
   * If there are resources to localize, sends a
   * ContainerLocalizationRequest (INIT_CONTAINER_RESOURCES) 
   * to the ResourceLocalizationManager and enters LOCALIZING state.
   * 
   * If there are no resources to localize, sends LAUNCH_CONTAINER event
   * and enters LOCALIZED state directly.
   * 
   * If there are any invalid resources specified, enters LOCALIZATION_FAILED
   * directly.
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class RequestResourcesTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      if (container.recoveredStatus == RecoveredContainerStatus.COMPLETED) {
        container.sendFinishedEvents();
        return ContainerState.DONE;
      } else if (container.recoveredAsKilled &&
          container.recoveredStatus == RecoveredContainerStatus.REQUESTED) {
        // container was killed but never launched
        container.metrics.killedContainer();
        NMAuditLogger.logSuccess(container.user,
            AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
            container.containerId.getApplicationAttemptId().getApplicationId(),
            container.containerId);
        container.metrics.releaseContainer(container.resource);
        container.sendFinishedEvents();
        return ContainerState.DONE;
      }

      final ContainerLaunchContext ctxt = container.launchContext;
      container.metrics.initingContainer();

      container.dispatcher.getEventHandler().handle(new AuxServicesEvent
          (AuxServicesEventType.CONTAINER_INIT, container));

      // Inform the AuxServices about the opaque serviceData
      Map<String,ByteBuffer> csd = ctxt.getServiceData();
      if (csd != null) {
        // This can happen more than once per Application as each container may
        // have distinct service data
        for (Map.Entry<String,ByteBuffer> service : csd.entrySet()) {
          container.dispatcher.getEventHandler().handle(
              new AuxServicesEvent(AuxServicesEventType.APPLICATION_INIT,
                  container.user, container.containerId
                      .getApplicationAttemptId().getApplicationId(),
                  service.getKey().toString(), service.getValue()));
        }
      }

      // Send requests for public, private resources
      Map<String,LocalResource> cntrRsrc = ctxt.getLocalResources();
      if (!cntrRsrc.isEmpty()) {
        try {
          for (Map.Entry<String,LocalResource> rsrc : cntrRsrc.entrySet()) {
            try {
              LocalResourceRequest req =
                  new LocalResourceRequest(rsrc.getValue());
              List<String> links = container.pendingResources.get(req);
              if (links == null) {
                links = new ArrayList<String>();
                container.pendingResources.put(req, links);
              }
              links.add(rsrc.getKey());
              storeSharedCacheUploadPolicy(container, req, rsrc.getValue()
                  .getShouldBeUploadedToSharedCache());
              switch (rsrc.getValue().getVisibility()) {
              case PUBLIC:
                container.publicRsrcs.add(req);
                break;
              case PRIVATE:
                container.privateRsrcs.add(req);
                break;
              case APPLICATION:
                container.appRsrcs.add(req);
                break;
              }
            } catch (URISyntaxException e) {
              LOG.info("Got exception parsing " + rsrc.getKey()
                  + " and value " + rsrc.getValue());
              throw e;
            }
          }
        } catch (URISyntaxException e) {
          // malformed resource; abort container launch
          LOG.warn("Failed to parse resource-request", e);
          container.cleanup();
          container.metrics.endInitingContainer();
          return ContainerState.LOCALIZATION_FAILED;
        }
        Map<LocalResourceVisibility, Collection<LocalResourceRequest>> req =
            new LinkedHashMap<LocalResourceVisibility,
                        Collection<LocalResourceRequest>>();
        if (!container.publicRsrcs.isEmpty()) {
          req.put(LocalResourceVisibility.PUBLIC, container.publicRsrcs);
        }
        if (!container.privateRsrcs.isEmpty()) {
          req.put(LocalResourceVisibility.PRIVATE, container.privateRsrcs);
        }
        if (!container.appRsrcs.isEmpty()) {
          req.put(LocalResourceVisibility.APPLICATION, container.appRsrcs);
        }
        
        container.dispatcher.getEventHandler().handle(
              new ContainerLocalizationRequestEvent(container, req));
        return ContainerState.LOCALIZING;
      } else {
        container.sendLaunchEvent();
        container.metrics.endInitingContainer();
        return ContainerState.LOCALIZED;
      }
    }
  }

  /**
   * Store the resource's shared cache upload policies
   * Given LocalResourceRequest can be shared across containers in
   * LocalResourcesTrackerImpl, we preserve the upload policies here.
   * In addition, it is possible for the application to create several
   * "identical" LocalResources as part of
   * ContainerLaunchContext.setLocalResources with different symlinks.
   * There is a corner case where these "identical" local resources have
   * different upload policies. For that scenario, upload policy will be set to
   * true as long as there is at least one LocalResource entry with
   * upload policy set to true.
   */
  private static void storeSharedCacheUploadPolicy(ContainerImpl container,
      LocalResourceRequest resourceRequest, Boolean uploadPolicy) {
    Boolean storedUploadPolicy =
        container.resourcesUploadPolicies.get(resourceRequest);
    if (storedUploadPolicy == null || (!storedUploadPolicy && uploadPolicy)) {
      container.resourcesUploadPolicies.put(resourceRequest, uploadPolicy);
    }
  }

  /**
   * Transition when one of the requested resources for this container
   * has been successfully localized.
   */
  static class LocalizedTransition implements
      MultipleArcTransition<ContainerImpl,ContainerEvent,ContainerState> {
    @SuppressWarnings("unchecked")
    @Override
    public ContainerState transition(ContainerImpl container,
        ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      LocalResourceRequest resourceRequest = rsrcEvent.getResource();
      Path location = rsrcEvent.getLocation();
      List<String> syms = container.pendingResources.remove(resourceRequest);
      if (null == syms) {
        LOG.warn("Localized unknown resource " + resourceRequest +
                 " for container " + container.containerId);
        assert false;
        // fail container?
        return ContainerState.LOCALIZING;
      }
      container.localizedResources.put(location, syms);

      // check to see if this resource should be uploaded to the shared cache
      // as well
      if (shouldBeUploadedToSharedCache(container, resourceRequest)) {
        container.resourcesToBeUploaded.put(resourceRequest, location);
      }
      if (!container.pendingResources.isEmpty()) {
        return ContainerState.LOCALIZING;
      }

      container.dispatcher.getEventHandler().handle(
          new ContainerLocalizationEvent(LocalizationEventType.
              CONTAINER_RESOURCES_LOCALIZED, container));

      container.sendLaunchEvent();
      container.metrics.endInitingContainer();

      // If this is a recovered container that has already launched, skip
      // uploading resources to the shared cache. We do this to avoid uploading
      // the same resources multiple times. The tradeoff is that in the case of
      // a recovered container, there is a chance that resources don't get
      // uploaded into the shared cache. This is OK because resources are not
      // acknowledged by the SCM until they have been uploaded by the node
      // manager.
      if (container.recoveredStatus != RecoveredContainerStatus.LAUNCHED
          && container.recoveredStatus != RecoveredContainerStatus.COMPLETED) {
        // kick off uploads to the shared cache
        container.dispatcher.getEventHandler().handle(
            new SharedCacheUploadEvent(container.resourcesToBeUploaded, container
                .getLaunchContext(), container.getUser(),
                SharedCacheUploadEventType.UPLOAD));
      }

      return ContainerState.LOCALIZED;
    }
  }

  /**
   * Transition from LOCALIZED state to RUNNING state upon receiving
   * a CONTAINER_LAUNCHED event
   */
  static class LaunchTransition extends ContainerTransition {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.sendContainerMonitorStartEvent();
      container.metrics.runningContainer();
      container.wasLaunched  = true;
      long duration = clock.getTime() - container.containerLaunchStartTime;
      container.metrics.addContainerLaunchDuration(duration);
      //we start docker container monitor thread here
      container.containerMonitor.start();

      if (container.recoveredAsKilled) {
        LOG.info("Killing " + container.containerId
            + " due to recovered as killed");
        container.addDiagnostics("Container recovered as killed.\n");
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }
    }
  }

  /**
   * Transition from RUNNING or KILLING state to EXITED_WITH_SUCCESS state
   * upon EXITED_WITH_SUCCESS message.
   */
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class ExitedWithSuccessTransition extends ContainerTransition {

    boolean clCleanupRequired;

    public ExitedWithSuccessTransition(boolean clCleanupRequired) {
      this.clCleanupRequired = clCleanupRequired;
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Set exit code to 0 on success    	
      container.exitCode = 0;
    	
      // TODO: Add containerWorkDir to the deletion service.

      if (clCleanupRequired) {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }

      container.cleanup();
    }
  }

  /**
   * Transition to EXITED_WITH_FAILURE state upon
   * CONTAINER_EXITED_WITH_FAILURE state.
   **/
  @SuppressWarnings("unchecked")  // dispatcher not typed
  static class ExitedWithFailureTransition extends ContainerTransition {

    boolean clCleanupRequired;

    public ExitedWithFailureTransition(boolean clCleanupRequired) {
      this.clCleanupRequired = clCleanupRequired;
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      container.exitCode = exitEvent.getExitCode();
      if (exitEvent.getDiagnosticInfo() != null) {
        container.addDiagnostics(exitEvent.getDiagnosticInfo(), "\n");
      }

      // TODO: Add containerWorkDir to the deletion service.
      // TODO: Add containerOuputDir to the deletion service.

      if (clCleanupRequired) {
        container.dispatcher.getEventHandler().handle(
            new ContainersLauncherEvent(container,
                ContainersLauncherEventType.CLEANUP_CONTAINER));
      }

      container.cleanup();
    }
  }

  /**
   * Transition to EXITED_WITH_FAILURE upon receiving KILLED_ON_REQUEST
   */
  static class KilledExternallyTransition extends ExitedWithFailureTransition {
    KilledExternallyTransition() {
      super(true);
    }

    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      super.transition(container, event);
      container.addDiagnostics("Killed by external signal\n");
    }
  }

  /**
   * Transition from LOCALIZING to LOCALIZATION_FAILED upon receiving
   * RESOURCE_FAILED event.
   */
  static class ResourceFailedTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {

      ContainerResourceFailedEvent rsrcFailedEvent =
          (ContainerResourceFailedEvent) event;
      container.addDiagnostics(rsrcFailedEvent.getDiagnosticMessage(), "\n");

      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.cleanup();
      container.metrics.endInitingContainer();
    }
  }

  /**
   * Transition from LOCALIZING to KILLING upon receiving
   * KILL_CONTAINER event.
   */
  static class KillDuringLocalizationTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Inform the localizer to decrement reference counts and cleanup
      // resources.
      container.cleanup();
      container.metrics.endInitingContainer();
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.exitCode = killEvent.getContainerExitStatus();
      container.addDiagnostics(killEvent.getDiagnostic(), "\n");
      container.addDiagnostics("Container is killed before being launched.\n");
    }
  }

  /**
   * Remain in KILLING state when receiving a RESOURCE_LOCALIZED request
   * while in the process of killing.
   */
  static class LocalizedResourceDuringKillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerResourceLocalizedEvent rsrcEvent = (ContainerResourceLocalizedEvent) event;
      List<String> syms =
          container.pendingResources.remove(rsrcEvent.getResource());
      if (null == syms) {
        LOG.warn("Localized unknown resource " + rsrcEvent.getResource() +
                 " for container " + container.containerId);
        assert false;
        // fail container?
        return;
      }
      container.localizedResources.put(rsrcEvent.getLocation(), syms);
    }
  }

  /**
   * Transitions upon receiving KILL_CONTAINER:
   * - LOCALIZED -> KILLING
   * - RUNNING -> KILLING
   */
  @SuppressWarnings("unchecked") // dispatcher not typed
  static class KillTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      // Kill the process/process-grp
      container.dispatcher.getEventHandler().handle(
          new ContainersLauncherEvent(container,
              ContainersLauncherEventType.CLEANUP_CONTAINER));
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.addDiagnostics(killEvent.getDiagnostic(), "\n");
      container.exitCode = killEvent.getContainerExitStatus();
    }
  }

  /**
   * Transition from KILLING to CONTAINER_CLEANEDUP_AFTER_KILL
   * upon receiving CONTAINER_KILLED_ON_REQUEST.
   */
  static class ContainerKilledTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerExitEvent exitEvent = (ContainerExitEvent) event;
      if (container.hasDefaultExitCode()) {
        container.exitCode = exitEvent.getExitCode();
      }

      if (exitEvent.getDiagnosticInfo() != null) {
        container.addDiagnostics(exitEvent.getDiagnosticInfo(), "\n");
      }

      // The process/process-grp is killed. Decrement reference counts and
      // cleanup resources
      container.cleanup();
    }
  }

  /**
   * Handle the following transitions:
   * - {LOCALIZATION_FAILED, EXITED_WITH_SUCCESS, EXITED_WITH_FAILURE,
   *    KILLING, CONTAINER_CLEANEDUP_AFTER_KILL}
   *   -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ContainerDoneTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    @SuppressWarnings("unchecked")
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.releaseContainer(container.resource);
      container.sendFinishedEvents();
      //if the current state is NEW it means the CONTAINER_INIT was never 
      // sent for the event, thus no need to send the CONTAINER_STOP
      if (container.getCurrentState() 
          != org.apache.hadoop.yarn.api.records.ContainerState.NEW) {
        container.dispatcher.getEventHandler().handle(new AuxServicesEvent
            (AuxServicesEventType.CONTAINER_STOP, container));
      }
      container.context.getNodeStatusUpdater().sendOutofBandHeartBeat();
    }
  }

  /**
   * Handle the following transition:
   * - NEW -> DONE upon KILL_CONTAINER
   */
  static class KillOnNewTransition extends ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerKillEvent killEvent = (ContainerKillEvent) event;
      container.exitCode = killEvent.getContainerExitStatus();
      container.addDiagnostics(killEvent.getDiagnostic(), "\n");
      container.addDiagnostics("Container is killed before being launched.\n");
      container.metrics.killedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - LOCALIZATION_FAILED -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class LocalizationFailedToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.failedContainer();
      NMAuditLogger.logFailure(container.user,
          AuditConstants.FINISH_FAILED_CONTAINER, "ContainerImpl",
          "Container failed with state: " + container.getContainerState(),
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - EXITED_WITH_SUCCESS -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ExitedWithSuccessToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.endRunningContainer();
      container.metrics.completedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_SUCCESS_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - EXITED_WITH_FAILURE -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ExitedWithFailureToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      if (container.wasLaunched) {
        container.metrics.endRunningContainer();
      }
      container.metrics.failedContainer();
      NMAuditLogger.logFailure(container.user,
          AuditConstants.FINISH_FAILED_CONTAINER, "ContainerImpl",
          "Container failed with state: " + container.getContainerState(),
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * - KILLING -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class KillingToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      container.metrics.killedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Handle the following transition:
   * CONTAINER_CLEANEDUP_AFTER_KILL -> DONE upon CONTAINER_RESOURCES_CLEANEDUP
   */
  static class ContainerCleanedupAfterKillToDoneTransition extends
      ContainerDoneTransition {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      if (container.wasLaunched) {
        container.metrics.endRunningContainer();
      }
      container.metrics.killedContainer();
      NMAuditLogger.logSuccess(container.user,
          AuditConstants.FINISH_KILLED_CONTAINER, "ContainerImpl",
          container.containerId.getApplicationAttemptId().getApplicationId(),
          container.containerId);
      super.transition(container, event);
    }
  }

  /**
   * Update diagnostics, staying in the same state.
   */
  static class ContainerDiagnosticsUpdateTransition implements
      SingleArcTransition<ContainerImpl, ContainerEvent> {
    @Override
    public void transition(ContainerImpl container, ContainerEvent event) {
      ContainerDiagnosticsUpdateEvent updateEvent =
          (ContainerDiagnosticsUpdateEvent) event;
      container.addDiagnostics(updateEvent.getDiagnosticsUpdate(), "\n");
      try {
        container.stateStore.storeContainerDiagnostics(container.containerId,
            container.diagnostics);
      } catch (IOException e) {
        LOG.warn("Unable to update state store diagnostics for "
            + container.containerId, e);
      }
    }
  }

  @Override
  public void handle(ContainerEvent event) {
    try {
      this.writeLock.lock();

      ContainerId containerID = event.getContainerID();
      LOG.debug("Processing " + containerID + " of type " + event.getType());

      ContainerState oldState = stateMachine.getCurrentState();
      ContainerState newState = null;
      try {
        newState =
            stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.warn("Can't handle this event at current state: Current: ["
            + oldState + "], eventType: [" + event.getType() + "]", e);
      }
      if (oldState != newState) {
        LOG.info("Container " + containerID + " transitioned from "
            + oldState
            + " to " + newState);
      }
    } finally {
      this.writeLock.unlock();
    }
  }

  @Override
  public String toString() {
    this.readLock.lock();
    try {
      return ConverterUtils.toString(this.containerId);
    } finally {
      this.readLock.unlock();
    }
  }

  private boolean hasDefaultExitCode() {
    return (this.exitCode == ContainerExitStatus.INVALID);
  }

  /**
   * Returns whether the specific resource should be uploaded to the shared
   * cache.
   */
  private static boolean shouldBeUploadedToSharedCache(ContainerImpl container,
      LocalResourceRequest resource) {
    return container.resourcesUploadPolicies.get(resource);
  }

@Override
public ContainerMonitor getContainerMonitor() {
	//return container monitor for this container
	return containerMonitor;
}

}
