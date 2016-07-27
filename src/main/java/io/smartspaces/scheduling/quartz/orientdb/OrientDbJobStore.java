/*
 * Copyright (C) 2016 Keith M. Hughes
 * Forked from code (c) Michael S. Klishin, Alex Petrov, 2011-2015.
 * Forked from code from MuleSoft.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * $Id: MongoDBJobStore.java 253170 2014-01-06 02:28:03Z waded $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package io.smartspaces.scheduling.quartz.orientdb;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;

import io.smartspaces.scheduling.quartz.orientdb.util.Keys;

public class OrientDbJobStore implements JobStore, Constants {

  private StandardOrientDbStoreAssembler assembler = new StandardOrientDbStoreAssembler();

  MongoClient mongo;
  String collectionPrefix = "quartz_";
  String dbName;
  String authDbName;
  String schedulerName;
  String instanceId;
  String[] addresses;
  String orientdbUri;
  String username;
  String password;
  long misfireThreshold = 5000;
  long triggerTimeoutMillis = 10 * 60 * 1000L;
  long jobTimeoutMillis = 10 * 60 * 1000L;
  private boolean clustered = false;
  long clusterCheckinIntervalMillis = 7500;

  // Options for the Mongo client.
  Boolean mongoOptionSocketKeepAlive;
  Integer mongoOptionMaxConnectionsPerHost;
  Integer mongoOptionConnectTimeoutMillis;
  Integer mongoOptionSocketTimeoutMillis; // read timeout
  Integer mongoOptionThreadsAllowedToBlockForConnectionMultiplier;
  Boolean mongoOptionEnableSSL;
  Boolean mongoOptionSslInvalidHostNameAllowed;

  int mongoOptionWriteConcernTimeoutMillis = 5000;

  public OrientDbJobStore() {
  }

  public OrientDbJobStore(final MongoClient mongo) {
    this.mongo = mongo;
  }

  public OrientDbJobStore(final String orientdbUri, final String username, final String password) {
    this.orientdbUri = orientdbUri;
    this.username = username;
    this.password = password;
  }

  /**
   * Override to change class loading mechanism, to e.g. dynamic
   * 
   * @param original
   *          default provided by Quartz
   * @return loader to use for loading of Quartz Jobs' classes
   */
  protected ClassLoadHelper getClassLoaderHelper(ClassLoadHelper original) {
    return original;
  }

  @Override
  public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler)
      throws SchedulerConfigException {
    assembler.build(this, loadHelper, signaler);

    if (isClustered()) {
      try {
        assembler.triggerRecoverer.recover();
      } catch (JobPersistenceException e) {
        throw new SchedulerConfigException("Cannot recover triggers", e);
      }
      assembler.checkinExecutor.start();
    }

    ensureIndexes();
  }

  @Override
  public void schedulerStarted() throws SchedulerException {
    // No-op
  }

  @Override
  public void schedulerPaused() {
    // No-op
  }

  @Override
  public void schedulerResumed() {
  }

  @Override
  public void shutdown() {
    assembler.checkinExecutor.shutdown();
    assembler.orientDbConnector.shutdown();
  }

  @Override
  public boolean supportsPersistence() {
    return true;
  }

  @Override
  public long getEstimatedTimeToReleaseAndAcquireTrigger() {
    // this will vary...
    return 200;
  }

  /**
   * Set whether this instance is part of a cluster.
   */
  public void setIsClustered(boolean isClustered) {
    this.clustered = isClustered;
  }

  @Override
  public boolean isClustered() {
    return clustered;
  }

  /**
   * Set the frequency (in milliseconds) at which this instance "checks-in" with
   * the other instances of the cluster.
   *
   * Affects the rate of detecting failed instances.
   */
  public void setClusterCheckinInterval(long clusterCheckinInterval) {
    this.clusterCheckinIntervalMillis = clusterCheckinInterval;
  }

  /**
   * Job and Trigger storage Methods
   */
  @Override
  public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
      throws JobPersistenceException {
    assembler.persister.storeJobAndTrigger(newJob, newTrigger);
  }

  @Override
  public void storeJob(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
    assembler.getJobDao().storeJobInMongo(newJob, replaceExisting);
  }

  @Override
  public void storeJobsAndTriggers(Map<JobDetail, List<Trigger>> triggersAndJobs, boolean replace)
      throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
    return assembler.persister.removeJob(jobKey);
  }

  @Override
  public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
    return assembler.persister.removeJobs(jobKeys);
  }

  @Override
  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    return assembler.jobDao.retrieveJob(jobKey);
  }

  @Override
  public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
      throws JobPersistenceException {
    assembler.persister.storeTrigger(newTrigger, replaceExisting);
  }

  @Override
  public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.persister.removeTrigger(triggerKey);
  }

  @Override
  public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
    return assembler.persister.removeTriggers(triggerKeys);
  }

  @Override
  public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger)
      throws JobPersistenceException {
    return assembler.persister.replaceTrigger(triggerKey, newTrigger);
  }

  @Override
  public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.triggerDao.getTrigger(triggerKey);
  }

  @Override
  public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
    return assembler.jobDao.exists(jobKey);
  }

  @Override
  public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.triggerDao.exists(Keys.toFilter(triggerKey));
  }

  @Override
  public void clearAllSchedulingData() throws JobPersistenceException {
    assembler.jobDao.clear();
    assembler.triggerDao.clear();
    assembler.calendarDao.clear();
    assembler.pausedJobGroupsDao.remove();
    assembler.pausedTriggerGroupsDao.remove();
  }

  @Override
  public void storeCalendar(String name, Calendar calendar, boolean replaceExisting,
      boolean updateTriggers) throws JobPersistenceException {
    // TODO implement updating triggers
    if (updateTriggers) {
      throw new UnsupportedOperationException("Updating triggers is not supported.");
    }

    assembler.calendarDao.store(name, calendar);
  }

  @Override
  public boolean removeCalendar(String calName) throws JobPersistenceException {
    return assembler.calendarDao.remove(calName);
  }

  @Override
  public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
    return assembler.calendarDao.retrieveCalendar(calName);
  }

  @Override
  public int getNumberOfJobs() throws JobPersistenceException {
    return assembler.jobDao.getCount();
  }

  @Override
  public int getNumberOfTriggers() throws JobPersistenceException {
    return assembler.triggerDao.getCount();
  }

  @Override
  public int getNumberOfCalendars() throws JobPersistenceException {
    return assembler.calendarDao.getCount();
  }

  @Override
  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
    return assembler.jobDao.getJobKeys(matcher);
  }

  @Override
  public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    return assembler.triggerDao.getTriggerKeys(matcher);
  }

  @Override
  public List<String> getJobGroupNames() throws JobPersistenceException {
    return assembler.jobDao.getGroupNames();
  }

  @Override
  public List<String> getTriggerGroupNames() throws JobPersistenceException {
    return assembler.triggerDao.getGroupNames();
  }

  @Override
  public List<String> getCalendarNames() throws JobPersistenceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
    return assembler.persister.getTriggersForJob(jobKey);
  }

  @Override
  public TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
    return assembler.triggerStateManager.getState(triggerKey);
  }

  @Override
  public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    assembler.triggerStateManager.pause(triggerKey);
  }

  @Override
  public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    return assembler.triggerStateManager.pause(matcher);
  }

  @Override
  public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
    assembler.triggerStateManager.resume(triggerKey);
  }

  @Override
  public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
      throws JobPersistenceException {
    return assembler.triggerStateManager.resume(matcher);
  }

  @Override
  public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
    return assembler.triggerStateManager.getPausedTriggerGroups();
  }

  // only for tests
  public Set<String> getPausedJobGroups() throws JobPersistenceException {
    return assembler.pausedJobGroupsDao.getPausedGroups();
  }

  @Override
  public void pauseAll() throws JobPersistenceException {
    assembler.triggerStateManager.pauseAll();
  }

  @Override
  public void resumeAll() throws JobPersistenceException {
    assembler.triggerStateManager.resumeAll();
  }

  @Override
  public void pauseJob(JobKey jobKey) throws JobPersistenceException {
    assembler.triggerStateManager.pauseJob(jobKey);
  }

  @Override
  public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    return assembler.triggerStateManager.pauseJobs(groupMatcher);
  }

  @Override
  public void resumeJob(JobKey jobKey) throws JobPersistenceException {
    assembler.triggerStateManager.resume(jobKey);
  }

  @Override
  public Collection<String> resumeJobs(GroupMatcher<JobKey> groupMatcher)
      throws JobPersistenceException {
    return assembler.triggerStateManager.resumeJobs(groupMatcher);
  }

  @Override
  public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow)
      throws JobPersistenceException {
    return assembler.triggerRunner.acquireNext(noLaterThan, maxCount, timeWindow);
  }

  @Override
  public void releaseAcquiredTrigger(OperableTrigger trigger) throws JobPersistenceException {
    assembler.lockManager.unlockAcquiredTrigger(trigger);
  }

  @Override
  public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
      throws JobPersistenceException {
    return assembler.triggerRunner.triggersFired(triggers);
  }

  @Override
  public void triggeredJobComplete(OperableTrigger trigger, JobDetail job,
      CompletedExecutionInstruction triggerInstCode) throws JobPersistenceException {
    assembler.jobCompleteHandler.jobComplete(trigger, job, triggerInstCode);
  }

  @Override
  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  @Override
  public void setInstanceName(String schedName) {
    // Used as part of cluster node identifier:
    schedulerName = schedName;
  }

  @Override
  public void setThreadPoolSize(int poolSize) {
    // No-op
  }

  public void setAddresses(String addresses) {
    this.addresses = addresses.split(",");
  }

  public MongoCollection<Document> getJobCollection() {
    return assembler.jobDao.getCollection();
  }

  public MongoCollection<Document> getTriggerCollection() {
    return assembler.triggerDao.getCollection();
  }

  public MongoCollection<Document> getCalendarCollection() {
    return assembler.calendarDao.getCollection();
  }

  public MongoCollection<Document> getLocksCollection() {
    return assembler.locksDao.getCollection();
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setCollectionPrefix(String prefix) {
    collectionPrefix = prefix + "_";
  }

  public void setOrientdbUri(final String orientdbUri) {
    this.orientdbUri = orientdbUri;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setMisfireThreshold(long misfireThreshold) {
    this.misfireThreshold = misfireThreshold;
  }

  public void setTriggerTimeoutMillis(long triggerTimeoutMillis) {
    this.triggerTimeoutMillis = triggerTimeoutMillis;
  }

  public void setJobTimeoutMillis(long jobTimeoutMillis) {
    this.jobTimeoutMillis = jobTimeoutMillis;
  }

  /**
   * Initializes the indexes for the scheduler collections.
   *
   * @throws SchedulerConfigException
   *           if an error occurred communicating with the OrientDB server.
   */
  private void ensureIndexes() throws SchedulerConfigException {
    try {
      // Indexes are to be declared as group then name. This is important
      // as the quartz API allows for the searching of jobs and triggers
      // using a group matcher. To be able to use the compound index using
      // group alone (as the API allows), group must be the first key in
      // that index.
      //
      // To be consistent, all such indexes are ensured in the order group
      // then name. The previous indexes are removed after we have
      // "ensured" the new ones.

      assembler.jobDao.createIndex();
      assembler.triggerDao.createIndex();
      assembler.locksDao.createIndex(isClustered());
      assembler.calendarDao.createIndex();
      assembler.schedulerDao.createIndex();

      try {
        // Drop the old indexes that were declared as name then group
        // rather than group then name
        assembler.jobDao.dropIndex();
        assembler.triggerDao.dropIndex();
        assembler.locksDao.dropIndex();
      } catch (MongoCommandException cfe) {
        // Ignore, the old indexes have already been removed
      }
    } catch (MongoException e) {
      throw new SchedulerConfigException("Error while initializing the indexes", e);
    }
  }

  public void setMongoOptionMaxConnectionsPerHost(int maxConnectionsPerHost) {
    this.mongoOptionMaxConnectionsPerHost = maxConnectionsPerHost;
  }

  public void setMongoOptionConnectTimeoutMillis(int maxConnectWaitTime) {
    this.mongoOptionConnectTimeoutMillis = maxConnectWaitTime;
  }

  public void setMongoOptionSocketTimeoutMillis(int socketTimeoutMillis) {
    this.mongoOptionSocketTimeoutMillis = socketTimeoutMillis;
  }

  public void setMongoOptionThreadsAllowedToBlockForConnectionMultiplier(
      int threadsAllowedToBlockForConnectionMultiplier) {
    this.mongoOptionThreadsAllowedToBlockForConnectionMultiplier =
        threadsAllowedToBlockForConnectionMultiplier;
  }

  public void setMongoOptionSocketKeepAlive(boolean socketKeepAlive) {
    this.mongoOptionSocketKeepAlive = socketKeepAlive;
  }

  public void setMongoOptionEnableSSL(boolean enableSSL) {
    this.mongoOptionEnableSSL = enableSSL;
  }

  public void setMongoOptionSslInvalidHostNameAllowed(boolean sslInvalidHostNameAllowed) {
    this.mongoOptionSslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
  }

  public void setMongoOptionWriteConcernTimeoutMillis(int writeConcernTimeoutMillis) {
    this.mongoOptionWriteConcernTimeoutMillis = writeConcernTimeoutMillis;
  }

  public String getAuthDbName() {
    return authDbName;
  }

  public void setAuthDbName(String authDbName) {
    this.authDbName = authDbName;
  }
}
