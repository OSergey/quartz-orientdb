/*
 * Copyright (c) 2018 Serhii Ovsiuk
 * Forked from code (c) Keith M. Hughes 2016
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

package io.smartspaces.scheduling.quartz.orientdb.internal;

import io.smartspaces.scheduling.quartz.orientdb.OrientDbJobStore;
import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.CheckinExecutor;
import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.RecoveryTriggerFactory;
import io.smartspaces.scheduling.quartz.orientdb.internal.cluster.TriggerRecoverer;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardCalendarDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardJobDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardLockDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardPausedJobGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardPausedTriggerGroupsDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardSchedulerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.dao.StandardTriggerDao;
import io.smartspaces.scheduling.quartz.orientdb.internal.db.StandardOrientDbConnector;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.MisfireHandler;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.StandardMisfireHandler;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerConverter;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Clock;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.QueryHelper;

import org.quartz.SchedulerConfigException;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

/**
 * This class creates the database connection, does initial database schema
 * building, etc.
 */
public class StandardOrientDbStoreAssembler {

  private StandardOrientDbConnector orientDbConnector;
  private JobCompleteHandler jobCompleteHandler;
  private TriggerStateManager triggerStateManager;
  private TriggerRunner triggerRunner;
  private TriggerAndJobPersister persister;

  private MisfireHandler misfireHandler;

  private StandardCalendarDao calendarDao;
  private StandardJobDao jobDao;
  private StandardSchedulerDao schedulerDao;
  private StandardPausedJobGroupsDao pausedJobGroupsDao;
  private StandardPausedTriggerGroupsDao pausedTriggerGroupsDao;
  private StandardTriggerDao triggerDao;

  private TriggerRecoverer triggerRecoverer;
  private CheckinExecutor checkinExecutor;

  private QueryHelper queryHelper = new QueryHelper();
  private TriggerConverter triggerConverter;

  private long dbRetryInterval;

  /**
   * The clock to use.
   */
  private Clock clock;

  public void build(OrientDbJobStore jobStore, ClassLoadHelper classLoadHelper,
      SchedulerSignaler signaler, Clock clock, long dbRetryInterval)
      throws SchedulerConfigException {
    this.clock = clock;
    this.dbRetryInterval = dbRetryInterval;

    orientDbConnector = createOrientDbConnector(jobStore);

    jobDao = createJobDao(jobStore, classLoadHelper);

    triggerConverter = new TriggerConverter(jobDao, classLoadHelper, jobStore.getCollectionPrefix());

    triggerDao = createTriggerDao(jobStore);
    calendarDao = createCalendarDao(jobStore);
    pausedJobGroupsDao = createPausedJobGroupsDao(jobStore);
    pausedTriggerGroupsDao = createPausedTriggerGroupsDao(jobStore);
    schedulerDao = createSchedulerDao(jobStore);

    persister = createTriggerAndJobPersister();

    jobCompleteHandler = createJobCompleteHandler(signaler);

    triggerStateManager = createTriggerStateManager();

    misfireHandler = createMisfireHandler(jobStore, signaler);

    RecoveryTriggerFactory recoveryTriggerFactory =
        new RecoveryTriggerFactory(jobStore.getInstanceId(), clock);

    triggerRecoverer =
        new TriggerRecoverer(persister, triggerDao, jobDao, recoveryTriggerFactory, misfireHandler);

    triggerRunner = createTriggerRunner(misfireHandler);

    checkinExecutor = createCheckinExecutor(jobStore);
  }

  public StandardOrientDbConnector getOrientDbConnector() {
    return orientDbConnector;
  }

  /**
   * Get the misfire handler.
   * 
   * @return the misfire handler
   */
  public MisfireHandler getMisfireHandler() {
    return misfireHandler;
  }

  public JobCompleteHandler getJobCompleteHandler() {
    return jobCompleteHandler;
  }

  public TriggerStateManager getTriggerStateManager() {
    return triggerStateManager;
  }

  public TriggerRunner getTriggerRunner() {
    return triggerRunner;
  }

  public TriggerAndJobPersister getPersister() {
    return persister;
  }

  public TriggerRecoverer getTriggerRecoverer() {
    return triggerRecoverer;
  }

  public CheckinExecutor getCheckinExecutor() {
    return checkinExecutor;
  }

  public QueryHelper getQueryHelper() {
    return queryHelper;
  }

  public TriggerConverter getTriggerConverter() {
    return triggerConverter;
  }

  public StandardCalendarDao getCalendarDao() {
    return calendarDao;
  }

  public StandardJobDao getJobDao() {
    return jobDao;
  }

  public StandardSchedulerDao getSchedulerDao() {
    return schedulerDao;
  }

  public StandardPausedJobGroupsDao getPausedJobGroupsDao() {
    return pausedJobGroupsDao;
  }

  public StandardPausedTriggerGroupsDao getPausedTriggerGroupsDao() {
    return pausedTriggerGroupsDao;
  }

  public StandardTriggerDao getTriggerDao() {
    return triggerDao;
  }

  private CheckinExecutor createCheckinExecutor(OrientDbJobStore jobStore) {
    return null;
    // return new CheckinExecutor(new CheckinTask(schedulerDao),
    // jobStore.getClusterCheckinIntervalMillis(),
    // jobStore.getInstanceId());
  }

  private StandardCalendarDao createCalendarDao(OrientDbJobStore jobStore) {
    return new StandardCalendarDao(this, jobStore.getCollectionPrefix());
  }

  private StandardJobDao createJobDao(OrientDbJobStore jobStore, ClassLoadHelper loadHelper) {
    JobConverter jobConverter = new JobConverter(loadHelper, jobStore.getCollectionPrefix());
    return new StandardJobDao(this, queryHelper, jobConverter, jobStore.getCollectionPrefix());
  }

  private JobCompleteHandler createJobCompleteHandler(SchedulerSignaler signaler) {
    return new JobCompleteHandler(persister, signaler, jobDao, triggerDao);
  }

  private StandardLockDao createLocksDao(OrientDbJobStore jobStore) {
    return new StandardLockDao(this, clock, jobStore.getInstanceId());
  }

  private MisfireHandler createMisfireHandler(OrientDbJobStore jobStore,
      SchedulerSignaler signaler) {
    return new StandardMisfireHandler(persister, triggerDao, calendarDao, jobStore.getMisfireThreshold(),
        dbRetryInterval, orientDbConnector, clock, signaler);
  }

  private StandardOrientDbConnector createOrientDbConnector(OrientDbJobStore jobStore)
      throws SchedulerConfigException {
    return StandardOrientDbConnector.builder().withUri(jobStore.getOrientDbUri())
        .withCredentials(jobStore.getUsername(), jobStore.getPassword())
        .withDatabaseName(jobStore.getDbName())
        .withCollectionPrefix(jobStore.getCollectionPrefix())
        /*
         * .withAuthDatabaseName(jobStore.authDbName)
         * .withMaxConnectionsPerHost(jobStore.
         * mongoOptionMaxConnectionsPerHost) .withConnectTimeoutMillis(jobStore.
         * mongoOptionConnectTimeoutMillis) .withSocketTimeoutMillis(jobStore.
         * mongoOptionSocketTimeoutMillis)
         * .withSocketKeepAlive(jobStore.mongoOptionSocketKeepAlive)
         * .withThreadsAllowedToBlockForConnectionMultiplier( jobStore.
         * mongoOptionThreadsAllowedToBlockForConnectionMultiplier)
         * .withSSL(jobStore.mongoOptionEnableSSL,
         * jobStore.mongoOptionSslInvalidHostNameAllowed)
         * .withWriteTimeout(jobStore. mongoOptionWriteConcernTimeoutMillis)
         */
        .build();
  }

  private StandardPausedJobGroupsDao createPausedJobGroupsDao(OrientDbJobStore jobStore) {
    return new StandardPausedJobGroupsDao(this, queryHelper, jobStore.getCollectionPrefix());
  }

  private StandardPausedTriggerGroupsDao createPausedTriggerGroupsDao(OrientDbJobStore jobStore) {
    return new StandardPausedTriggerGroupsDao(this, queryHelper, jobStore.getCollectionPrefix());
  }

  private StandardSchedulerDao createSchedulerDao(OrientDbJobStore jobStore) {
    return new StandardSchedulerDao(this, jobStore.getSchedulerName(), jobStore.getInstanceId(),
        Clock.SYSTEM_CLOCK, jobStore.getCollectionPrefix());
  }

  private TriggerAndJobPersister createTriggerAndJobPersister() {
    return new TriggerAndJobPersister(triggerDao, jobDao, triggerConverter);
  }

  private StandardTriggerDao createTriggerDao(OrientDbJobStore jobStore) {
    return new StandardTriggerDao(this, queryHelper, triggerConverter, jobStore.getCollectionPrefix());
  }

  private TriggerRunner createTriggerRunner(MisfireHandler misfireHandler) {
    return new TriggerRunner(persister, triggerDao, jobDao, calendarDao, misfireHandler,
        triggerConverter, triggerRecoverer, clock);
  }

  private TriggerStateManager createTriggerStateManager() {
    return new TriggerStateManager(triggerDao, jobDao, pausedJobGroupsDao, pausedTriggerGroupsDao);
  }
}
