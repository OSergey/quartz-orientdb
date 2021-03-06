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

package io.smartspaces.scheduling.quartz.orientdb.internal.dao;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.impl.matchers.GroupMatcher;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.JobConverter;
import io.smartspaces.scheduling.quartz.orientdb.internal.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.Keys;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.ODocumentHelper;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.QueryHelper;

/**
 * The Data Access Object for the Job class.
 */
public class StandardJobDao {

  private final StandardOrientDbStoreAssembler storeAssembler;
  private final QueryHelper queryHelper;
  private final JobConverter jobConverter;
  private String iClassName = "Job";

  public StandardJobDao(StandardOrientDbStoreAssembler storeAssembler, QueryHelper queryHelper,
      JobConverter jobConverter) {
    this.storeAssembler = storeAssembler;
    this.queryHelper = queryHelper;
    this.jobConverter = jobConverter;
  }

  public StandardJobDao(StandardOrientDbStoreAssembler storeAssembler, QueryHelper queryHelper,
                        JobConverter jobConverter, String collectionPrefix) {
    this(storeAssembler, queryHelper, jobConverter);
    this.iClassName = new StringBuilder(collectionPrefix).append(this.iClassName).toString();
  }

  public void startup() {
    // Nothing to do
  }

  public void removeAll() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    for (ODocument job : database.browseClass(this.iClassName)) {
      job.delete();
    }
  }

  public boolean exists(JobKey jobKey) {
    return !getJobsByKey(jobKey).isEmpty();
  }

  public ODocument getById(ORID id) {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    return database.getRecord(id);
  }

  public ODocument getJob(JobKey jobKey) {
    List<ODocument> result = getJobsByKey(jobKey);

    if (result.isEmpty()) {
      return null;
    } else {
      return result.get(0);
    }
  }

  private List<ODocument> getJobsByKey(JobKey jobKey) {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>(new StringBuilder("select from ")
                    .append(this.iClassName)
                    .append(" where keyGroup=? and keyName=?").toString());
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> result = database.command(query).execute(jobKey.getGroup(), jobKey.getName());

    return result;
  }

  public int getCount() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    return (int) database.countClass(this.iClassName);
  }

  public List<String> getGroupNames() {
    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>(new StringBuilder("select DISTINCT(keyGroup) from ").append(this.iClassName).toString());

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    List<ODocument> dbResult = database.command(query).execute();
    return dbResult.stream().map(res -> res.field("DISTINCT").toString()).collect(Collectors.toList());
  }

  public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) {
    Set<JobKey> keys = new HashSet<>();
    for (ODocument doc : findMatching(matcher)) {
      keys.add(Keys.toJobKey(doc));
    }

    return keys;
  }

  public Set<String> groupsOfMatching(GroupMatcher<JobKey> matcher) {
    String groupMatcherClause = queryHelper.matchingKeysConditionFor(matcher);
    OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>(new StringBuilder("select DISTINCT(keyGroup) from ")
                    .append(this.iClassName)
                    .append(" where ").append(groupMatcherClause)
                    .toString());

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    List<String> groups = database.query(query);

    return new HashSet<String>(groups);

  }

  public void remove(ODocument job) {
    job.delete();
  }

  public boolean requestsRecovery(JobKey jobKey) {
    // TODO check if it's the same as getJobDataMap?
    ODocument jobDoc = getJob(jobKey);
    return ODocumentHelper.getBooleanField(jobDoc, Constants.JOB_REQUESTS_RECOVERY, false);
  }

  public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
    ODocument doc = getJob(jobKey);
    if (doc == null) {
      // Return null if job does not exist, per interface
      return null;
    }
    return jobConverter.toJobDetail(doc);
  }

  public ORID storeJob(JobDetail newJob, boolean replaceExisting)
      throws ObjectAlreadyExistsException {
    JobKey key = newJob.getKey();

    ODocument newJobDoc = jobConverter.toDocument(newJob, key);
 
    ODocument oldJobDoc = getJob(key);

    ORID jobId = null;
    if (oldJobDoc != null && replaceExisting) {
      oldJobDoc.merge(newJobDoc, true, true);
      oldJobDoc.save();
      jobId = oldJobDoc.getIdentity();
    } else if (oldJobDoc == null) {
      // try {
      newJobDoc.save();
      jobId = newJobDoc.getIdentity();
      // } catch (Exception e) {
      // Fine, find it and get its id.
      // object = getJob(keyDbo);
      // objectId = object.getObjectId("_id");
      // }
    } else {
      jobId = oldJobDoc.getIdentity();
    }

    return jobId;
  }

  private List<ODocument> findMatching(GroupMatcher<JobKey> matcher) {
    String groupMatcherClause = queryHelper.matchingKeysConditionFor(matcher);
    OSQLSynchQuery<ODocument> query =
            new OSQLSynchQuery<ODocument>(new StringBuilder("select from ")
                    .append(this.iClassName)
                    .append(" where ")
                    .append(groupMatcherClause).toString());

    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    List<ODocument> documents = database.query(query);

    return documents;
  }
}
