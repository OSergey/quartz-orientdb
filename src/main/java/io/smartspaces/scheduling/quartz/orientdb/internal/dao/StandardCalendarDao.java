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

import java.util.List;

import org.quartz.Calendar;
import org.quartz.JobPersistenceException;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.StandardOrientDbStoreAssembler;
import io.smartspaces.scheduling.quartz.orientdb.internal.util.SerialUtils;

public class StandardCalendarDao {


  private final StandardOrientDbStoreAssembler storeAssembler;
  private String iClassName = "Calendar";

  public StandardCalendarDao(StandardOrientDbStoreAssembler storeAssembler) {
    this.storeAssembler = storeAssembler;
  }

  public StandardCalendarDao(StandardOrientDbStoreAssembler storeAssembler, String collectionPrefix) {
    this(storeAssembler);
    this.iClassName = new StringBuilder(collectionPrefix).append(this.iClassName).toString();
  }

  public void startup() {
    // Nothing to do
  }
  
  public void removeAll() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    for (ODocument calendar : database.browseClass(this.iClassName)) {
      calendar.delete();
    }
  }

  public int getCount() {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();
    return (int) database.countClass(this.iClassName);
  }

  public boolean remove(String calName) {
    List<ODocument> result = getCalendarsByName(calName);

    if (!result.isEmpty()) {
      result.get(0).delete();
      return true;
    }
    return false;
  }

  public Calendar getCalendar(String calName) throws JobPersistenceException {
    if (calName != null) {
      List<ODocument> result = getCalendarsByName(calName);
      if (!result.isEmpty()) {
        ORecordBytes serializedCalendar = result.get(0).field(Constants.CALENDAR_SERIALIZED_OBJECT);
        return SerialUtils.deserialize(serializedCalendar.toStream(), Calendar.class);
      }
    }
    return null;
  }

  public void store(String name, Calendar calendar) throws JobPersistenceException {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    ORecordBytes serializedCalendar = new ORecordBytes(SerialUtils.serialize(calendar));
    List<ODocument> result = getCalendarsByName(name);
    ODocument doc;
    if (!result.isEmpty()) {
      doc = result.get(0);
    } else {
      doc = new ODocument(this.iClassName);
    }
    doc = doc.field(Constants.CALENDAR_NAME, name).field(Constants.CALENDAR_SERIALIZED_OBJECT, serializedCalendar);
    doc.save();
  }
  

  private List<ODocument> getCalendarsByName(String name) {
    ODatabaseDocumentTx database = storeAssembler.getOrientDbConnector().getConnection();

    // TODO(keith): class and field names should come from external constants
    // Also create query ahead of time when DAO starts.
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>(new StringBuilder("select from ").append(this.iClassName).append(" where name=?").toString());
    List<ODocument> result = database.command(query).execute(name);
    return result;
  }
}
