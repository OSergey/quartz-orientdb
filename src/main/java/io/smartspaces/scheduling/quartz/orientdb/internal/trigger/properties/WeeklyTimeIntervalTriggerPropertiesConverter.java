/*
 * Copyright (c) 2018 Serhii Ovsiuk
 * Forked from code Copyright (C) 2016 Keith M. Hughes
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

package io.smartspaces.scheduling.quartz.orientdb.internal.trigger.properties;

import java.text.ParseException;
import java.util.TimeZone;

import com.orientechnologies.orient.core.record.impl.ODocument;
import io.smartspaces.scheduling.quartz.orientdb.internal.Constants;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.TriggerPropertiesConverter;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.WeeklyTrigger;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.impl.WeeklyTriggerImpl;
import org.quartz.CronExpression;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

public class WeeklyTimeIntervalTriggerPropertiesConverter extends TriggerPropertiesConverter {


  @Override
  protected boolean canHandle(OperableTrigger trigger) {
    return ((trigger instanceof WeeklyTriggerImpl)
        && !((WeeklyTriggerImpl) trigger).hasAdditionalProperties());
  }

  @Override
  public ODocument injectExtraPropertiesForInsert(OperableTrigger trigger, ODocument original) {
    WeeklyTrigger t = (WeeklyTrigger) trigger;

    ODocument newDoc = new ODocument();
    original.copyTo(newDoc);

    newDoc.field(Constants.TRIGGER_CRON_EXPRESSION, t.getCronExpression()).field(Constants.TRIGGER_TIMEZONE,
        t.getTimeZone().getID());
    newDoc.field(Constants.TRIGGER_INTERVAL_IN_WEEK, ((WeeklyTriggerImpl)t).getIntervalInWeek());
    return newDoc;
  }

  @Override
  public void setExtraPropertiesAfterInstantiation(OperableTrigger trigger, ODocument stored) {
    WeeklyTriggerImpl t = (WeeklyTriggerImpl) trigger;
    t.setScheduleTrigger(new CronTriggerImpl());
    t.setIntervalInWeek(stored.field(Constants.TRIGGER_INTERVAL_IN_WEEK) != null ? stored.field(Constants.TRIGGER_INTERVAL_IN_WEEK) : 0);
    String expression = stored.field(Constants.TRIGGER_CRON_EXPRESSION);
    if (expression != null) {
      try {
        t.setCronExpression(new CronExpression(expression));
      } catch (ParseException e) {
        // no good handling strategy and
        // checked exceptions route sucks just as much.
      }
    }
    String tz = stored.field(Constants.TRIGGER_TIMEZONE);
    if (tz != null) {
      t.setTimeZone(TimeZone.getTimeZone(tz));
    }
  }
}
