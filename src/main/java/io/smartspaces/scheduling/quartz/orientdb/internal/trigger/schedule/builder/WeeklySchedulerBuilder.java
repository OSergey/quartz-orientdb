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

package io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.builder;


import java.util.Date;
import java.util.Optional;

import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.WeeklyTrigger;
import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.impl.WeeklyTriggerImpl;
import org.quartz.ScheduleBuilder;
import org.quartz.spi.MutableTrigger;
import org.quartz.spi.OperableTrigger;

/**
 * @author Serhii Ovsiuk
 */
public class WeeklySchedulerBuilder extends ScheduleBuilder<WeeklyTrigger> {
    private int intervalInWeek;
    private OperableTrigger trigger;
    private Date endAt;
    private String calendarName;

    public MutableTrigger build() {
        WeeklyTriggerImpl weeklyTrigger = new WeeklyTriggerImpl();
        weeklyTrigger.setIntervalInWeek(this.intervalInWeek);
        if (Optional.ofNullable(trigger).isPresent()) {
            weeklyTrigger.setScheduleTrigger(trigger);
            if (Optional.ofNullable(trigger.getKey()).isPresent()) {
                weeklyTrigger.setKey(trigger.getKey());
            }
        }
        weeklyTrigger.setWeeklyBuilder(this);
        if (Optional.ofNullable(endAt).isPresent()) {
            weeklyTrigger.setEndTime(endAt);
        }
        weeklyTrigger.setCalendarName(this.calendarName);
        return weeklyTrigger;
    }

    public WeeklySchedulerBuilder() {
    }

    public WeeklySchedulerBuilder(OperableTrigger trigger, int intervalInWeek) {
        this.trigger = trigger;
        this.intervalInWeek = intervalInWeek;
    }

    public WeeklySchedulerBuilder withIntervalInWeek(int intervalInWeek) {
        this.intervalInWeek = intervalInWeek;
        return this;
    }

    public WeeklySchedulerBuilder withBasedTrigger(OperableTrigger trigger) {
        this.trigger = trigger;
        return this;
    }

    public WeeklySchedulerBuilder modifiedByCalendar(String calendarName) {
        this.calendarName = calendarName;
        return this;
    }

    public void endAt(Date endAt) {
        this.endAt = endAt;
    }
}
