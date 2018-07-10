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

package io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.impl;

import java.text.ParseException;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule.WeeklyTrigger;
import org.quartz.Calendar;
import org.quartz.CronExpression;
import org.quartz.ScheduleBuilder;
import org.quartz.impl.triggers.AbstractTrigger;
import org.quartz.impl.triggers.CoreTrigger;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.OperableTrigger;

/**
 * @author Serhii Ovsiuk
 */
public class WeeklyTriggerImpl extends AbstractTrigger<WeeklyTrigger> implements WeeklyTrigger, CoreTrigger {
	private OperableTrigger trigger;
	private int intervalInWeek;
	private ScheduleBuilder<WeeklyTrigger> weeklyBuilder;

	@Override
	public void triggered(Calendar calendar) {
		this.trigger.triggered(calendar);
	}

	@Override
	public Date computeFirstFireTime(Calendar calendar) {
		return trigger.computeFirstFireTime(calendar);
	}

	@Override
	public boolean mayFireAgain() {
		return trigger.mayFireAgain();
	}

	@Override
	public Date getStartTime() {
		return trigger.getStartTime();
	}

	@Override
	public void setStartTime(Date date) {
		this.trigger.setStartTime(date);
	}

	@Override
	public void setEndTime(Date date) {
		this.trigger.setEndTime(date);
	}

	@Override
	public Date getEndTime() {
		return this.trigger.getEndTime();
	}

	@Override
	public Date getNextFireTime() {
		//Get the fire time of the cron trigger after afterTime
		Date nextFireTime = trigger.getNextFireTime();
		if (Optional.ofNullable(nextFireTime).isPresent() && this.intervalInWeek > 1) {
			Date previousFireTime = trigger.getStartTime();
			if (Optional.ofNullable(trigger.getPreviousFireTime()).isPresent()) {
				previousFireTime = trigger.getPreviousFireTime();
			}
			if (!isSameWeek(nextFireTime, previousFireTime)) {
				java.util.Calendar c = java.util.Calendar.getInstance();
				c.setTime(nextFireTime);
				c.add(java.util.Calendar.WEEK_OF_MONTH, this.intervalInWeek - 1);
				nextFireTime = c.getTime();
				if(Optional.ofNullable(trigger.getEndTime()).isPresent() && nextFireTime.after(trigger.getEndTime())){
					nextFireTime = null;
				}
			}
		}
		return nextFireTime;
	}

	private boolean isSameWeek(Date firstDate, Date secondDate) {
		java.util.Calendar calFirstDate = java.util.Calendar.getInstance();
		calFirstDate.setTime(firstDate);
		int weekFirstDate = calFirstDate.get(java.util.Calendar.WEEK_OF_YEAR);
		java.util.Calendar calSecondDate = java.util.Calendar.getInstance();
		calSecondDate.setTime(secondDate);
		int weekSecondDate = calSecondDate.get(java.util.Calendar.WEEK_OF_YEAR);
		return weekFirstDate == weekSecondDate;
	}

	@Override
	public Date getPreviousFireTime() {
		return this.trigger.getPreviousFireTime();
	}

	@Override
	public Date getFireTimeAfter(Date afterTime) {
		return trigger.getFireTimeAfter(afterTime);
	}

	@Override
	public Date getFinalFireTime() {
		return this.trigger.getFinalFireTime();
	}

	@Override
	protected boolean validateMisfireInstruction(int misfireInstruction) {
		return misfireInstruction >= -1 && misfireInstruction <= 2;
	}

	@Override
	public void updateAfterMisfire(Calendar calendar) {
		this.trigger.updateAfterMisfire(calendar);
	}

	@Override
	public void updateWithNewCalendar(Calendar calendar, long l) {
		this.trigger.updateWithNewCalendar(calendar, l);
	}

	@Override
	public void setNextFireTime(Date date) {
		this.trigger.setNextFireTime(date);
	}

	@Override
	public void setPreviousFireTime(Date date) {
		this.trigger.setPreviousFireTime(date);
	}

	@Override
	public ScheduleBuilder<WeeklyTrigger> getScheduleBuilder() {
		return this.weeklyBuilder;
	}

	@Override
	public void setScheduleTrigger(OperableTrigger trigger) {
		this.trigger = trigger;
	}

	@Override
	public void setIntervalInWeek(int intervalInWeek) {
		this.intervalInWeek = intervalInWeek;
	}

	@Override
	public String getCronExpression() {
		return ((CronTriggerImpl)this.trigger).getCronExpression();
	}

	@Override
	public TimeZone getTimeZone() {
		return ((CronTriggerImpl)this.trigger).getTimeZone();
	}

	@Override
	public boolean hasAdditionalProperties() {
		return false;
	}

	public void setCronExpression(CronExpression cronExpression) throws ParseException {
		if (this.trigger instanceof CronTriggerImpl) {
			((CronTriggerImpl) this.trigger).setCronExpression(cronExpression);
		}
	}

	public void setTimeZone(TimeZone timeZone) {
		if (this.trigger instanceof CronTriggerImpl) {
			((CronTriggerImpl) this.trigger).setTimeZone(timeZone);
		}
	}

	public int getIntervalInWeek() {
		return intervalInWeek;
	}

	public void setMisfireInstruction(int i){
		this.trigger.setMisfireInstruction(i);
	}

	@Override
	public int getMisfireInstruction() {
		return this.trigger.getMisfireInstruction();
	}

	public void setWeeklyBuilder(ScheduleBuilder<WeeklyTrigger> weeklyBuilder) {
		this.weeklyBuilder = weeklyBuilder;
	}
}
