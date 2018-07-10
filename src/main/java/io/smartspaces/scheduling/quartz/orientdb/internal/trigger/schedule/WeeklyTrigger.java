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

package io.smartspaces.scheduling.quartz.orientdb.internal.trigger.schedule;

import java.util.TimeZone;

import org.quartz.Trigger;
import org.quartz.spi.OperableTrigger;

/**
 * @author Serhii Ovsiuk
 */
public interface WeeklyTrigger extends Trigger {

	void setScheduleTrigger(OperableTrigger trigger);

	void setIntervalInWeek(int intervalInWeek);

	String getCronExpression();

	TimeZone getTimeZone();
}