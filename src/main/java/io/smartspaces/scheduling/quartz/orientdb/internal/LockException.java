/*
 * Copyright (C) 2016 Keith M. Hughes
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

import org.quartz.JobPersistenceException;

/**
 * An exception for the lock manager.
 * 
 * @author Keith M. Hughes
 */
public class LockException extends JobPersistenceException {

  /**
   * The serial version UUID for persistence.
   */
  private static final long serialVersionUID = -2867184089222307302L;

  /**
   * Construct a new lock exception.
   * 
   * @param msg
   *          the message for the exception
   */
  public LockException(String msg) {
    super(msg);
  }

  /**
   * Construct a new lock exception.
   * 
   * @param msg
   *          the message for the exception
   * @param cause
   *          the cause of the exception
   */
  public LockException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
