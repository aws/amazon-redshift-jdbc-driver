/*
 * Copyright (c) 2014, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.hostchooser;

/**
 * Known state of a server.
 */
public enum HostStatus {
  ConnectFail,
  ConnectOK,
  Primary,
  Secondary
}
