/*
 * Copyright (c) 2018, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.util;

import java.util.HashMap;

public class GettableHashMap<K,V> extends HashMap<K,V> implements Gettable<K,V> {

}
