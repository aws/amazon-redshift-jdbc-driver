/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package com.amazon.redshift.jdbc;

/**
 * Represents {@link RedshiftStatementImpl#cancel()} state.
 */
enum StatementCancelState {
    IDLE,
    IN_QUERY,
    CANCELING,
    CANCELLED
}
