package com.amazon.redshift.core.v3;

import com.amazon.redshift.core.Tuple;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;

/**
 * Utility class for memory-related calculations in Redshift JDBC driver.
 * Provides methods to estimate memory usage of result set data structures.
 */
public final class RedshiftMemoryUtils {

    /**
     * Scaling factor to account for JVM memory overhead and alignment.
     * The factor 1.2 adds a 20% safety margin to the calculated size.
     */
    static final double MEMORY_ESTIMATE_SCALING_FACTOR = 1.2;

    /**
     * Memory overhead for a node in a 64-bit JVM:
     * - 8 bytes: Object reference for Tuple
     * - 8 bytes: Reference for next pointer
     * - 16 bytes: Object header overhead
     */
    private static final int NODE_OVERHEAD_64BIT = 32;

    /**
     * Memory overhead for a node in a 32-bit JVM:
     * - 4 bytes: Object reference for Tuple
     * - 4 bytes: Reference for next pointer
     * - 8 bytes: Object header overhead
     */
    private static final int NODE_OVERHEAD_32BIT = 16;

    /**
     * JVM-specific memory overhead for node structures.
     */
    private static final int NODE_OVERHEAD = RedshiftConnectionImpl.IS_64_BIT_JVM
            ? NODE_OVERHEAD_64BIT
            : NODE_OVERHEAD_32BIT;

    private RedshiftMemoryUtils() {
        throw new AssertionError("Utility class should not be instantiated");
    }

    /**
     * Calculates the estimated memory size in bytes for a tuple node, including JVM overhead.
     * The calculation includes:
     * <ul>
     *   <li>The actual tuple data size</li>
     *   <li>JVM object overhead (32 bytes for 64-bit JVM, 16 bytes for 32-bit JVM)</li>
     *   <li>A scaling factor to account for additional JVM overhead</li>
     * </ul>
     *
     * Note: We don't worry about overflow in this method because:
     * - MEMORY_ESTIMATE_SCALING_FACTOR is 1.2, so even with scaling we're only increasing values by 20%
     * - NODE_OVERHEAD is a modest constant value
     * The main overflow risk comes from accumulating multiple tuples' sizes.
     *
     * @param row The tuple to calculate size for, can be null
     * @return Estimated size in bytes, or 0 if the tuple is null
     */
    public static int calculateNodeSize(Tuple row) {
        if (row == null) {
            return 0;
        }

        int rawSize = row.getTupleSize() + NODE_OVERHEAD;
        return (int) (rawSize * MEMORY_ESTIMATE_SCALING_FACTOR);
    }
}
