/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import org.opensearch.test.OpenSearchTestCase;

public class SemanticVersionComparatorTests extends OpenSearchTestCase {

    public void testVersionNormalizationSimple() {
        assertEquals("v1.0.0", SemanticVersionComparator.normalizeVersion("1"));
        assertEquals("v2.0.0", SemanticVersionComparator.normalizeVersion("2.0"));
        assertEquals("v1.2.3", SemanticVersionComparator.normalizeVersion("1.2.3"));
        assertEquals("v1.2.3", SemanticVersionComparator.normalizeVersion("v1.2.3"));
    }

    public void testVersionNormalizationWithIncompleteVersions() {
        assertEquals("v30.500.0", SemanticVersionComparator.normalizeVersion("30.500"));
        assertEquals("v29.5.0", SemanticVersionComparator.normalizeVersion("29.5"));
        assertEquals("v30.500.100", SemanticVersionComparator.normalizeVersion("30.500.100"));
        assertEquals("v30.600.0", SemanticVersionComparator.normalizeVersion("30.600"));
    }

    public void testVersionNormalizationEdgeCases() {
        assertEquals("v0.0.0", SemanticVersionComparator.normalizeVersion("0"));
        assertEquals("v0.1.0", SemanticVersionComparator.normalizeVersion("0.1"));
        assertEquals("v0.0.1", SemanticVersionComparator.normalizeVersion("0.0.1"));
    }

    public void testVersionNormalizationInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion(null));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion(""));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("   "));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("abc"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.2.3.4"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.-1"));
    }

    public void testSemanticVersionDetection() {
        // Valid semantic versions
        assertTrue(SemanticVersionComparator.isSemanticVersion("1"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.0"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.2.3"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("v1.2.3"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("30.500.100"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("29.5"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("0.1.10"));

        // Invalid semantic versions
        assertFalse(SemanticVersionComparator.isSemanticVersion(null));
        assertFalse(SemanticVersionComparator.isSemanticVersion(""));
        assertFalse(SemanticVersionComparator.isSemanticVersion("abc"));
        assertFalse(SemanticVersionComparator.isSemanticVersion("1.2.3.4"));
        assertFalse(SemanticVersionComparator.isSemanticVersion("not-a-version"));
    }

    public void testSemanticVersionComparison() {
        // Major version differences
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0", "2.0.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("2.0.0", "1.0.0") > 0);

        // Minor version differences
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.1.0", "1.2.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.2.0", "1.1.0") > 0);

        // Patch version differences
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.1", "1.0.2") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.2", "1.0.1") > 0);

        // Equal versions
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.2.3", "1.2.3"));
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1", "1.0.0"));
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.2", "1.2.0"));
    }

    public void testSemanticVersionComparisonFromExamples() {
        // From the documentation examples
        assertTrue(SemanticVersionComparator.compareSemanticVersions("29.5", "30.500.100") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("30.5", "30.500.100") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("30.500.1", "30.500.100") < 0);
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("30.500.100", "30.500.100"));
        assertTrue(SemanticVersionComparator.compareSemanticVersions("30.600", "30.500.100") > 0);
    }

    public void testSemanticVersionComparisonInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.compareSemanticVersions("1.2.3", "invalid"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.compareSemanticVersions("invalid", "1.2.3"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.compareSemanticVersions(null, "1.2.3"));
    }

    public void testSemanticVersionComparisonAllOperatorsViaCompare() {
        // Test compareSemanticVersions result can be used with all comparison operators
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0", "2.0.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("2.0.0", "1.0.0") > 0);
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.0.0", "1.0.0"));
    }
}
