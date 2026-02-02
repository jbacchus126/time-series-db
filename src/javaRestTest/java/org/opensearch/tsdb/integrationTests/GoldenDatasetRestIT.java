/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.tsdb.framework.RestTimeSeriesTestFramework;

/**
 * REST integration tests for TSDB M3QL golden dataset.
 *
 *
 *
 */
public class GoldenDatasetRestIT extends RestTimeSeriesTestFramework {

    private static final String GOLDEN_DATASET_REST_IT = "test_cases/golden_dataset_rest_it.yaml";

    /**
     * Runs the complete golden dataset test suite via REST API.
     *
     * @throws Exception if any test fails
     */
    public void testGoldenDataset() throws Exception {
        initializeTest(GOLDEN_DATASET_REST_IT);
        runBasicTest();
    }
}
