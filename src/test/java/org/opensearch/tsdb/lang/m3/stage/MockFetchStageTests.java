/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for MockFetchStage.
 */
public class MockFetchStageTests extends AbstractWireSerializingTestCase<MockFetchStage> {

    @Override
    protected MockFetchStage createTestInstance() {
        int numValues = randomIntBetween(1, 10);
        List<Double> values = randomList(numValues, numValues, () -> randomDouble());
        Map<String, String> tags = randomBoolean() ? Map.of("name", "test") : Map.of("name", "test", "dc", "dca1");
        return new MockFetchStage(values, tags);
    }

    @Override
    protected Writeable.Reader<MockFetchStage> instanceReader() {
        return MockFetchStage::readFrom;
    }

    // ========== Behavior Tests ==========

    public void testMockFetchStageBasicExecution() {
        List<Double> values = List.of(1.0, 2.0, 3.0);
        Map<String, String> tags = Map.of("name", "test_series", "region", "us-east");

        MockFetchStage stage = new MockFetchStage(values, tags);
        stage.setQueryContext(1000L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertNotNull(result);
        assertEquals(1, result.size());

        TimeSeries series = result.get(0);
        assertEquals(3, series.getSamples().size());

        // Check values
        assertEquals(1.0f, series.getSamples().getValue(0), 0.001f);
        assertEquals(2.0f, series.getSamples().getValue(1), 0.001f);
        assertEquals(3.0f, series.getSamples().getValue(2), 0.001f);

        // Check timestamps (startTime + i * step)
        assertEquals(1000L, series.getSamples().getTimestamp(0));
        assertEquals(1001L, series.getSamples().getTimestamp(1));
        assertEquals(1002L, series.getSamples().getTimestamp(2));

        // Check metadata
        assertEquals(1000L, series.getMinTimestamp());
        assertEquals(1002L, series.getMaxTimestamp());
        assertEquals(1L, series.getStep());

        // Check labels
        Labels expectedLabels = ByteLabels.fromMap(tags);
        assertEquals(expectedLabels, series.getLabels());
    }

    public void testMockFetchStageWithSingleValue() {
        List<Double> values = List.of(42.5);
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "constant"));
        stage.setQueryContext(0L, 1L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(1, series.getSamples().size());
        assertEquals(42.5f, series.getSamples().getValue(0), 0.001f);
        assertEquals(0L, series.getSamples().getTimestamp(0));
        assertEquals(0L, series.getMinTimestamp());
        assertEquals(0L, series.getMaxTimestamp());
    }

    public void testMockFetchStageWithLargerStep() {
        List<Double> values = List.of(1.0, 2.0, 3.0, 4.0);
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "test"));
        stage.setQueryContext(0L, 10L);

        List<TimeSeries> result = stage.process(null);

        assertEquals(1, result.size());
        TimeSeries series = result.get(0);
        assertEquals(4, series.getSamples().size());

        // Check timestamps with larger step
        assertEquals(0L, series.getSamples().getTimestamp(0));
        assertEquals(10L, series.getSamples().getTimestamp(1));
        assertEquals(20L, series.getSamples().getTimestamp(2));
        assertEquals(30L, series.getSamples().getTimestamp(3));
    }

    public void testMockFetchStageProcessWithNonNullInput() {
        List<Double> values = List.of(10.0, 20.0);
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "test"));
        stage.setQueryContext(0L, 1000L);

        // Input should be ignored
        List<TimeSeries> dummyInput = List.of();
        List<TimeSeries> result = stage.process(dummyInput);

        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
    }

    // ========== fromArgs() Tests ==========

    public void testMockFetchStageFromArgsWithVariousTypes() {
        // Test with Double list
        Map<String, Object> args1 = Map.of("values", List.of(1.0, 2.0, 3.0), "tags", Map.of("name", "test"));
        MockFetchStage stage1 = MockFetchStage.fromArgs(args1);
        assertEquals(List.of(1.0, 2.0, 3.0), stage1.getValues());

        // Test with Integer list
        Map<String, Object> args2 = Map.of("values", List.of(1, 2, 3));
        MockFetchStage stage2 = MockFetchStage.fromArgs(args2);
        assertEquals(List.of(1.0, 2.0, 3.0), stage2.getValues());

        // Test with String list
        Map<String, Object> args3 = Map.of("values", List.of("1.5", "2.5", "3.5"));
        MockFetchStage stage3 = MockFetchStage.fromArgs(args3);
        assertEquals(List.of(1.5, 2.5, 3.5), stage3.getValues());

        // Test with single Number
        Map<String, Object> args4 = Map.of("values", 42.0);
        MockFetchStage stage4 = MockFetchStage.fromArgs(args4);
        assertEquals(List.of(42.0), stage4.getValues());

        // Test with single String
        Map<String, Object> args5 = Map.of("values", "100.5");
        MockFetchStage stage5 = MockFetchStage.fromArgs(args5);
        assertEquals(List.of(100.5), stage5.getValues());
    }

    public void testMockFetchStageFromArgsDefaultTag() {
        Map<String, Object> args = Map.of("values", List.of(1.0, 2.0));
        MockFetchStage stage = MockFetchStage.fromArgs(args);

        // Should have default "name: mockFetch" tag when no tags provided
        assertEquals(Map.of("name", "mockFetch"), stage.getTags());
    }

    public void testMockFetchStageFromArgsInvalidTypes() {
        // Invalid string value
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of("values", "invalid")));

        // Invalid value type in list
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of("values", List.of(1.0, new Object()))));

        // Invalid values argument type
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of("values", new Object())));

        // Missing values
        assertThrows(IllegalArgumentException.class, () -> MockFetchStage.fromArgs(Map.of()));
    }

    // ========== Validation Tests ==========

    public void testMockFetchStageCreationValidation() {
        // Null values
        assertThrows(IllegalArgumentException.class, () -> new MockFetchStage(null, Map.of()));

        // Empty values
        assertThrows(IllegalArgumentException.class, () -> new MockFetchStage(List.of(), Map.of()));

        // Null tags should be handled gracefully
        MockFetchStage stage = new MockFetchStage(List.of(1.0, 2.0), null);
        assertTrue(stage.getTags().isEmpty());
    }

    public void testMockFetchStageThrowsIfProcessCalledWithoutQueryContext() {
        List<Double> values = List.of(1.0, 2.0, 3.0);
        MockFetchStage stage = new MockFetchStage(values, Map.of("name", "test"));

        // Should throw if process() is called without setQueryContext()
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> stage.process(null));
        assertTrue(exception.getMessage().contains("setQueryContext"));
    }

    // ========== Metadata Tests ==========

    public void testMockFetchStageGetName() {
        MockFetchStage stage = new MockFetchStage(List.of(1.0), Map.of());
        assertEquals("mockFetch", stage.getName());
    }

    public void testMockFetchStageIsCoordinatorOnly() {
        MockFetchStage stage = new MockFetchStage(List.of(1.0, 2.0, 3.0), Map.of());
        assertTrue(stage.isCoordinatorOnly());
    }
}
