/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

public class AbstractConfigTest {

    @Test
    public void testConfiguredInstances() {
        testValidInputs("");
        testValidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter");
        testValidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter, org.apache.kafka.common.metrics.FakeMetricsReporter");
        testInvalidInputs(",");
        testInvalidInputs("org.apache.kafka.clients.producer.unknown-metrics-reporter");
        testInvalidInputs("test1,test2");
        testInvalidInputs("org.apache.kafka.common.metrics.FakeMetricsReporter,");
    }

    @Test
    public void testOriginalsWithPrefix() {
        Properties props = new Properties();
        props.put("foo.bar", "abc");
        props.put("setting", "def");
        TestConfig config = new TestConfig(props);
        Map<String, Object> originalsWithPrefix = config.originalsWithPrefix("foo.");

        assertTrue(config.unused().contains("foo.bar"));
        originalsWithPrefix.get("bar");
        assertFalse(config.unused().contains("foo.bar"));

        Map<String, Object> expected = new HashMap<>();
        expected.put("bar", "abc");
        assertEquals(expected, originalsWithPrefix);
    }

    @Test
    public void testUnused() {
        Properties props = new Properties();
        String configValue = "org.apache.kafka.common.config.AbstractConfigTest$ConfiguredFakeMetricsReporter";
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        props.put(FakeMetricsReporterConfig.EXTRA_CONFIG, "my_value");
        TestConfig config = new TestConfig(props);

        assertTrue("metric.extra_config should be marked unused before getConfiguredInstances is called",
            config.unused().contains(FakeMetricsReporterConfig.EXTRA_CONFIG));

        config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        assertTrue("All defined configurations should be marked as used", config.unused().isEmpty());
    }

    private void testValidInputs(String configValue) {
        Properties props = new Properties();
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        TestConfig config = new TestConfig(props);
        try {
            config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
        } catch (ConfigException e) {
            fail("No exceptions are expected here, valid props are :" + props);
        }
    }

    private void testInvalidInputs(String configValue) {
        Properties props = new Properties();
        props.put(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, configValue);
        TestConfig config = new TestConfig(props);
        try {
            config.getConfiguredInstances(TestConfig.METRIC_REPORTER_CLASSES_CONFIG, MetricsReporter.class);
            fail("Expected a config exception due to invalid props :" + props);
        } catch (KafkaException e) {
            // this is good
        }
    }

    private static class TestConfig extends AbstractConfig {

        private static final ConfigDef CONFIG;

        public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
        private static final String METRIC_REPORTER_CLASSES_DOC = "A list of classes to use as metrics reporters.";

        static {
            CONFIG = new ConfigDef().define(METRIC_REPORTER_CLASSES_CONFIG,
                                            Type.LIST,
                                            "",
                                            Importance.LOW,
                                            METRIC_REPORTER_CLASSES_DOC);
        }

        public TestConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

    public static class FakeMetricsReporterConfig extends AbstractConfig {
        private static final ConfigDef CONFIG;

        public static final String EXTRA_CONFIG = "metric.extra_config";
        private static final String EXTRA_CONFIG_DOC = "An extraneous configuration string.";

        static {
            CONFIG = new ConfigDef().define(
                EXTRA_CONFIG, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, EXTRA_CONFIG_DOC);
        }

        public FakeMetricsReporterConfig(Map<?, ?> props) {
            super(CONFIG, props);
        }
    }

}
