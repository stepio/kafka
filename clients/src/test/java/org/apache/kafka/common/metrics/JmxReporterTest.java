/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.utils.Time;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() throws Exception {
        final JmxReporter reporter = new JmxReporter();
        MetricName name;
        try (Metrics metrics = new Metrics()) {
            assertEquals(0, reporter.size());
            metrics.addReporter(reporter);
            assertEquals(1, reporter.size());
            Sensor sensor = metrics.sensor("kafka.requests");
            name = metrics.metricName("pack.bean1.avg", "grp1");
            sensor.add(name, new Avg());
            assertEquals(2, reporter.size());
            assertEquals(1, reporter.getMBean(name).size());
            name = metrics.metricName("pack.bean2.total", "grp2");
            sensor.add(name, new Total());
            assertEquals(3, reporter.size());
            assertEquals(1, reporter.getMBean(name).size());
            Sensor sensor2 = metrics.sensor("kafka.blah");
            name = metrics.metricName("pack.bean1.some", "grp1");
            sensor2.add(name, new Total());
            assertEquals(3, reporter.size());
            assertEquals(2, reporter.getMBean(name).size());
            name = metrics.metricName("pack.bean2.some", "grp1");
            sensor2.add(name, new Total());
            assertEquals(3, reporter.size());
            assertEquals(3, reporter.getMBean(name).size());
            metrics.removeSensor(sensor2.name());
            assertEquals(3, reporter.size());
            assertEquals(1, reporter.getMBean(name).size());
        }
        assertEquals(0, reporter.size());
        assertEquals(0, reporter.getMBean(name).size());
    }

    @Test
    public void testJmxRegistrationWithMock() throws Exception {
        final JmxReporter reporter = new JmxReporter();
        MockKafkaMetric metric;
        assertEquals(0, reporter.size());
        metric = MockKafkaMetric.of("pack.bean1.avg", "grp1", new Avg());
        reporter.metricChange(metric);
        assertEquals(1, reporter.size());
        assertEquals(1, reporter.getMBean(metric.metricName()).size());
        MockKafkaMetric metricAnotherGroup = MockKafkaMetric.of("pack.bean2.total", "grp2", new Total());
        reporter.metricChange(metricAnotherGroup);
        assertEquals(2, reporter.size());
        assertEquals(1, reporter.getMBean(metricAnotherGroup.metricName()).size());
        List<KafkaMetric> list = new ArrayList<KafkaMetric>();
        metric = MockKafkaMetric.of("pack.bean1.some", "grp1", new Total());
        list.add(metric);
        metric = MockKafkaMetric.of("pack.bean2.some", "grp1", new Total());
        list.add(metric);
        reporter.init(list);
        assertEquals(2, reporter.size());
        assertEquals(3, reporter.getMBean(metric.metricName()).size());
        reporter.metricRemoval(metric);
        assertEquals(2, reporter.size());
        assertEquals(2, reporter.getMBean(metric.metricName()).size());
        reporter.metricRemoval(metricAnotherGroup);
        assertEquals(1, reporter.size());
        assertEquals(0, reporter.getMBean(metricAnotherGroup.metricName()).size());
        reporter.close();
        assertEquals(0, reporter.size());
        assertEquals(0, reporter.getMBean(metric.metricName()).size());
    }

    @Test
    public void testGetMBeanNameWithNull() {
        final JmxReporter reporter = new JmxReporter();
        try {
            reporter.getMBeanName(null);
        } catch (RuntimeException e) {
            assertThat(e, CoreMatchers.instanceOf(NullPointerException.class));
            assertThat(e.getMessage(), CoreMatchers.is("Parameter 'metricName' is mandatory"));
        }
    }

    @Test
    public void testGetMBeanWithNull() {
        final JmxReporter reporter = new JmxReporter();
        try {
            reporter.getMBean(null);
        } catch (RuntimeException e) {
            assertThat(e, CoreMatchers.instanceOf(NullPointerException.class));
            assertThat(e.getMessage(), CoreMatchers.is("Parameter 'metricName' is mandatory"));
        }
    }

    private static class MockKafkaMetric extends KafkaMetric {

        private MockKafkaMetric(Object lock, MetricName metricName, Measurable measurable, MetricConfig config, Time time) {
            super(lock, metricName, measurable, config, time);
        }

        private MockKafkaMetric(MetricName metricName, Measurable measurable) {
            this(new Object(), metricName, measurable, null, Time.SYSTEM);
        }

        private static MockKafkaMetric of(String name, String group, Measurable measurable) {
            final MetricName metricName = new MetricName(name, group, "", Collections.<String, String>emptyMap());
            return new MockKafkaMetric(metricName, measurable);
        }
    }
}
