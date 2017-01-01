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
import org.assertj.core.api.ThrowableAssert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JmxReporterTest {

    @Test
    public void testJmxRegistration() {
        final JmxReporter reporter = new JmxReporter();
        MetricName name;
        try (Metrics metrics = new Metrics()) {
            assertThat(reporter.size())
                    .withFailMessage("List of 'mbeans' in 'JmxReporter' should be empty").isZero();
            metrics.addReporter(reporter);
            assertThat(reporter.size())
                    .withFailMessage("Size of 'mbeans' in 'JmxReporter' should be incremented by 1").isEqualTo(1);
            Sensor sensor = metrics.sensor("kafka.requests");
            name = metrics.metricName("pack.bean1.avg", "grp1");
            sensor.add(name, new Avg());
            assertThat(reporter.size())
                    .withFailMessage("Size of 'mbeans' in 'JmxReporter' should be incremented by 1").isEqualTo(2);
            assertThat(reporter.getMBean(name).size())
                    .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 1").isEqualTo(1);
            name = metrics.metricName("pack.bean2.total", "grp2");
            sensor.add(name, new Total());
            assertThat(reporter.size())
                    .withFailMessage("Size of 'mbeans' in 'JmxReporter' should be incremented by 1").isEqualTo(3);
            assertThat(reporter.getMBean(name).size())
                    .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 1").isEqualTo(1);
            Sensor sensor2 = metrics.sensor("kafka.blah");
            name = metrics.metricName("pack.bean1.some", "grp1");
            sensor2.add(name, new Total());
            assertThat(reporter.size())
                    .withFailMessage("Size of 'mbeans' in 'JmxReporter' should not change").isEqualTo(3);
            assertThat(reporter.getMBean(name).size())
                    .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 1").isEqualTo(2);
            name = metrics.metricName("pack.bean2.some", "grp1");
            sensor2.add(name, new Total());
            assertThat(reporter.size())
                    .withFailMessage("Size of 'mbeans' in 'JmxReporter' should not change").isEqualTo(3);
            assertThat(reporter.getMBean(name).size())
                    .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 1").isEqualTo(3);
            metrics.removeSensor(sensor2.name());
            assertThat(reporter.size())
                    .withFailMessage("Size of 'mbeans' in 'JmxReporter' should not change").isEqualTo(3);
            assertThat(reporter.getMBean(name).size())
                    .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be decremented by 2").isEqualTo(1);
        }
        assertThat(reporter.size())
                .withFailMessage("List of 'mbeans' in 'JmxReporter' should be empty").isZero();
        assertThat(reporter.getMBean(name).size())
                .withFailMessage("List of 'metrics' in 'KafkaMbean' should be empty").isZero();
    }

    @Test
    public void testJmxRegistrationWithMock() {
        final JmxReporter reporter = new JmxReporter();
        MockKafkaMetric metric;
        assertThat(reporter.size())
                .withFailMessage("List of 'mbeans' in 'JmxReporter' should be empty").isZero();
        metric = MockKafkaMetric.of("pack.bean1.avg", "grp1", new Avg());
        reporter.metricChange(metric);
        assertThat(reporter.size())
                .withFailMessage("Size of 'mbeans' in 'JmxReporter' should be incremented by 1").isEqualTo(1);
        assertThat(reporter.getMBean(metric.metricName()).size())
                .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 1").isEqualTo(1);
        MockKafkaMetric metricAnotherGroup = MockKafkaMetric.of("pack.bean2.total", "grp2", new Total());
        reporter.metricChange(metricAnotherGroup);
        assertThat(reporter.size())
                .withFailMessage("Size of 'mbeans' in 'JmxReporter' should be incremented by 1").isEqualTo(2);
        assertThat(reporter.getMBean(metricAnotherGroup.metricName()).size())
                .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 1").isEqualTo(1);
        List<KafkaMetric> list = new ArrayList<KafkaMetric>();
        metric = MockKafkaMetric.of("pack.bean1.some", "grp1", new Total());
        list.add(metric);
        metric = MockKafkaMetric.of("pack.bean2.some", "grp1", new Total());
        list.add(metric);
        reporter.init(list);
        assertThat(reporter.size())
                .withFailMessage("Size of 'mbeans' in 'JmxReporter' should not change").isEqualTo(2);
        assertThat(reporter.getMBean(metric.metricName()).size())
                .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be incremented by 2").isEqualTo(3);
        reporter.metricRemoval(metric);
        assertThat(reporter.size())
                .withFailMessage("Size of 'mbeans' in 'JmxReporter' should not change").isEqualTo(2);
        assertThat(reporter.getMBean(metric.metricName()).size())
                .withFailMessage("Size of 'metrics' in 'KafkaMbean' should be decremented by 1").isEqualTo(2);
        reporter.metricRemoval(metricAnotherGroup);
        assertThat(reporter.size())
                .withFailMessage("Size of 'mbeans' in 'JmxReporter' should be decremented by 1").isEqualTo(1);
        assertThat(reporter.getMBean(metricAnotherGroup.metricName()).size())
                .withFailMessage("List of 'metrics' in 'KafkaMbean' should be empty").isZero();
        reporter.close();
        assertThat(reporter.size())
                .withFailMessage("List of 'mbeans' in 'JmxReporter' should be empty").isZero();
        assertThat(reporter.getMBean(metric.metricName()).size())
                .withFailMessage("List of 'metrics' in 'KafkaMbean' should be empty").isZero();
    }

    @Test
    public void testGetMBeanNameWithNull() {
        final JmxReporter reporter = new JmxReporter();
        assertThatThrownBy(new ThrowableAssert.ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                reporter.getMBeanName(null);
            }
        }).isInstanceOf(NullPointerException.class).hasMessage("Parameter 'metricName' is mandatory");
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
