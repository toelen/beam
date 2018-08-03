/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.synthetic;

import static org.apache.beam.sdk.io.synthetic.SyntheticSourceTestUtils.fromString;

import java.io.IOException;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance fanout test for {@link GroupByKey}. */
@RunWith(JUnit4.class)
public class GroupByKeyLoadIT {

  private static Options options;

  private static SyntheticBoundedIO.SyntheticSourceOptions sourceOptions;

  @Rule public TestPipeline pipeline = TestPipeline.create();

  /** Pipeline options for the test. */
  public interface Options extends TestPipelineOptions {

    @Description("The JSON representation of SyntheticBoundedInput.SourceOptions.")
    @Validation.Required
    String getInputOptions();

    void setInputOptions(String inputOptions);

    @Description("The number of gbk to perform")
    @Default.Integer(5)
    Integer getFanout();

    void setFanout(Integer shuffleFanout);

    @Description("The number of reiterations to perform")
    @Default.Integer(1)
    Integer getIterations();

    void setIterations(Integer shuffleFanout);
  }

  @BeforeClass
  public static void setup() throws IOException {
    PipelineOptionsFactory.register(Options.class);

    options =
        PipelineOptionsValidator.validate(
            Options.class, TestPipeline.testingPipelineOptions().as(Options.class));

    sourceOptions = fromString(options.getInputOptions());
  }

  @Test
  public void groupByKeyLoadTest() {
    PCollection<KV<byte[], byte[]>> input = pipeline.apply(SyntheticBoundedIO.readFrom(sourceOptions));

    // fanout
    for (int branch = 0; branch < options.getFanout(); branch++) {
      input
        .apply(ParDo.of(new SyntheticStep(stepOptions)))
        .apply("Group", GroupByKey.create())
        .apply("Ungroup and reiterate", ParDo.of(new DoFn<KV<byte[], Iterable<byte[]>>, KV<byte[], byte[]>>() {
              Integer iterations = options.getIterations();

              @ProcessElement
              public void processElement(ProcessContext c) {
                byte[] key = c.element().getKey();

                // Reiterate 10 times emit output once
                for (int i = 0; i < iterations; i++) {
                  for (byte[] value : c.element().getValue()) {

                    if (i == iterations - 1) {
                      c.output(KV.of(key, value));
                    }
                  }
                }
              }
            }));
    }

    pipeline.run().waitUntilFinish();
  }



    }
  }
}
