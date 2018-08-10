package org.apache.beam.sdk.io.synthetic;


import java.io.IOException;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class ParDoLoadIT {

  private static Options options;

  @Rule public TestPipeline pipeline = TestPipeline.create();




  /** Pipeline options for the test. */
  public interface Options extends TestPipelineOptions {

    @Description("The JSON representation of SyntheticBoundedInput.SourceOptions.")
    @Validation.Required
    String getInputOptions();

    void setInputOptions(String inputOptions);

    @Description("The number of reiterations to perform")
    @Default.Integer(1)
    Integer getParDoCount();

    void setParDoCount(Integer shuffleFanout);
  }

  @BeforeClass
  public static void setup() throws IOException {
    PipelineOptionsFactory.register(GroupByKeyLoadIT.Options.class);

    options =
        PipelineOptionsValidator.validate(Options.class,
            TestPipeline.testingPipelineOptions().as(Options.class));
  }

  @Test
  public void parDoLoadTest() {
    PCollection<KV<byte[], byte[]>> inputData =
        pipeline.apply(SyntheticBoundedIO.readFrom(sourceOptions));

    for (int i = 0; i < options.getParDoCount(); i++) {
      inputData.apply(ParDo.of(new SyntheticStep(stepOptions)));
    }

    pipeline.run().waitUntilFinish();
  }
}
