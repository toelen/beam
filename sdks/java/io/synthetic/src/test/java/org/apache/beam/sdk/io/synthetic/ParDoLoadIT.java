package org.apache.beam.sdk.io.synthetic;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class ParDoLoadIT {

  @Test
  public void parDoLoadTest() {
    pipeline.apply(SyntheticBoundedIO.readFrom(sourceOptions))
        .apply(ParDo.of(new SyntheticStep(stepOptions)));
  }

}
