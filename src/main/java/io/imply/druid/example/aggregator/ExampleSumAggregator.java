/*
 * Copyright 2019 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.imply.druid.example.aggregator;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

public class ExampleSumAggregator implements Aggregator
{
  private final BaseFloatColumnValueSelector selector;

  private double sum;

  public ExampleSumAggregator(BaseFloatColumnValueSelector selector)
  {
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += selector.getFloat();
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return (float) sum;
  }

  @Override
  public long getLong()
  {
    return (long) sum;
  }

  @Override
  public Aggregator clone()
  {
    return new ExampleSumAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
