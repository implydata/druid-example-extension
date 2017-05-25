/*
 * Copyright 2016 Imply Data, Inc.
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

import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

public class ExampleSumBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;

  ExampleSumBufferAggregator(FloatColumnSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    // The amount of space here is given by getMaxIntermediateSize in the factory.
    buf.putDouble(position, 0.0d);
  }

  @Override
  public final void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(position, buf.getDouble(position) + selector.get());
  }

  @Override
  public final Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public final float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }

  @Override
  public final long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
