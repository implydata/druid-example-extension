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

package io.imply.druid.example;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.imply.druid.example.extraction.ExampleExtractionFn;
import io.imply.druid.example.parser.ExampleParseSpec;

import java.util.List;

public class ExampleExtensionModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    // Register Jackson module for any classes we need to be able to use in JSON queries or ingestion specs.
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName()).registerSubtypes(
            new NamedType(ExampleParseSpec.class, ExampleParseSpec.TYPE_NAME),
            new NamedType(ExampleExtractionFn.class, ExampleExtractionFn.TYPE_NAME)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
