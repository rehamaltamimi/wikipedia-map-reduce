/*
 * Borrowed from SVN trunk.  Remove when it's available.
 */

package wikiParser.mapReduce.util;
/**
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


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * An {@link InputFormat} for plain text files. Files are broken into lines.
 * Either line feed or carriage-return are used to signal end of line. 
 * Each line is divided into key and value parts by a separator byte. If no
 * such a byte exists, the key will be the entire line and value will be empty.
 */
public class KeyValueTextInputFormat extends FileInputFormat {

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
      return true;
//    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
//    if (null == codec) {
//      return true;
//    }
//    return codec instanceof SplittableCompressionCodec;
  }

  public RecordReader createRecordReader(InputSplit genericSplit,
      TaskAttemptContext context) throws IOException {
    
    context.setStatus(genericSplit.toString());
    return new KeyValueLineRecordReader(context.getConfiguration());
  }

}