package io.openio.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumulatingReducer extends MapReduceBase
    implements Reducer<Text, Text, Text, Text> {
  static final String VALUE_TYPE_LONG = "l:";
  static final String VALUE_TYPE_FLOAT = "f:";
  static final String VALUE_TYPE_STRING = "s:";

  private static final Logger LOG =
      LoggerFactory.getLogger(AccumulatingReducer.class);
  
  protected String hostName;
  
  public AccumulatingReducer () {
    try {
      hostName = java.net.InetAddress.getLocalHost().getHostName();
    } catch(Exception e) {
      hostName = "localhost";
    }
    LOG.info("Starting AccumulatingReducer on " + hostName);
  }
  
  public void reduce(Text key, 
                     Iterator<Text> values,
                     OutputCollector<Text, Text> output, 
                     Reporter reporter
                     ) throws IOException {
    String field = key.toString();

    reporter.setStatus("starting " + field + " ::host = " + hostName);

    // concatenate strings
    if (field.startsWith(VALUE_TYPE_STRING)) {
      StringBuffer sSum = new StringBuffer();
      while (values.hasNext())
        sSum.append(values.next().toString()).append(";");
      output.collect(key, new Text(sSum.toString()));
      reporter.setStatus("finished " + field + " ::host = " + hostName);
      return;
    }
    // sum long values
    if (field.startsWith(VALUE_TYPE_FLOAT)) {
      float fSum = 0;
      while (values.hasNext())
        fSum += Float.parseFloat(values.next().toString());
      output.collect(key, new Text(String.valueOf(fSum)));
      reporter.setStatus("finished " + field + " ::host = " + hostName);
      return;
    }
    // sum long values
    if (field.startsWith(VALUE_TYPE_LONG)) {
      long lSum = 0;
      while (values.hasNext()) {
        lSum += Long.parseLong(values.next().toString());
      }
      output.collect(key, new Text(String.valueOf(lSum)));
    }
    reporter.setStatus("finished " + field + " ::host = " + hostName);
  }
}
