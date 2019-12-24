package io.openio.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class DFSIO implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(DFSIO.class);

    private static final String USAGE =
            "Usage: " + DFSIO.class.getSimpleName() +
                    " [genericOptions]" +
                    " -read | -write | -clean" +
                    " [-nrFiles N]" +
                    " [-size Size[B|KB|MB|GB|TB]]" +
                    " [-baseDir baseDir]";

    private static final String BASE_FILE_NAME = "test_io_";

    private static final long MEGA = ByteMultiple.MB.value();

    private static final int DEFAULT_BUFFER_SIZE = 1000000;

    private static final int DEFAULT_NR_FILES = 1;

    private static final long DEFAULT_NR_BYTES = 10 * MEGA;

    private static final String DEFAULT_BASE_DIR = "/tmp/dfsio";


    private Configuration config;

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    private enum TestType {
        TEST_TYPE_READ("read"),
        TEST_TYPE_WRITE("write"),
        TEST_TYPE_CLEAN("clean");

        private String type;

        TestType(String t) {
            type = t;
        }


        @Override
        public String toString() {
            return type;
        }
    }

    enum ByteMultiple {
        B(1L),
        KB(0x400L),
        MB(0x100000L),
        GB(0x40000000L),
        TB(0x10000000000L);

        private long multi;

        ByteMultiple(long multiplier) {
            multi = multiplier;
        }

        long value() {
            return multi;
        }

        static ByteMultiple parseString(String multiple) {
            if (multiple == null || multiple.length() == 0) {
                return MB;
            }
            String upper = StringUtils.toUpperCase(multiple);
            if (StringUtils.toUpperCase(B.name()).endsWith(upper)) {
                return B;
            }
            if (StringUtils.toUpperCase(KB.name()).endsWith(upper)) {
                return KB;
            }
            if (StringUtils.toUpperCase(MB.name()).endsWith(upper)) {
                return MB;
            }
            if (StringUtils.toUpperCase(GB.name()).endsWith(upper)) {
                return GB;
            }
            if (StringUtils.toUpperCase(B.name()).endsWith(upper)) {
                return TB;
            }
            throw new IllegalArgumentException("Unsupported ByteMultiple" + multiple);
        }
    }

    static float toMB(long bytes) {
        return ((float) bytes) / MEGA;
    }

    static float msToSecs(long ms) {
        return ms / 1000.0f;
    }

    static long parseSize(String arg) {
        String[] args = arg.split("\\D", 2);
        assert args.length <= 2;
        long nrBytes = Long.parseLong(args[0]);
        String multiple = arg.substring(args[0].length());
        return nrBytes * ByteMultiple.parseString(multiple).value();
    }

    public DFSIO() {
        this.config = new Configuration();
    }

    private static String getBaseDir(Configuration config) {
        return config.get("dfsio.base.dir");
    }

    private static Path getReadDir(Configuration config) {
        return new Path(getBaseDir(config), "read");
    }

    private static Path getWriteDir(Configuration config) {
        return new Path(getBaseDir(config), "write");
    }

    private static Path getControlDir(Configuration config) {
        return new Path(getBaseDir(config), "control");
    }

    private static Path getDataDir(Configuration config) {
        return new Path(getBaseDir(config), "data");
    }

    private Path getReduceFilePath(TestType testType) {
        Path dir = null;
        if (testType == TestType.TEST_TYPE_READ) {
            dir = getReadDir(config);
        } else if (testType == TestType.TEST_TYPE_WRITE) {
            dir = getWriteDir(config);
        }
        return new Path(dir, "part-00000");
    }

    public static void main(String[] args) {
        DFSIO dfsio = new DFSIO();
        int res;
        try {
            res = ToolRunner.run(dfsio, args);
        } catch (Exception e) {
            System.err.print(StringUtils.stringifyException(e));
            res = -2;
        }
        if (res == -1) {
            System.err.println(USAGE);
        }
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        TestType testType = null;
        int nrFiles = DEFAULT_NR_FILES;
        long nrBytes = DEFAULT_NR_BYTES;
        String baseDir = DEFAULT_BASE_DIR;
        int bufferSize = DEFAULT_BUFFER_SIZE;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase("-read")) {
                testType = TestType.TEST_TYPE_READ;
            } else if (arg.equalsIgnoreCase("-write")) {
                testType = TestType.TEST_TYPE_WRITE;
            } else if (arg.equalsIgnoreCase("-clean")) {
                testType = TestType.TEST_TYPE_CLEAN;
            } else if (arg.equalsIgnoreCase("-nrFiles")) {
                nrFiles = Integer.parseInt(args[++i]);
            } else if (arg.equalsIgnoreCase("-size")) {
                nrBytes = parseSize(args[++i]);
            } else if (arg.equalsIgnoreCase("-baseDir")) {
                baseDir = args[++i];
            } else {
                System.err.println("Illegal argument: " + arg);
                return -1;
            }
        }

        if (testType == null) {
            return -1;
        }

        config.setInt("dfsio.file.buffer.size", bufferSize);
        config.set("dfsio.base.dir", baseDir);

        URI uri = new URI(baseDir);
        FileSystem fs = FileSystem.get(uri, config);

        LOG.info("nrFiles = " + nrFiles);
        LOG.info("size (MB) = " + nrBytes);
        LOG.info("bufferSize = " + bufferSize);
        LOG.info("baseDir = " + getBaseDir(config));

        if (testType == TestType.TEST_TYPE_CLEAN) {
            clean(fs);
            return 0;
        }

        createControlFile(fs, nrBytes, nrFiles);

        long start = System.currentTimeMillis();

        switch (testType) {
            case TEST_TYPE_WRITE:
                writeTest(fs);
                break;
            case TEST_TYPE_READ:
                readTest(fs);
                break;
            default:
        }

        long execTime = System.currentTimeMillis() - start;
        analyzeResult(fs, testType, execTime);

        return 0;
    }

    private void createControlFile(FileSystem fs, long nrBytes, int nrFiles) throws IOException {
        LOG.info("Creating control file: " + nrBytes + " bytes, " + nrFiles + " files");
        Path controlDir = getControlDir(config);
        fs.delete(controlDir, true);

        for (int i = 0; i < nrFiles; i++) {
            String name = getFileName(i);
            Path controlFile = new Path(controlDir, "in_file_" + name);
            SequenceFile.Writer writer = null;
            try {
                writer = SequenceFile.createWriter(
                        fs, config, controlFile, Text.class, LongWritable.class,
                        SequenceFile.CompressionType.NONE);
                writer.append(new Text(name), new LongWritable(nrBytes));
            } catch (Exception e) {
                throw new IOException(e.getLocalizedMessage());
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    private static String getFileName(int idx) {
        return BASE_FILE_NAME + idx;
    }

    private abstract static class IOStatMapper extends IOMapperBase<Long> {

        IOStatMapper() {
        }

        @Override
        void collectStats(OutputCollector<Text, Text> output,
                          String name,
                          long execTime,
                          Long objSize) throws IOException {
            long totalSize = objSize;
            float ioRateMbSec = (float) totalSize * 1000 / (execTime * MEGA);
            LOG.info("Number of bytes processed = " + totalSize);
            LOG.info("Exec time = " + execTime);
            LOG.info("IO rate = " + ioRateMbSec);

            output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "tasks"),
                    new Text(String.valueOf(1)));
            output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "size"),
                    new Text(String.valueOf(totalSize)));
            output.collect(new Text(AccumulatingReducer.VALUE_TYPE_LONG + "time"),
                    new Text(String.valueOf(execTime)));
            output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "rate"),
                    new Text(String.valueOf(ioRateMbSec * 1000)));
            output.collect(new Text(AccumulatingReducer.VALUE_TYPE_FLOAT + "sqrate"),
                    new Text(String.valueOf(ioRateMbSec * ioRateMbSec * 1000)));
        }
    }

    public static class WriteMapper extends IOStatMapper {
        public WriteMapper() {
            for (int i = 0; i < bufferSize; i++) {
                buffer[i] = (byte) ('0' + i % 50);
            }
        }

        @Override
        public Closeable getIOStream(String name) throws IOException {
            Path filePath = new Path(getDataDir(getConf()), name);
            OutputStream out = fs.create(filePath, true, bufferSize);
            LOG.info("out = " + out.getClass().getName());
            return out;
        }

        @Override
        public Long doIO(Reporter reporter, String name, long totalSize) throws IOException {
            OutputStream out = (OutputStream) this.stream;
            long nrRemaining;
            for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
                int curSize = bufferSize < nrRemaining ? bufferSize : (int) nrRemaining;
                out.write(buffer, 0, curSize);
                reporter.setStatus(String.format("writing %s@%d/%d ::host = %s", name, totalSize - nrRemaining, totalSize, hostName));
            }
            return totalSize;
        }
    }

    private long writeTest(FileSystem fs) throws IOException {
        Path writeDir = getWriteDir(config);
        fs.delete(getDataDir(config), true);
        fs.delete(writeDir, true);
        long start = System.currentTimeMillis();
        runIOTest(WriteMapper.class, writeDir);
        return System.currentTimeMillis() - start;
    }

    public static class ReadMapper extends IOStatMapper {

        public ReadMapper() {
        }

        @Override
        public Closeable getIOStream(String name) throws IOException {
            InputStream in = fs.open(new Path(getDataDir(getConf()), name));
            LOG.info("in = " + in.getClass().getName());
            return in;
        }

        @Override
        Long doIO(Reporter reporter, String name, long totalSize) throws IOException {
            InputStream in = (InputStream) this.stream;
            long actualSize = 0;
            while (actualSize < totalSize) {
                int curSize = in.read(buffer, 0, bufferSize);
                if (curSize < 0) break;
                actualSize += curSize;
                reporter.setStatus(String.format("reading %s@%d/%d ::host = %s", name, actualSize, totalSize, hostName));
            }
            return actualSize;
        }
    }

    private long readTest(FileSystem fs) throws IOException {
        Path readDir = getReadDir(config);
        fs.delete(readDir, true);
        long start = System.currentTimeMillis();
        runIOTest(ReadMapper.class, readDir);
        return System.currentTimeMillis() - start;
    }

    private void runIOTest(
            Class<? extends Mapper<Text, LongWritable, Text, Text>> mapperClass,
            Path outputDir) throws IOException {
        JobConf job = new JobConf(config, DFSIO.class);
        job.setJobName("DFSIO");
        FileInputFormat.setInputPaths(job, getControlDir(config));
        job.setInputFormat(SequenceFileInputFormat.class);
        job.setMapperClass(mapperClass);
        job.setReducerClass(AccumulatingReducer.class);

        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setSpeculativeExecution(false);
        JobClient.runJob(job);
    }

    private void clean(FileSystem fs) throws IOException {
        LOG.info("Cleaning up");
        fs.delete(new Path(getBaseDir(config)), true);
    }

    private void analyzeResult(FileSystem fs, TestType testType, long execTime) throws IOException {
        Path reduceFile = getReduceFilePath(testType);
        long tasks = 0;
        long size = 0;
        long time = 0;
        float rate = 0;
        float sqrate = 0;
        DataInputStream in = null;
        BufferedReader lines = null;

        try {
            in = new DataInputStream(fs.open(reduceFile));
            lines = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = lines.readLine()) != null) {
                StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
                String attr = tokens.nextToken();
                if (attr.endsWith(":tasks")) {
                    tasks = Long.parseLong(tokens.nextToken());
                } else if (attr.endsWith(":size")) {
                    size = Long.parseLong(tokens.nextToken());
                } else if (attr.endsWith(":time")) {
                    time = Long.parseLong(tokens.nextToken());
                } else if (attr.endsWith(":rate")) {
                    rate = Float.parseFloat(tokens.nextToken());
                } else if (attr.endsWith(":sqrate")) {
                    sqrate = Float.parseFloat(tokens.nextToken());
                }
            }
        } finally {
            if (in != null) {
                in.close();
            }
            if (lines != null) {
                lines.close();
            }
        }

        double med = rate / 1000 / tasks;
        double stdDeviation = Math.sqrt(Math.abs(sqrate / 1000 / tasks - (med * med)));
        DecimalFormat df = new DecimalFormat("#.##");

        String[] results = {
                "  ----- DFSIO ----- : " + testType,
                "                Date: " + new Date(System.currentTimeMillis()),
                "     Number of files: " + tasks,
                "Total Processed (MB): " + df.format(toMB(size)),
                "   Throughput (MB/s): " + df.format(toMB(size) / msToSecs(time)),
                "   Average IO (MB/s): " + df.format(med),
                "    IO std deviation: " + df.format(stdDeviation),
                "      Total Time (s): " + df.format(msToSecs(execTime)),
                ""
        };

        for (String line : results) {
            LOG.info(line);
        }
    }


    public void setConf(Configuration conf) {
        this.config = conf;
    }

    public Configuration getConf() {
        return this.config;
    }
}
