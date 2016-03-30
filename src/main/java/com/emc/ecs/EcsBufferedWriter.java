package com.emc.ecs;

import com.emc.ecs.s3.sample.ECSS3Factory;
import com.emc.object.Protocol;
import com.emc.object.Range;
import com.emc.object.s3.S3Client;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.emc.rest.smart.ecs.Vdc;
import com.emc.ecs.util.OptionBuilder;
import org.apache.commons.cli.*;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by conerj on 3/15/16.
 * copied from _01_CreateObject
 */
public class EcsBufferedWriter {
    private static final Logger l4j = Logger.getLogger(EcsBufferedWriter.class);
    //public static int BUFFER_SIZE = 2*1024*1024;
    public static int BUFFER_SIZE = 200;
    S3Client s3;

    //S3_ECS_NAMESPACE (optional otherwise null)

    public static final String VERSION_OPTION = "v";
    public static final String VERSION_DESC = "prints version information";

    public static final String ENDPOINT_OPTION = "e";
    public static final String ENDPOINT_ARG = "endpoint";
    public static final String ENDPOINT_DESC = "(required) the ECS endpoint (default scheme is HTTPS, default ports are 9020 for HTTP, 9021 for HTTPS). multiple VDCs can be specified by separating endpoints with commas. format is [http[s]://]<vdc1-ip>[:port][,[http[s]://]<vdc2-ip>[:port]][,...]";

    public static final String BUCKET_OPTION = "b";
    public static final String BUCKET_ARG = "bucket";
    public static final String BUCKET_DESC = "(required) the bucket to/from which to upload/download the file (must exist if download streaming)";

    public static final String ACCESS_KEY_OPTION = "a";
    public static final String ACCESS_KEY_ARG = "access-key";
    public static final String ACCESS_KEY_DESC = "(required) the access key (user)";

    public static final String SECRET_KEY_OPTION = "s";
    public static final String SECRET_KEY_ARG = "secret-key";
    public static final String SECRET_KEY_DESC = "(required) the user's secret key";

    public static final String KEY_OPTION = "k";
    public static final String KEY_ARG = "object-key";
    public static final String KEY_DESC = "the object key (name) to use for the S3 file including any directory prefix.";


    public static final String NAMESPACE_OPTION = "n";
    public static final String NAMESPACE_ARG = "namespace";
    public static final String NAMESPACE_DESC = "(optional) namespace of the bucket. It's normally the default s3 namespace";

    public static final String BYTES_FROM_END_OPTION = "c";
    public static final String BYTES_FROM_END_ARG = "object-key";
    public static final String BYTES_FROM_END_DESC = "Applies on to tail. Number of bytes from end.";

    public static final String WRITE_COMMAND = "write";
    public static final String APPEND_COMMAND = "append";
    public static final String TAILBYTE_COMMAND = "tailbyte";
    public static final String TAIL_COMMAND = "tail";



    public void EcsBufferedWriter() {
        try {
            System.out.println("creating the s3 client");
            s3 = ECSS3Factory.getS3Client();
        }
        catch(Exception e) {
            System.out.println("Exception instantiating the S3Client");
        }
    }


    protected static Options options() {
        Options options = new Options();
        options.addOption(new OptionBuilder().withDescription(VERSION_DESC).create(VERSION_OPTION));

        options.addOption(new OptionBuilder().hasArgs().withValueSeparator(',').withArgName(ENDPOINT_ARG)
                .withDescription(ENDPOINT_DESC).create(ENDPOINT_OPTION));
        options.addOption(new OptionBuilder().hasArg().withArgName(ACCESS_KEY_ARG)
                .withDescription(ACCESS_KEY_DESC).create(ACCESS_KEY_OPTION));
        options.addOption(new OptionBuilder().hasArg().withArgName(SECRET_KEY_ARG)
                .withDescription(SECRET_KEY_DESC).create(SECRET_KEY_OPTION));
        options.addOption(new OptionBuilder().hasArg().withArgName(BUCKET_ARG)
                .withDescription(BUCKET_DESC).create(BUCKET_OPTION));
        options.addOption(new OptionBuilder().hasArg().withArgName(KEY_ARG)
                .withDescription(KEY_DESC).create(KEY_OPTION));

        options.addOption(new OptionBuilder().hasArg().withArgName(NAMESPACE_ARG)
                .withDescription(NAMESPACE_DESC).create(NAMESPACE_OPTION));
        options.addOption(new OptionBuilder().hasArg().withArgName(BYTES_FROM_END_ARG)
                .withDescription(BYTES_FROM_END_DESC).create(BYTES_FROM_END_OPTION));
        return options;
    }

    protected static WriterTask createTask(CommandLine line) throws Exception {

        WriterTask task;

        // Special check for version
        if (line.hasOption(VERSION_OPTION)) {
            //System.out.println(versionLine());
            System.out.println("JMC 1.0");
            System.exit(0);
        }

        //LogManager.getRootLogger().setLevel(Level.INFO);
        //this.l4j.
        //LogManager.getRootLogger().addAppender(new FileAppender());
        // check required options
        boolean status = true;
        if (line.hasOption(ENDPOINT_OPTION) == false) {
            status = false;
            System.out.println("Missing endpoint option");
        }
        if (line.hasOption(ACCESS_KEY_OPTION) == false) {
            status = false;
            System.out.println("Missing access key option");
        }
        if (line.hasOption(SECRET_KEY_OPTION) == false) {
            status = false;
            System.out.println("Missing secret key option");
        }
        if (line.hasOption(BUCKET_OPTION) == false) {
            status = false;
            System.out.println("Missing bucket option");
        }
        if (line.hasOption(KEY_OPTION) == false) {
            status = false;
            System.out.println("Missing key/file option");
        }
        if (line.getArgs().length == 0) {
            status = false;
            //JMC tailbuf is undocumented because that last partially filled buffer doesn't
            //flush if it's just hanging around waiting
            System.out.println("Missing initial command option (write|append|tail)");
        }
        if (status == false) {
            help();
            System.exit(1);
        }

        String command = line.getArgs()[0];

        // parse endpoint string
        String[] endpointStrings = line.getOptionValues(ENDPOINT_OPTION);
////////////////////////////////////
        /*
        Protocol protocol = Protocol.HTTPS;
        int port = -1;
        List<Vdc> vdcList = new ArrayList<Vdc>();
        for (String endpointString : endpointStrings) {
            URI endpoint = new URI(endpointString);

            // check for just a host
            if (endpointString.matches("^[a-zA-Z0-9.-]*$"))
                endpoint = new URI(null, endpointString, null, null);

            // get protocol
            if (endpoint.getScheme() != null) protocol = Protocol.valueOf(endpoint.getScheme().toUpperCase());

            // get port
            port = endpoint.getPort();

            vdcList.add(new Vdc(endpoint.getHost()));
        }

        // create S3 config
        S3Config s3Config = new S3Config(protocol, vdcList.toArray(new Vdc[vdcList.size()])).withPort(port);
        s3Config.withIdentity(line.getOptionValue(ACCESS_KEY_OPTION)).withSecretKey(line.getOptionValue(SECRET_KEY_OPTION));
        //s3Config.setProperty(S3Config.PROPERTY_DISABLE_HEALTH_CHECK, true);
        */
 ///////
        S3Config s3Config = new S3Config(new URI(endpointStrings[0]));
        s3Config.withIdentity(line.getOptionValue(ACCESS_KEY_OPTION)).withSecretKey(line.getOptionValue(SECRET_KEY_OPTION));
        if (line.hasOption(NAMESPACE_OPTION) == true) {
            String ns = new String(line.getOptionValue(NAMESPACE_OPTION));
            System.err.println("Creating s3Config with namespace: " + ns);
            s3Config.withNamespace(ns);
        }
        l4j.debug(s3Config);
        
        S3Client s3Client = new S3JerseyClient(s3Config);
        String bucket = line.getOptionValue(BUCKET_OPTION);
        // figure out file/key name
        String key = line.getOptionValue(KEY_OPTION);

        System.err.println("executing command: " + command);
        //if stream to ecs
        if (WRITE_COMMAND.equals(command)) {
            System.err.println("Found matching write command");
            StreamToEcs ste = new StreamToEcs(s3Client, bucket, key, StreamToEcs.DOWRITE);
            task = new WriterTask(ste);
        }
        else if (APPEND_COMMAND.equals(command)) {
            System.err.println("Found matching append command");
            StreamToEcs ste = new StreamToEcs(s3Client, bucket, key, StreamToEcs.DOAPPEND);
            task = new WriterTask(ste);
        }
        //else if tail from ecs
        else if (TAILBYTE_COMMAND.equals(command)) {
            System.err.println("Found matching tail command");
            TailFromEcs tfe = new TailFromEcs(s3Client, bucket, key, TailFromEcs.TAILBYTE);
            task = new WriterTask(tfe);
        }
        else if (TAIL_COMMAND.equals(command)) {
            System.err.println("Found matching tailbuf command");
            TailFromEcs tfe = new TailFromEcs(s3Client, bucket, key, TailFromEcs.TAIL);
            if (line.hasOption(BYTES_FROM_END_OPTION) == true) {
                Long bytesFromEnd = new Long(line.getOptionValue(BYTES_FROM_END_OPTION));
                tfe.bytesFromEnd = bytesFromEnd.longValue();
                System.err.println("tailing " + bytesFromEnd + " bytes from EOF");

            }
            task = new WriterTask(tfe);
        }
        //else... not sure
        else {
            throw new RuntimeException("unrecognized command word");
        }

        return (task);
    }


    public static class WriterTask implements Runnable {
        private Runnable delegate;

        public WriterTask() {

        }
        public WriterTask(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            delegate.run();
        }

        public Runnable getDelegate() {
            return delegate;
        }
    }


    protected static void help() {
        HelpFormatter fmt = new HelpFormatter();
        fmt.setWidth(79);

        Options options = options();
        fmt.printHelp("java -jar ECSWriter.jar (write|append|tail) [options]\n"
                + "options:", options);
    }

    public static void main(String... args) {
        try {
            Options opts = options();
            CommandLine line = new GnuParser().parse(opts, args, true);
            /*
            for (org.apache.commons.cli.Option o : opts.getOptions()) {
                System.out.println("here's an opt: " + o.getOpt());
            }
            */

            WriterTask task = EcsBufferedWriter.createTask(line);
            task.run();

        } catch (Exception e) {
            e.printStackTrace();
            help();
            System.exit(1);
        }
    }

}
