package com.ikanow.aleph2.analytics.r.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rosuda.JRI.Rengine;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by shawnkyzer on 12/8/15.
 */
public class RScriptUtils {

    private static Rengine engine;

    private static List<String> hdfsPaths = new ArrayList<>();

    public static void start(String...args) {
          		if (!Rengine.versionCheck()) {
              		    System.err.println("** Version mismatch " +
                      		    		"- Java files don't match library version. **");
              		    System.exit(1);
              		}
         	    System.out.println("Creating R Engine (with arguments)");
         		// 1) we pass the arguments from the command line
         		// 2) we won't use the main loop at first, we'll start it later
         		//    (that's the "false" as second argument)
        		// 3) the callbacks are implemented by the TextConsole class above
         		engine = new Rengine(args, false, null);
        		// the engine creates R is a new thread, so we should wait until it's ready
            if (!engine.waitForR()) {
             	    	System.out.println("Cannot load R.");
            	        return;
            	    }
        engine.eval("{library(rJava);.jinit()}",false);
     }

    public static void shutdown(){
        engine.end();
    }

    public static void main(String[] args) throws IOException {
        start("--vanilla");
        initializeSparkR();

        System.out.println(engine.eval("jrdd <- SparkR:::callJStatic(\"com.ikanow.aleph2.analytics.r.utils.RScriptUtils\",\"getJavaRDD\", sc)"));
        System.out.println(engine.eval("SparkR:::show(jrdd)"));
        System.out.println(engine.eval("newRdd <- SparkR:::RDD(jrdd, \"string\")"));
        System.out.println(engine.eval("df <- SparkR:::createDataFrame(sqlContext, newRdd)"));
        System.out.println(engine.eval("showDF(df)"));
        System.out.println(engine.eval("localDf <- collect(df)"));
        System.out.println(engine.eval("localDf"));

    //    processAnalytics();
    //    readRScript();
        shutdown();
    }

    private static void processAnalytics() {

        createDataFrame();
    }

    public static JavaRDD<String> getJavaRDD(JavaSparkContext sparkContext){

        System.out.println("Converting" + sparkContext.version() + sparkContext.appName());

        JavaRDD<String> testJRDD = null;
        try {
            testJRDD = sparkContext.textFile("/Users/shawnkyzer/Documents/aleph2_analytic_services_R/hs_err_pid2930.log");
        } catch (Exception e){
            System.out.println(e.fillInStackTrace());
        }
            System.out.println("Converting");
        return testJRDD;
    }

    public static void createDataFrame() {
        // Convert batch to JSON and inject into R native dataframe
        // This will be required for JSON processing
        System.out.println(engine.eval("library(jsonlite)"));

        for (int i = 0; i < hdfsPaths.size(); i++) {
            System.out.println(engine.eval("testdata_"+i+" <- read.df(sqlContext, '"+hdfsPaths.get(i)+"', 'json')"));
        }

    }

    public static void addFilePaths(List<String> updatedPaths) {

        hdfsPaths.addAll(updatedPaths);

    }

    public static void initializeSparkR() {
        start("--vanilla");
        // To do move these into an array or separate file or something and check for null
        // if null we need to exit our gracefully as all statements are required for the jobs to run
        System.out.println(engine.eval(".libPaths(c(.libPaths(), '/root/spark-1.5.2/R/lib'))"));
        System.out.println(engine.eval("Sys.setenv(SPARK_HOME = '/root/spark-1.5.2')"));
        System.out.println(engine.eval("Sys.setenv(PATH = paste(Sys.getenv(c('PATH')), '/root/spark-1.5.2/bin', sep=':'))"));
        System.out.println(engine.eval("library(SparkR)"));
        // We should probably point to the server here Im guessing this is pointing to localhost
        System.out.println(engine.eval("sc <- sparkR.init(sparkJars = \"/root/aleph2_analytic_services_R-0.0.1-SNAPSHOT.jar\")"));
        System.out.println(engine.eval("sqlContext <- sparkRSQL.init(sc)"));

    }

    public static void runTestScript() {
        System.out.println(engine.eval("jrdd <- SparkR:::callJStatic(\"com.ikanow.aleph2.analytics.r.utils.RScriptUtils\",\"getJavaRDD\", sc)"));
        System.out.println(engine.eval("SparkR:::show(jrdd)"));
        System.out.println(engine.eval("newRdd <- SparkR:::RDD(jrdd, \"string\")"));
        System.out.println(engine.eval("df <- SparkR:::createDataFrame(sqlContext, newRdd)"));
        System.out.println(engine.eval("showDF(df)"));
        System.out.println(engine.eval("localDf <- collect(df)"));
        System.out.println(engine.eval("localDf"));
    }



    public static void readRScript(String pathToRScript) {
        Path file = Paths.get(pathToRScript);
        try (Stream<String> lines = Files.lines(file, Charset.defaultCharset())) {
            lines.forEachOrdered(line -> System.out.println(engine.eval(line)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
