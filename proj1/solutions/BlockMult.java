//INF553 HW1
//Myungjin Lee 
//USC ID 5128876730
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BlockMult {

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int dim=2;
            String[]indexArr=new String[dim];
            List<String> myList = new ArrayList<>();
            String lines = value.toString();
            Pattern patKey = Pattern.compile("\\((\\d+,\\d+)\\)"); 
            Matcher matKey = patKey.matcher(lines);
            try {
               if (matKey.find()) {
                  indexArr=matKey.group(1).split(",");
                  }
                } catch(Exception e) {
                }
            Pattern patVal = Pattern.compile("\\((\\d+,\\d+,\\d+)\\)");
            Matcher matVal = patVal.matcher(lines);
            try {
               while (matVal.find()) {
                  myList.add(matVal.group(1));                
                  }
                } catch(Exception e) {
                }
            if (myList.size()<4){
               if (!myList.get(0).substring(0,3).contains("1,1")){
                  myList.add(0,"1,1,0");
               }
               try {
                 if (!myList.get(1).substring(0,3).contains("1,2")){
                  myList.add(1,"1,2,0");
                  }
               } catch ( IndexOutOfBoundsException e ) {
                 myList.add("1,2,0");
               }
                try {
                 if (!myList.get(2).substring(0,3).contains("2,1")){
                  myList.add(2,"2,1,0");
                  }
               } catch ( IndexOutOfBoundsException e ) {
                 myList.add("2,1,0");
               }
                try {
                 if (!myList.get(3).substring(0,3).contains("2,2")){
                  myList.add(3,"2,2,0");
                  }
               } catch ( IndexOutOfBoundsException e ) {
                 myList.add("2,2,0");
               }

            }
            for (int x=1;x<=3;x++){
            context.write(new Text(indexArr[0] + "," + x), new Text("A" + "," + indexArr[1] + "," + myList));
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int dim=2;
            String[]indexArr=new String[dim];
            List<String> myList = new ArrayList<>();
            String lines = value.toString();
            Pattern patKey = Pattern.compile("\\((\\d+,\\d+)\\)");
            Matcher matKey = patKey.matcher(lines);
            try {
               if (matKey.find()) {
                  indexArr = matKey.group(1).split(",");
                  }
                } catch(Exception e) {
                }
            Pattern patVal = Pattern.compile("\\((\\d+,\\d+,\\d+)\\)");
            Matcher matVal = patVal.matcher(lines);
            try {
               while (matVal.find()) {
                  myList.add(matVal.group(1));
                  }
                } catch(Exception e) {
                }
            if (myList.size()<4){
               if (!myList.get(0).substring(0,3).contains("1,1")){
                  myList.add(0,"1,1,0");
               }
               try {
                 if (!myList.get(1).substring(0,3).contains("1,2")){
                  myList.add(1,"1,2,0");
                  }
               } catch ( IndexOutOfBoundsException e ) {
                 myList.add("1,2,0");
               }
                try {
                 if (!myList.get(2).substring(0,3).contains("2,1")){
                  myList.add(2,"2,1,0");
                  }
               } catch ( IndexOutOfBoundsException e ) {
                 myList.add("2,1,0");
               }
                try {
                 if (!myList.get(3).substring(0,3).contains("2,2")){
                  myList.add(3,"2,2,0");
                  }
               } catch ( IndexOutOfBoundsException e ) {
                 myList.add("2,2,0");
               }

            }
            for (int x=1;x<=3;x++){
            context.write(new Text(x+","+indexArr[1]), new Text("B" + "," + indexArr[0] + "," + myList));
            }
        }
    }
    public static class BlockReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String,List<String>> aMap = new TreeMap<String,List<String>>();
            Map<String,List<String>> bMap = new TreeMap<String,List<String>>();
            Map<String,List<String>> cMap = new TreeMap<String,List<String>>();

            for (Text val: values) {
                String[] temp = val.toString().split(",");
                if (temp[0].equals("A")) {
                    Pattern aVal = Pattern.compile("(\\d+,\\d+,\\d+)");
                    Matcher aMatVal = aVal.matcher(val.toString());
                    List<String> aList = new ArrayList<>();
                    while (aMatVal.find()){
                       aList.add(aMatVal.group(1));
                    }
                    aMap.put(temp[1], aList);
                }
                else {
                    Pattern bVal = Pattern.compile("(\\d+,\\d+,\\d+)");
                    Matcher bMatVal = bVal.matcher(val.toString());
                    List<String> bList = new ArrayList<>();
                    while (bMatVal.find()){
                       bList.add(bMatVal.group(1));
                    }
                    bMap.put(temp[1],bList);
                }
            }
            List<String> cList = new ArrayList<>();
                int key1=0;
                int key2=0;
                int key3=0;
                int key4=0;
            for (Map.Entry<String,List<String>> aentry :aMap.entrySet()){
                for (Map.Entry<String,List<String>> bentry :bMap.entrySet()){ 
                    String aMapKey = aentry.getKey();
                    String bMapKey = bentry.getKey();
                    List<String> aMapVal = aentry.getValue();
                    List<String> bMapVal = bentry.getValue();
                    if (aMapKey.equals(bMapKey)){   
                       int aEle11 = Integer.parseInt(aMapVal.get(0).substring(4,5));
                       int aEle12 = Integer.parseInt(aMapVal.get(1).substring(4,5));
                       int aEle21 = Integer.parseInt(aMapVal.get(2).substring(4,5));
                       int aEle22 = Integer.parseInt(aMapVal.get(3).substring(4,5));
                       int bEle11 = Integer.parseInt(bMapVal.get(0).substring(4,5));
                       int bEle12 = Integer.parseInt(bMapVal.get(1).substring(4,5));
                       int bEle21 = Integer.parseInt(bMapVal.get(2).substring(4,5));
                       int bEle22 = Integer.parseInt(bMapVal.get(3).substring(4,5));
                       int cEle11 = aEle11*bEle11 + aEle12*bEle21;
                       int cEle12 = aEle11*bEle12 + aEle12*bEle22;
                       int cEle21 = aEle21*bEle11 + aEle22*bEle21;
                       int cEle22 = aEle21*bEle12 + aEle22*bEle22;
                       key1+=cEle11;
                       key2+=cEle12;
                       key3+=cEle21;
                       key4+=cEle22;
                    }
                }
            }  
            if (key1!=0 || key2!=0 || key3!=0 || key4!=0){
                if (key1!=0){ 
                   cList.add("(1,1,"+key1+")");
                }
                if (key2!=0){
                   cList.add("(1,2,"+key2+")");
                }
                if (key3!=0){
                   cList.add("(2,1,"+key3+")");
                }
                if (key4!=0){
                   cList.add("(2,2,"+key4+")");
                }cMap.put("",cList);
            }
            if (cMap.size()>0){
            context.write(new Text("("+key.toString()+"),"), new Text(cMap.toString().replace("{","").replace("=","").replace("}","")));
            }
        } 
    }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length !=3) {
      System.err.println("ERROR: You need to define three arguments");
      System.exit(2);
    }
    Job job = Job.getInstance(conf,"average");
    job.setJarByClass(BlockMult.class);

    job.setMapperClass(Map1.class);
    job.setMapperClass(Map2.class);

    job.setReducerClass(BlockReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setJarByClass(MultipleInputs.class);

    Path p1=new Path(otherArgs[0]);
    Path p2=new Path(otherArgs[1]);
    
    MultipleInputs.addInputPath(job,p1,TextInputFormat.class,Map1.class);
    MultipleInputs.addInputPath(job,p2,TextInputFormat.class,Map2.class);

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1])); 
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
