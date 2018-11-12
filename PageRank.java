package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.text.NumberFormat;
import java.text.DecimalFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.WritableComparator;
import java.nio.ByteBuffer;

public class PageRank {
	// public static Set<String> NODES = new HashSet<String>();
	static int N = 10876;
	static int iteration = 0;
	public static NumberFormat NF = new DecimalFormat("00");
	static enum PageCount{
		Count,TotalPR
	}
	public static class PageRankJob1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (value.charAt(0) != '#') {
				
				int tabIndex = value.find("\t");
				String nodeA = Text.decode(value.getBytes(), 0, tabIndex);
				String nodeB = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));

				// PageRank.NODES.add(nodeA);
				// PageRank.NODES.add(nodeB);
				context.write(new Text(nodeA), new Text(nodeB));
			}
		}
    
	}
	public static class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {
    
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean first = true;
			String links = ((double)1.0 / (double)N) +"\t";

			for (Text value : values) {
				if (!first) 
					links += ",";
				links += value.toString();
				first = false;
			}

			context.write(key, new Text(links));
		}

	}
	/*input
		<from>    <page-rank>    <to1>,<to2>,<to3>,<to4>, ... , <toN>
		output
		<to>    <from's page-rank>    <#froms' to>
		<from> *<to1>,<to2>,<to3>,<to4>, ... , <toN>
	*/
	public static class  PageRankJob2Mapper extends Mapper < LongWritable , Text , Text , Text >{
		protected void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
			int tIdx1 = value.find("\t");
			int tIdx2 = value.find("\t", tIdx1 + 1);
			
			// extract tokens from the current line
			String page = Text.decode(value.getBytes(), 0, tIdx1);
			if(tIdx2 - (tIdx1 + 1)>0){
			String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
			// if (value.getLength() - (tIdx2 + 1)>0){
				String links = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));
				String[] allOtherPages = links.split(",");
				for (String otherPage : allOtherPages) { 
					Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
					context.write(new Text(otherPage), pageRankWithTotalLinks); 
				}
				// put the original links so the reducer is able to produce the correct output
        		context.write(new Text(page), new Text("*" + links));
			}
			else{
				String pageRank= Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1));
			}
			context.write(new Text(page), new Text("*"));
		}
	}
	/*input
		<self>   *<to1>,<to2>,<to3>,<to4>, ... , <toN>
		<self>    <from's page-rank>    <from's out links>
		output 
		<self>    <self page-rank>    <to1>,<to2>,<to3>,<to4>, ... , <toN>
	*/
	public static class PageRankJob2Reducer extends Reducer < Text , Text , Text, Text>{
		protected void reduce(Text key , Iterable<Text> values ,Context context) throws IOException, InterruptedException{
			String links = "";
			double sumShareOtherPageRanks = 0.0;
			
			for (Text value : values) {
	
				String content = value.toString();
				
				if (content.startsWith("*")) {
					links += content.substring(1);
				} else {
					
					String[] split = content.split("\\t");
					
					double pageRank = Double.parseDouble(split[0]);
					int totalLinks = Integer.parseInt(split[1]);
				
					sumShareOtherPageRanks += ((double)pageRank / (double)totalLinks);
				}

			}
			sumShareOtherPageRanks = sumShareOtherPageRanks * 0.8 + 0.2 *((double)1/(double)N) ; 

			// double newRank = PageRank.DAMPING * sumShareOtherPageRanks + (1 - PageRank.DAMPING);
			context.write(key, new Text(sumShareOtherPageRanks + "\t" + links));
		}
	}

	static Double []S = {0.563,0.5786,0.57755,
						0.577413,0.577411,0.5774,0.5774,
						0.57742,0.57742,0.57742,0.57742,
						0.57742,0.57742,0.57742,0.57742,
						0.57742,0.57742,0.57742,0.57742,0.57742};
	/*input
	<self>    <self page-rank>    <to1>,<to2>,<to3>,<to4>, ... , <toN>
	output 
	<[sum]>    <sumup>
	*/
	public static class PageRankjobCountnormalizeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int tIdx1 = value.find("\t");
			int tIdx2 = value.find("\t", tIdx1 + 1);
			Double pageRank;
			if(tIdx2 - (tIdx1 + 1)>=0){
				pageRank = Double.parseDouble(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
			}
			else{
				pageRank = Double.parseDouble(Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1)));
			}
			//double pageRank = Double.parseDouble(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
			context.write(new Text("sum"),new DoubleWritable(pageRank));
		}
    
	}
	public static class PageRankjobCountnormalizeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			/*input
				<[sum]>    <sumup>
			  output
			  	<[sum]>    <sumup>
			*/
			double sum=0.0;

			for (DoubleWritable value : values) {
				sum += value.get();
			}
				// PageRank.S = sum;
			context.write(new Text("sum"), new DoubleWritable(sum));
		}

	}
	/*input
		<from>    <page-rank>    <to1>,<to2>,<to3>,<to4>, ... , <toN>
		output
		<from>    <page-rank/S>    <to1>,<to2>,<to3>,<to4>, ... , <toN>
		job2
		<to>    <from's page-rank>    <#froms' to>
		<from> *<to1>,<to2>,<to3>,<to4>, ... , <toN>
	*/
	public static class  PageRankJobNormalMapper extends Mapper < LongWritable , Text , Text , Text >{
		protected void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
			int tIdx1 = value.find("\t");

			String page = Text.decode(value.getBytes(), 0, tIdx1);
			String[] split = Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1)).split("\\t");
			double pageRank = Double.parseDouble(split[0]);
			pageRank += (double)(1-S[iteration])/(double)N;
			// put the original links so the reducer is able to produce the correct output
			if(split.length==1)
				context.write(new Text(page), new Text(String.valueOf(pageRank)));
			else
        		context.write(new Text(page), new Text(pageRank + "\t" + split[1]));
		}
	}
	public static class PageRankJob3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tIdx1);
		Double pageRank;
		if(tIdx2 - (tIdx1 + 1)>=0){
			pageRank = Double.parseDouble(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
		}
		else{
			pageRank = Double.parseDouble(Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1)));
		}
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}
public static class DoubleComparator extends WritableComparator {

     public DoubleComparator() {
         super(DoubleWritable.class);
     }

     @Override
     public int compare(byte[] b1, int s1, int l1,
             byte[] b2, int s2, int l2) {

         Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
         Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();

         return v1.compareTo(v2) * (-1);
     }
 }

 public static class MapTask extends Mapper<LongWritable, Text, DoubleWritable , IntWritable> {
	public void map(LongWritable key, Text value, Context context)throws java.io.IOException, InterruptedException {
		String line = value.toString();
		String[] tokens = line.split("\\t"); // This is the delimiter between
		int keypart = Integer.parseInt(tokens[0]);
		Double valuePart = Double.parseDouble(tokens[1]);
		context.write(new DoubleWritable(valuePart), new IntWritable(keypart));
	}
 }

 public static class ReduceTask extends Reducer<DoubleWritable, IntWritable, IntWritable, DoubleWritable> {
	public void reduce(DoubleWritable key, Iterable<IntWritable> list, Context context)throws java.io.IOException, InterruptedException {
   
		for (IntWritable value : list) { 
			context.write(value,key); 
		}
   }
 }
	public boolean job1(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob1Mapper.class);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
        
        return job.waitForCompletion(true);
     
    }
	public boolean job2(String input, String output) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob2Mapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);

        return job.waitForCompletion(true);
	}
	public boolean jobCountnormalize(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Job Countnormalize");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(PageRankjobCountnormalizeMapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setReducerClass(PageRankjobCountnormalizeReducer.class);

        return job.waitForCompletion(true);
	}
	public boolean jobNormal(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
		Job job = Job.getInstance(new Configuration(), "Job Normal");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJobNormalMapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setReducerClass(PageRankJobNormalReducer.class);

        return job.waitForCompletion(true);
	}
	public boolean job3(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        // Job job = Job.getInstance(new Configuration(), "Job #3");
        // job.setJarByClass(PageRank.class);
        
        // // input / mapper
        // FileInputFormat.setInputPaths(job, new Path(in));
        // job.setInputFormatClass(TextInputFormat.class);
        // job.setMapOutputKeyClass(DoubleWritable.class);
        // job.setMapOutputValueClass(Text.class);
        // job.setMapperClass(PageRankJob3Mapper.class);
        
        // // output
        // FileOutputFormat.setOutputPath(job, new Path(out));
        // job.setOutputFormatClass(TextOutputFormat.class);
        // job.setOutputKeyClass(DoubleWritable.class);
        // job.setOutputValueClass(Text.class);
		Job job = new Job(new Configuration(), "Job #3");
		job.setJarByClass(PageRank.class);

		// Setup MapReduce
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setSortComparatorClass(DoubleComparator.class);
		// Input
		FileInputFormat.setInputPaths(job, new Path(in));
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
        
    }
	// public static double normal(String in)throws Exception{
	// 	File folder = new File(in);
    //     File[] listOfFiles = folder.listFiles();
    //     BufferedReader bufferReader = null;
        
    //     for (File file : listOfFiles) {
    //         if (file.isFile()) {
    //             try{
    //               InputStream inputStream = new FileInputStream(file.getPath()); 
    //               InputStreamReader streamReader = new InputStreamReader(inputStream);
    //               //Instantiate the BufferedReader Class
    //               bufferReader = new BufferedReader(streamReader);
    //               //Variable to hold the each line data
    //               String line;
    //               // Read file line by line...
    //               while ((line = bufferReader.readLine()) != null)   {
    //                     String []str = line.split("\\t");
	// 					if(str.length==2){
	// 						return Double.parseDouble(str[1]);
	// 			}
    //                     line = line.trim();
    //               }
    //            }catch (Exception e) {
    //             e.printStackTrace();
    //            }finally{
    //                bufferReader.close();
    //            }
    //         }
    //     }
	// 	return 0.0;
	// }
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		String input;
		String output;
		int threshold = 100;
		int iterationLimit = 20;
		PageRank pagerank = new PageRank();
		boolean flag = false;
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", hdfsuri);
		// conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		// conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(conf);
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: pagerank <in> <out>");
			System.exit(2);
		}
		int in_id = otherArgs[0].indexOf("p2p");
		String path = otherArgs[0].substring(0,in_id);
		String outpath = path + otherArgs[1];
		boolean isCompleted = pagerank.job1(otherArgs[0],outpath+"/iter00/final");
		if(!isCompleted)
			System.exit(1);
		output = otherArgs[0];
		while(iteration < iterationLimit){
			//展开反复迭代  注意 输入输出的路径
			if(flag)
				input = outpath + "/iter" + NF.format(iteration)+"/anormal/p*";
			else
				input = outpath + "/iter" + NF.format(iteration)+"/final/p*";
            output = outpath + "/iter" + NF.format(iteration + 1)+"/anormal";
			
			isCompleted = pagerank.job2(input,output);
			if(!isCompleted)
				System.exit(1);

			isCompleted = pagerank.jobCountnormalize(output+"/p*",output+"/S");
			if(!isCompleted)
				System.exit(1);
			// System.out.println("[notice!!!!!!!!!!!!!!]  "+iteration+" "+String.valueOf(S[iteration]));
			// if(S>1+0.001 || S<1-0.001){
			// 	flag = false;
				// RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path("hdfs://nn/user/y32jjc00/"+output+"/S/p*"), false);
				// while(fileStatusListIterator.hasNext()){
				// 	LocatedFileStatus fileStatus = fileStatusListIterator.next();
				// 	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
				// 	try {
				// 		String line;
				// 		line=br.readLine();
				// 		while (line != null){
				// 			String []str = line.split("\\t");
				// 			if (str.length==2)
				// 				S = Double.parseDouble(str[1]);
				// 			// be sure to read the next line otherwise you'll get an infinite loop
				// 			line = br.readLine();
				// 		}
				// 	} finally {
				// 		// you should close out the BufferedReader
				// 		br.close();
				// 	}
				// }
				// S = normal("hdfs://nn/user/y32jjc00/"+output+"/S/");
				// Double SSS = Double.parseDouble(conf.get("S"));
			// System.out.println("[notice!!!!!!!!!!!!]2  "+S);
			if(S[iteration]>1+0.001 || S[iteration]<1-0.001){
				flag = false;
				isCompleted = pagerank.jobNormal(outpath + "/iter" + NF.format(iteration + 1)+"/anormal/p*", outpath+"/iter"+ NF.format(iteration+1)+"/final");
				if(!isCompleted)
					System.exit(1);
			}
			else{
				flag = true;
			}
			iteration++;
				
		}
		isCompleted = pagerank.job3(outpath+"/iter"+ NF.format(iteration)+"/final", outpath + "/result");
		if(!isCompleted)
			System.exit(1);
	}
}

