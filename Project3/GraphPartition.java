import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid; 				// the id of the centroid in which this vertex belongs to
    public long size;
	public short depth;               // the BFS depth
    
	Vertex() {}
	
	Vertex( long id, Vector<Long> adjacent, long centroid, short depth){
		this.id = id;
		this.adjacent = adjacent;
		this.centroid = centroid;
		this.depth = depth;
		size = adjacent.size();
		
	}
	public void write(DataOutput out) throws IOException{
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);
		out.writeLong(size);
		for(int i =0;i<size;i++){
			out.writeLong(adjacent.get(i));
		}
	}
	
	public void readFields(DataInput in) throws IOException{
		id = in.readLong();
		centroid = in.readLong();
		depth = in.readShort();
		adjacent = new Vector<Long>();
		size = in.readLong();
		for(int n=0;n<size;n++){
			adjacent.add(in.readLong());
		}
		
		
	}
}
public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
	static int count = 0;

    public static class MapperFirst extends Mapper<Object, Text, LongWritable, Vertex> {
		static Vector<Long> adjacent = new Vector<Long>();
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
			long centroid = 0;
		
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			
		    long id = s.nextLong();				
			while(s.hasNext()){
				adjacent.add(s.nextLong());
				
				
			}
			if(count<10){
				centroid = id;
			}else{
				centroid = -1;
			}
		        context.write(new LongWritable(id), new Vertex(id, adjacent, centroid, (short)0));
			count++;
						
			s.close(); 
	
				
		}
	}
	
	public static class MapperSecond extends Mapper<LongWritable, Vertex, LongWritable, Vertex>{
		static Vector<Long> empty = new Vector<Long>();
		@Override
		public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(vertex.id), vertex);
			
			
			if(vertex.centroid > 0){
				for(long n : vertex.adjacent){
					
					context.write(new LongWritable(n), new Vertex(n, empty, vertex.centroid, BFS_depth));
				
			
			
				}
			}
			
		}
	}
	
	public static class ReducerSecond extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{
		static Vector<Long> empty = new Vector<Long>();
		@Override
		public void reduce(LongWritable id, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
			short min_depth = 1000;
			long a = id.get();
			Vertex m = new Vertex(a, empty, (long)-1, (short)0);
			for(Vertex v : values){
				if(!(v.adjacent.isEmpty())){
					m.adjacent = v.adjacent;
				}
				if(v.centroid > 0 && v.depth < min_depth){
					min_depth = v.depth;
					m.centroid = v.centroid;
				}
			}
			m.depth = min_depth;
			context.write(id, m);
	
			
		}
	}
	
	public static class MapperThird extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>{
		@Override
		public void map(LongWritable id, Vertex value, Context context) throws IOException, InterruptedException {
                        
			context.write(new LongWritable(value.centroid), new LongWritable(1));
			
		}
	}
	
	public static class ReducerThird extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
		@Override
		public void reduce(LongWritable centroid, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long m = 0;
			for(LongWritable v : values){
				m = m + v.get();
			}
			context.write(centroid, new LongWritable(m));
		}
	}
	
	

    public static void main ( String[] args ) throws Exception {			
			
        Job job = Job.getInstance();
        job.setJobName("MyJob");
		  job.setJarByClass(GraphPartition.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(MapperFirst.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
		/*MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapperFirst.class); */
		FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        /* ... First Map-Reduce job to read the graph */
        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
		job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(MapperSecond.class);
        job.setReducerClass(ReducerSecond.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        /*MultipleInputs.addInputPath(job, new Path(args[1]+"/i0"), SequenceFileInputFormat.class, MapperSecond.class);*/
		FileInputFormat.setInputPaths(job,new Path(args[1]+"/i0"));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i"+(i+1)));
            /* ... Second Map-Reduce job to do BFS */
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
		job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(MapperThird.class);
        job.setReducerClass(ReducerThird.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        /*MultipleInputs.addInputPath(job, new Path(args[1]+"/i8"), SequenceFileInputFormat.class, MapperThird.class); */
        FileInputFormat.setInputPaths(job,new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.waitForCompletion(true);
    }
}
