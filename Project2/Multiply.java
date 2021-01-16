import java.io.*;
import java.util.*;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;

class Elem implements Writable {
	public short tag;
	public int index;
	public double value;
	
	Elem () {}
	
	Elem( short tag, int index, double value){
		this.tag = tag;
		this.index = index;
		this.value = value;
		
	}
	
	 public void write ( DataOutput out ) throws IOException {
	        out.writeShort(tag);
	        out.writeInt(index);
	        out.writeDouble(value);
	    }
	 
	 public void readFields ( DataInput in ) throws IOException {
	        tag = in.readShort();
	        index = in.readInt();
	        value = in.readDouble();
	    }
}

class Pair implements WritableComparable<Pair> {
	public int i;
	public int j;
	
	Pair () {}
	
	Pair( int i, int j){
		this.i = i;
		this.j = j;
		
	}
	
	 public void write ( DataOutput out ) throws IOException {
	        out.writeInt(i);
	        out.writeInt(j);
	    }
	 
	 public void readFields ( DataInput in ) throws IOException {
	        i = in.readInt();
	        j = in.readInt();
	    }
	 public int compareTo(Pair p) {
		if (i > p.i) {
			return 1;
		} else if(i<p.i) {
			return -1;
		}else {
			if (j > p.j) {
				return 1;
			}else if(j < p.j) {
				return -1;
			}
		}
		return 0;
	}
	
	 public String toString() {
		 return "" + i + "\t" + j;
	 }
}
public class Multiply {
	public static class MapperM extends Mapper<Object, Text, LongWritable, Elem> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int i = s.nextInt();
			long j = s.nextLong();
			double v = s.nextDouble();
			context.write(new LongWritable(j), new Elem((short)0,i,v));
			s.close();

		}
	}

	public static class MapperN extends Mapper<Object,Text,LongWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
        	Scanner s = new Scanner(value.toString()).useDelimiter(",");
        	long i = s.nextLong();
        	int j = s.nextInt();
        	double v = s.nextDouble();
        	context.write(new LongWritable(i), new Elem((short)1,j,v));
        	s.close();
            	
        }
	}
	
	public static class ReducerMN extends Reducer<LongWritable, Elem, Pair, DoubleWritable> {
		static Vector<Elem> A = new Vector<Elem>();
        static Vector<Elem> B = new Vector<Elem>();
		@Override
		public void reduce(LongWritable index, Iterable<Elem> values, Context context)
				throws IOException, InterruptedException {
			
			A.clear();
			B.clear();
			
			Configuration conf = context.getConfiguration();

			for (Elem v : values) {
				Elem newElem = ReflectionUtils.newInstance(Elem.class, conf);
				ReflectionUtils.copy(conf,v , newElem);
				if (newElem.tag == 0) {
					A.add(newElem);

				} else if (newElem.tag == 1) {
					B.add(newElem);
				}
			};
			
			for (Elem a : A)
				for (Elem b : B)
					context.write(new Pair(a.index, b.index), new DoubleWritable(a.value * b.value));
			
			
		}
	}
	public static class FinalMapper extends Mapper<Object, Text, Pair, DoubleWritable> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter("\t");
			int a = s.nextInt();
			int b = s.nextInt();
			double c = s.nextDouble();
			context.write(new Pair(a, b), new DoubleWritable(c));
			s.close();

		}
	}

	public static class FinalReducer extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {

		@Override
		public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			double m = 0;
			for (DoubleWritable v : values) {
				m = m + v.get();
			}
			;
			context.write(pair, new DoubleWritable(m));
		}
	}
	
	

    public static void main ( String[] args ) throws Exception {
	   Job job1 = Job.getInstance();
		job1.setJobName("Job1");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setInputFormatClass(TextInputFormat.class);
	job1.setReducerClass(ReducerMN.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MapperM.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,MapperN.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);

	Job job2 = Job.getInstance();
		job2.setJobName("Job2");
		job2.setJarByClass(Multiply.class);
		job2.setOutputKeyClass(Pair.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Pair.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setMapperClass(FinalMapper.class);
		job2.setReducerClass(FinalReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[2]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.waitForCompletion(true);
        
       
    }
}
