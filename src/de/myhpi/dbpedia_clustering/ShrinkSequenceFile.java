package de.myhpi.dbpedia_clustering;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class ShrinkSequenceFile {
	private String inputName;
	private int divider;
	private SequenceFileHandler handler;
	
	public ShrinkSequenceFile() throws Exception {
		this.handler = new SequenceFileHandler();
	}
	
	public void setDivider(String divider) {
		this.divider = Integer.parseInt(divider);
	}

	public int getDivider() {
		return divider;
	}

	public void setInputName(String inputFile) {
		this.inputName = inputFile;
	}

	public String getInputname() {
		return inputName;
	}
	
	public String getOutputName() throws IOException {
		return this.getInputname()+"_"+(handler.getReaderSize(this.getInputname()) / this.getDivider());
	}
	
	protected void execute() throws IOException {
		SequenceFile.Reader input = handler.openReader(this.getInputname());
		SequenceFile.Writer output = handler.openWriter(this.getOutputName());
		Text key = new Text();
		BytesWritable value = new BytesWritable();
		int i = 0;
		
		System.out.println("begin shrink");
		while (input.next(key, value)) {
			if (i % this.getDivider() == 0) {
				output.append(key, value);
			}
			i++;
		}
		output.close();
		System.out.println("end shrink");
	}
	
	public static void main(String[] args) {
		try {
			if (args.length != 2) {
				System.err.println("use parameters: <input> <divider>");
			} else {
				ShrinkSequenceFile me = new ShrinkSequenceFile();
				me.setInputName(args[0]);
				me.setDivider(args[1]);
				me.execute();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
