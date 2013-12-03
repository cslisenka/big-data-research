package by.bsuir.kslisenko.util.handler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Writable;

import by.bsuir.kslisenko.util.ReaderHandler;

public class TextFileOutputReaderHandler<K extends Writable, V extends Writable> implements ReaderHandler<K, V> {
	
	private String outputTextFileName;
	private ReaderHandler<K, V> outputHandler;
	private PrintStream out;
	
	public TextFileOutputReaderHandler(String outputTextFileName) {
		this(outputTextFileName, new SimpleReaderHandler<K, V>());
	}
	
	public TextFileOutputReaderHandler(String outputTextFileName, ReaderHandler<K, V> outputHandler) {
		this.outputTextFileName = outputTextFileName;
		this.outputHandler = outputHandler;
	}	
	
	@Override
	public void before() throws IOException {
		out = new PrintStream(new FileOutputStream(new File(outputTextFileName)));
	}
	
	@Override
	public void read(K key, V value, PrintStream myOut) throws IOException {
		outputHandler.read(key, value, out);
	}
	
	@Override
	public void after() throws IOException {
		out.flush();
		out.close();
	}	
}