package by.kslisenko.example.lmax;

import com.lmax.disruptor.EventHandler;

public class TestHandlerTwo implements EventHandler<MyStruct> {

	public void onEvent(MyStruct event, long sequence, boolean endOfBatch) throws Exception {
		System.out.println("Handler TWO, event: " + event.getMessage() + " secuence: " + sequence);
		Thread.sleep(500);
	}
}