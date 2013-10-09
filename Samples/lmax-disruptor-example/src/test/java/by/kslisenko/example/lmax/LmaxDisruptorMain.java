package by.kslisenko.example.lmax;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SingleThreadedClaimStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class LmaxDisruptorMain {

	public static void main(String[] args) {
		int ringSize = 16;
		ExecutorService executor = Executors.newCachedThreadPool();
		MyEventFactory factory = new MyEventFactory();
		Disruptor<MyStruct> disruptor = new Disruptor<MyStruct>(factory, executor, new SingleThreadedClaimStrategy(ringSize), new YieldingWaitStrategy());
		disruptor.handleExceptionsWith(new MyExceptionHandler());
//		disruptor.handleEventsWith(new TestHandlerOne()).then(new TestHandlerTwo());
		disruptor.handleEventsWith(new TestHandlerOne(), new TestHandlerTwo());
		
		RingBuffer<MyStruct> ringBuffer = disruptor.start();
		for (int i = 0; i < 10; i++) {
			long sequence = ringBuffer.next();
			System.out.println("sequence: " + sequence);
			MyStruct event = ringBuffer.get(sequence);
			event.setMessage("event " + i);
			ringBuffer.publish(sequence);			
		}

		disruptor.shutdown();
		executor.shutdown();
	}
}