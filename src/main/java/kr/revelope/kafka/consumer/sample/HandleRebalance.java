package kr.revelope.kafka.consumer.sample;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class HandleRebalance implements ConsumerRebalanceListener {
	/**
	 * 리밸런싱이 시작되기 전에, 그리고 컨슈머가 메세지 처리를 중단한 후 실행된다.
	 * 따라서 이곳에서 오프셋을 커밋할 수 있다.
	 */
	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println("onPartitionsRevoked");
	}

	/**
	 * 파티션을 새로 할당받아 컨슘이 진행되기 전에 실행된다.
	 */
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		System.out.println("onPartitionsAssigned");
	}
}
