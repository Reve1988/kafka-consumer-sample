package kr.revelope.kafka.consumer.sample;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerSample {
	public static void main(String[] args) {
//		 defaultConsume();
		 manualSyncCommit();
		// manualAsyncCommit();
		// commitEachRecord();
	}

	private static void defaultConsume() {
	/*
	1. Subscribe
	 - 토픽 구독을 요청하여 파티션을 할당받는다.
	 - 이 함수를 여러번 요청한다고 구독하는 토픽이 늘어나는것은 아니다. (이전에 구독한 모든 구독을 취소하고 새로 요청한 토픽 구독을 요청)
	 - 따라서, 한번에 구독을 해야한다.
	 */
		KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(createConsumerProperties());
		consumer.subscribe(Collections.singletonList("cpu-usage"));
		// regexp를 이용하여 패턴으로 구독가능
		// consumer.subscribe(Collections.singletonList("*-usage"));

		/*
		2. Polling Loop
		 - 실제로 데이터를 받아서 처리하는 무한루프
		 */
		try {
			while (true) {
				// Duration은 읽을 데이터가 컨슈머 버퍼에 없을 때 fetch(브로커로부터 받아옴)를 기다릴 시간
				// Duration이 끝나도 버퍼에 데이터가 없으면 빈 레코드 리스트를 반환한다.
				ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, Double> record : records) {
					System.out.println(String.format("[%s-%d/%d]%f",
						record.topic(),
						record.partition(),
						record.offset(),
						record.value()
					));
				}
			}
		} finally {
			// try with resource 문을 사용해도 됨
			consumer.close();
		}
	}

	private static void manualSyncCommit() {
		KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(createConsumerManualCommitProperties());
		consumer.subscribe(Collections.singletonList("cpu-usage"));

		try {
			while (true) {
				ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, Double> record : records) {
					System.out.println(String.format("[%s-%d/%d]%f",
						record.topic(),
						record.partition(),
						record.offset(),
						record.value()
					));
				}

				try {
					// polling한 마지막 오프셋이 커밋된다.
					// 메세지를 처리하는 도중에 커밋해버리면 장애 발생시 유실이 발생할 수 있다.
					// 커밋될 때 까지 대기
					consumer.commitSync();
				} catch (CommitFailedException e) {
					e.printStackTrace();
				}
			}
		} finally {
			// try with resource 문을 사용해도 됨
			consumer.close();
		}
	}

	private static void manualAsyncCommit() {
		KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(createConsumerManualCommitProperties());
		consumer.subscribe(Collections.singletonList("cpu-usage"));


		try {
			while (true) {
				ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, Double> record : records) {
					System.out.println(String.format("[%s-%d/%d]%f",
						record.topic(),
						record.partition(),
						record.offset(),
						record.value()
					));
				}

				consumer.commitAsync((offsets, exception) -> exception.printStackTrace());
			}
		} finally {
			try {
				// 최종적으로 마지막 커밋을 한다.
				// 처리되는 중간에 프로세스가 종료될 수 있기 때문
				// 이때는 sync로 호출
				consumer.commitSync();
			} catch (CommitFailedException e) {
				e.printStackTrace();
			}
			consumer.close();
		}
	}

	private static void commitEachRecord() {
		// 오프셋을 직접 관리하기 위한 map이 하나 필요하다.
		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

		KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(createConsumerManualCommitProperties());
		consumer.subscribe(Collections.singletonList("cpu-usage"), new HandleRebalance());

		try {
			while (true) {
				ConsumerRecords<String, Double> records = consumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, Double> record : records) {
					System.out.println(String.format("[%s-%d/%d]%f",
						record.topic(),
						record.partition(),
						record.offset(),
						record.value()
					));

					// 레코드를 하나 처리할 때 마다 파티션의 오프셋 정보를 바꿔줌
					currentOffsets.put(
						new TopicPartition(record.topic(), record.partition()),
						new OffsetAndMetadata(record.offset() + 1, "no metadata")
					);
					// 오프셋 정보를 커밋
					consumer.commitAsync(currentOffsets, null);
				}
			}
		} finally {
			consumer.close();
		}
	}

	private static void seekOffset() {
		KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(createConsumerManualCommitProperties());
		consumer.subscribe(Collections.singletonList("cpu-usage"), new HandleRebalance());

		// 특정 파티션의 특정 오프셋을 탐색
		// 주로 Rebalance Listener에서 사용.
		List<PartitionInfo> partitionList = consumer.partitionsFor("cpu-usage");
		for (PartitionInfo partitionInfo : partitionList) {
			consumer.seek(new TopicPartition("cpu-usage", partitionInfo.partition()), 1920);
		}
		// ...
	}

	private static Properties createConsumerProperties() {
		Properties properties = new Properties();
		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.DoubleDeserializer");
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, "some-group");

		return properties;
	}

	private static Properties createConsumerManualCommitProperties() {
		Properties properties = createConsumerProperties();
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		return properties;
	}
}
