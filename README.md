# IMAGE_PRODUCER

Windows 장비의 이미지 디렉터리를 장기간 감시하고, 완성된 이미지 파일을 Base64 JSON으로 Kafka에 비동기 발행하며, 별도 Kafka producer로 CNCF CloudEvents 1.0 헬스체크를 전송하는 Java 8 애플리케이션입니다.

## 구조와 처리 흐름

`ApplicationMain`이 설정과 객체를 조립합니다. `ConfigLoader`는 설정을 immutable 책임 객체로 변환하고 검증하며, `KafkaPropertiesFactory`는 각 producer에 새로운 `Properties`를 만듭니다.

이미지 흐름은 다음과 같습니다.

```text
ENTRY_CREATE → 확장자/임시파일 필터 → 중복 경로 차단 → worker executor
→ 파일 크기 안정화 및 open 확인 → 크기 제한 → Base64 → JSON
→ 이미지 전용 KafkaProducer.send(callback) → 성공 시 원본 삭제
```

헬스 흐름은 다음과 같습니다.

```text
single-thread scheduleWithFixedDelay → 상태 snapshot → 상태 평가
→ 샘플 기반 CloudEvent → 헬스 전용 KafkaProducer.send(callback)
→ 성공/실패 상태 반영 → 임계값 도달 시 producer 복구 → 실패 시 fatal 종료
```

이미지와 헬스 producer는 인스턴스, callback 상태, 실패 카운터, scheduler, close 책임을 공유하지 않습니다. Kafka 연결·보안 값만 공통 설정에서 읽습니다.

## 헬스체크 샘플 반영

샘플의 CloudEvents envelope(`specversion`, `type`, `source`, `id`, `time`, `subject`, `datacontenttype`, `dataschema`, `data`)와 `sourceInfo`, `status`, `hearbeat`, `kafkaInfo`, `workInfo`, `errorInfo`를 유지합니다. 기존 소비자 호환성을 위해 샘플의 `hearbeat` 오타도 유지했습니다.

다음 표준 위반 예시는 보정했습니다.

- `time`은 `2024-...Z+09:00` 같은 이중 offset 대신 UTC RFC 3339 형식으로 만듭니다.
- `dataschema`는 URI여야 하므로 기본값을 `urn:company:schema:health-status:1.0`으로 사용합니다.
- 상태 level은 요구사항의 `UP`, `WARN`, `DOWN`, `UNKNOWN`을 사용합니다.

## 빌드와 실행

JDK 8 이상에서 Wrapper를 사용합니다. 빌드 결과는 Java 8 bytecode입니다.

```bat
gradlew.bat clean test
gradlew.bat build
java -jar build\libs\IMAGE_PRODUCER-1.1-SNAPSHOT.jar .\config.properties
```

실행 전 `image.watch.directory`가 존재하고 읽기 가능한 디렉터리인지 확인해야 합니다. fat JAR은 모든 runtime 의존성을 포함합니다.

## 설정

루트의 `config.properties`가 전체 예제입니다.

- `application.*`: 이름, 버전, fatal exit code
- `image.watch.*`, `image.allowed.extensions`: 감시 위치와 허용 확장자
- `image.file.stability.*`: 크기 확인 간격, 동일 크기 필요 횟수, timeout
- `image.processing.thread.count`, `image.max.file.size.bytes`: 작업 풀과 메모리 상한
- `kafka.bootstrap.servers`, `kafka.security.mode`: 공통 연결 설정
- `image.kafka.*`: 이미지 topic과 producer delivery 설정
- `health.*`: 주기, 초기화 재시도, 장애 임계값, 복구 및 종료 timeout
- `health.kafka.*`: 짧은 block timeout을 사용하는 헬스 producer 설정
- `health.device/system/program/event.*`: CloudEvent 식별자

### PLAINTEXT

```properties
kafka.security.mode=PLAINTEXT
kafka.bootstrap.servers=broker1:9092,broker2:9092
```

### SSL

```properties
kafka.security.mode=SSL
kafka.ssl.truststore.location=C:/kafka/certs/client.truststore.jks
kafka.ssl.truststore.password=changeit
```

mTLS가 필요한 경우에만 `kafka.ssl.keystore.location`, `kafka.ssl.keystore.password`, `kafka.ssl.key.password`를 모두 설정합니다. truststore는 broker 인증서를 검증하고, keystore는 client 인증서를 제공합니다.

### SASL_SSL

```properties
kafka.security.mode=SASL_SSL
kafka.ssl.truststore.location=C:/kafka/certs/client.truststore.jks
kafka.ssl.truststore.password=changeit
kafka.sasl.mechanism=SCRAM-SHA-512
kafka.sasl.username=health-user
kafka.sasl.password=secret
```

`kafka.sasl.jaas.config`가 비어 있지 않으면 username/password 조합보다 우선합니다. 애플리케이션이 JAAS를 생성할 때 따옴표와 역슬래시를 escape합니다. password, JAAS, truststore/keystore/key password는 설정 요약에 기록하지 않습니다.

## 상태와 장애 정책

이미지 입력이 없다는 사실만으로 장애로 판정하지 않습니다. 최신 처리/발행 결과와 watcher 진행 상태를 구분합니다. 개별 손상·빈 파일·크기 초과·안정화 timeout은 해당 파일만 실패 처리하며 계속 감시합니다.

헬스 producer 생성은 `health.init.max.attempts`만큼 backoff 재시도합니다. `health.required=true`에서 모두 실패하면 감시를 시작하지 않고 종료합니다. 전송 실패는 다음 두 조건이 모두 충족되어야 복구를 시작합니다.

- 연속 실패 수가 `health.send.max.consecutive.failures` 이상
- 마지막 성공(성공 전에는 reporter 시작) 이후 시간이 `health.send.max.failure.duration.seconds` 이상

복구는 producer를 닫고 `health.recovery.max.attempts`만큼 재생성한 뒤 시험 heartbeat를 보냅니다. 재생성까지 실패하면 `HEALTH_KAFKA_UNRECOVERABLE`로 안전하게 자원을 닫고 `health.fatal.exit.code`(기본 20)로 종료합니다. `System.exit()`은 `ApplicationLifecycleManager` 한 곳에서만 호출됩니다.

## 운영 점검과 재기동

fatal 로그의 오류 코드와 원인을 확인하고 다음을 점검한 후 운영 절차에 따라 수동 재기동합니다.

- broker 주소, DNS/방화벽, 이미지/헬스 topic 존재 및 ACL
- 시스템 시간과 인증서 만료일, truststore/keystore 경로·암호
- SASL mechanism, 계정 잠김·암호·ACL
- 감시 디렉터리 접근 권한과 디스크/메모리 여유

INFO는 시작/종료와 적용한 비민감 설정, WARN은 개별 파일·일시 Kafka 오류·복구, ERROR는 설정 오류와 fatal 원인을 기록합니다. Base64 본문과 전체 health JSON은 로그에 남기지 않습니다.

## 버전과 호환성

- Kafka client: 3.9.2
- CNCF CloudEvents core/json-jackson/kafka: 4.1.1
- SLF4J API/simple provider: 2.0.17
- Jackson: 2.21.3
- JUnit Jupiter: 5.10.2

이 조합은 Java 8 bytecode로 컴파일됩니다. Kafka Java client는 API version negotiation으로 broker 2.8.2가 지원하는 protocol을 선택하므로 producer 기본 기능과 호환됩니다. CloudEvents Kafka 모듈의 전이 Kafka client는 Gradle에서 명시한 3.9.2 하나로 정렬합니다.

## 알려진 한계

- producer 생성은 설정·인증서 형식 오류를 즉시 찾지만 실제 broker 연결은 Kafka가 비동기로 수행하므로, 네트워크/ACL 오류는 첫 callback부터 실패 정책에 반영됩니다.
- 처리 완료 후 성공 callback에서 파일을 삭제합니다. 별도 archive가 필요하면 운영 반입 전에 삭제 정책을 확장해야 합니다.
- 파일 내용 자체가 유효한 이미지인지 디코딩하지 않고 확장자·크기·읽기 가능 여부만 검증합니다.
- `healthcheck spec sample`의 `hearbeat` 오타는 소비자 호환성 때문에 유지했습니다.
