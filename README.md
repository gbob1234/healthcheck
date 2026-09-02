# file-collector

Windows 장비의 수집 디렉터리를 감시해 설정된 확장자의 파일을 S3에 업로드하고,
Kafka에는 파일 메타데이터만 발행한 뒤 원본을 `old` 디렉터리로 이동하는 Java 8 애플리케이션입니다.
별도 Kafka producer가 CNCF CloudEvents 1.0 형식의 헬스체크를 전송합니다.

## 멀티모듈 구조

```text
collector-core   공통 설정, Kafka 보안 설정, 헬스체크, lifecycle
file-collector   파일 감시, SHA-256, S3 업로드, 메타데이터 발행, archive 이동
```

## 처리 순서

```text
장비 PC의 로컬 날짜로 target.base.dir의 {날짜패턴} 치환
→ 완성된 타겟 경로 생성 감지 또는 시작 시 기존 경로 확인
→ 타겟 경로 등록 직후 기존 파일 스캔
→ 확장자/임시파일 필터
→ 파일 크기 안정화 및 open 확인
→ SHA-256 계산
→ S3 업로드
→ Kafka 메타데이터 발행 및 broker ack 대기
→ old 디렉터리로 이동
```

`target.base.dir=D:/Original/{yyyyMMdd}/EQP01`로 설정하고 장비 PC의 현재 날짜가
`2026-09-01`이면 `D:/Original/20260901/EQP01`을 감시합니다. 성공한 파일은
`D:/Original/20260901/EQP01/old`로 이동합니다. 날짜 뒤에 고정된 하위 경로가 있어도 지원하며,
자정 이후 새 타겟 경로도 프로세스 재시작 없이 자동 등록합니다. 타겟 경로는 장비가 만들고
수집기는 필요한 `old` 폴더만 생성합니다.

S3 object key는 `{s3.object.key.prefix}/{원본 파일명}`입니다. 같은 파일명은 의도적으로
동일 key를 덮어씁니다. endpoint와 인증정보는 Kafka 메시지에 포함하지 않습니다.

AWS SDK for Java v2 `2.31.76`을 사용합니다. 작은 파일은 `PutObject`,
`s3.multipart.threshold.bytes` 이상의 파일은 multipart upload를
사용합니다. multipart 실패 시 생성한 upload를 abort합니다.

S3 요청 체크섬은 SHA-256 digest의 Base64 값을 사용합니다. `PutObject`에는 전체 파일 체크섬을,
multipart upload에는 각 part 범위의 체크섬을 `UploadPartRequest.checksumSHA256`과
`CompletedPart.checksumSHA256`에 전달합니다. Kafka 메타데이터의 `checksum`은 Spark에서 다루기
쉽도록 동일한 전체 파일 SHA-256을 hex 문자열로 유지합니다.

Kafka 메시지 예시는 다음과 같습니다.

```json
{
  "schemaVersion": 1,
  "eventId": "85b5...",
  "deviceName": "DEVICE-001",
  "fileName": "GROWING_DIA_1A001_None_None_None.csv",
  "fileType": "CSV",
  "fileSize": 30184,
  "checksumAlgorithm": "SHA-256",
  "checksum": "1b5a...",
  "bucket": "equipment-collection",
  "objectKey": "production/equipment/GROWING_DIA_1A001_None_None_None.csv",
  "eTag": "...",
  "uploadedAt": "2026-08-31T01:02:03Z"
}
```

`eventId`는 `deviceName + bucket + objectKey + checksum`으로 결정적으로 생성합니다.
프로그램 재시작이나 Kafka 재전송으로 같은 이벤트가 중복되면 Spark에서 이 값으로 제거할 수 있습니다.

## 실패 정책

S3와 Kafka 단계는 `file.processing.max.attempts`만큼 재시도합니다. 모두 실패하거나
`old` 이동이 실패하면 fatal 종료합니다. 원본은 수집 디렉터리에 남으므로 운영자가 S3/Kafka를
점검하고 프로그램을 재시작하면 시작 스캔으로 다시 처리됩니다.

S3 성공 후 Kafka가 실패하면 재시작 과정에서 같은 key를 다시 덮어쓸 수 있습니다. Kafka 성공 후
프로세스 종료 또는 이동 실패가 발생하면 메타데이터가 다시 발행될 수 있으므로 소비자는 `eventId`로
멱등 처리해야 합니다. S3, Kafka, 로컬 파일시스템 사이에는 단일 분산 트랜잭션이 없습니다.

Kafka 장애 중에는 종료 헬스 메시지도 발행되지 않을 수 있으므로 모니터링 시스템은 heartbeat
미수신 임계시간으로 프로세스 장애를 감지해야 합니다.

## 주요 설정

- `target.base.dir`: 수집 타겟 전체 경로. `{yyyyMMdd}`처럼 중괄호 안에 날짜 패턴 지정
- `file.archive.directory.name`: 각 날짜 폴더 아래 생성할 archive 폴더명
- `file.watch.root.directory`, `file.watch.date.directory.pattern`, `file.watch.directory`,
  `file.archive.directory`: 하위 호환을 위한 기존 경로 설정
- `file.allowed.extensions`: `jpg,jpeg,png,csv` 형식의 허용 확장자
- `file.processing.max.attempts`, `file.processing.retry.backoff.ms`: S3/Kafka 단계 재시도
- `s3.endpoint`, `s3.region`, `s3.bucket`, `s3.object.key.prefix`: S3 목적지
- `s3.path.style.access.enabled`: `true`이면 Dell S3 compatible storage에 필요한 path-style 요청 사용
- `s3.tls.verify`: HTTPS 인증서 체인과 hostname 검증 여부
- `s3.multipart.threshold.bytes`, `s3.multipart.part.size.bytes`: multipart 전환 기준과 part 크기
- `file.kafka.*`: 파일 메타데이터 producer 설정
- `health.*`, `health.kafka.*`: heartbeat와 헬스 producer 설정

`s3.access.key`와 `s3.secret.key`를 비우면 AWS SDK default credentials provider chain을 사용합니다.
설정 파일에 직접 자격증명을 넣을 경우 파일 ACL을 제한해야 하며, 애플리케이션 로그에는 자격증명이
출력되지 않습니다. S3 권한은 지정 bucket/prefix의 업로드와 multipart abort에 필요한 최소 권한만
부여하는 것을 권장합니다.
Java properties에서는 따옴표가 값의 일부이므로 access key와 secret key를 큰따옴표로 감싸면 안 됩니다.

```properties
s3.access.key=ACCESS_KEY
s3.secret.key=SECRET_KEY
```

Dell 내부 endpoint에서 `s3.tls.verify=false`로 설정하면 HTTPS/TLS 암호화는 유지하지만
`SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES=true`가 적용되어 인증서 체인과 hostname을
검증하지 않습니다. 중간자 공격을 탐지하지 못하므로 승인된 폐쇄망 endpoint에서만 사용해야 합니다.

## 빌드와 실행

```bat
gradlew.bat clean test
gradlew.bat :file-collector:build
java -jar file-collector\build\libs\file-collector-2.0-SNAPSHOT.jar .\config.properties
```

Windows `.properties` 파일의 경로는 `D:/Original/{yyyyMMdd}/EQP01`처럼 `/`를 사용하거나
백슬래시를 `D:\\Original\\{yyyyMMdd}\\EQP01`처럼 두 번 써야 합니다. 날짜 자리표시자는 정확히
하나만 허용하며 `yyyyMMdd`, `yyyy-MM-dd` 같은 Java `DateTimeFormatter` 패턴을 사용합니다.

실행 전 날짜 자리표시자 앞의 고정 경로가 존재해야 합니다. 날짜가 포함된 최종 타겟 경로는 장비가
만들며 archive 경로는 첫 파일의 S3·Kafka 처리가 성공한 후 이동 직전에 생성합니다.
fat JAR은 실행에 필요한 의존성을 포함합니다.
