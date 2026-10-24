# Dimension CI/CD

## 실행 흐름

- `main` 대상 PR: Python 검사·단위 테스트·배포 실패/복원 테스트와 amd64/arm64 컨테이너 검증.
- `main` push 또는 `Dimension Deploy` 수동 실행: 같은 CI를 통과한 뒤 GHCR에 `sha-<커밋 SHA>` 태그로 다중 아키텍처 이미지를 발행한다.
- 발행된 digest를 다시 받아 컨테이너 검증 후, 저장소 변수 `DEPLOY_ENABLED=true`일 때만 production 서버에 SSH 배포한다.
- `Dimension Rollback`은 `main`에서 수동 실행하며 직전 성공 버전으로 복원한다.

기존 client/server와 동일한 GitHub Actions → GHCR → SSH/Compose 방식이다.
Action은 커밋 SHA로 고정하고 Dependabot이 갱신 PR을 만든다. PR 검증에는 배포 비밀값을 쓰지 않는다.
이미지는 `linux/amd64`, `linux/arm64`를 발행하며 PR 컨테이너 검증은 각 아키텍처의 네이티브 GitHub 러너에서 실행한다.
CI에는 재배포 불가 모델이 없으므로 측정 함수만 고정 응답으로 대체하고, 실제 Kafka에서 Worker 기동·준비 상태·정상 결과·이미지 오류 결과 왕복을 검증한다. 실제 모델 로딩은 운영 교체 전 읽기 전용 사전 검사에서 확인한다.

## 서비스와 데이터

운영 배포 대상은 Spring과 연결된 Kafka 서비스 `dl-worker`다. HTTP `/measure` 서버는 별도로 실행할 수 있다.
모델 파일은 이미지에 넣지 않고 기존 `/app/app/data` 마운트, `/images` 공유 볼륨, Hugging Face 캐시를 유지한다.
배포 전 후보 이미지로 서버의 실제 모델·캐시를 읽기 전용으로 마운트해 오프라인 로딩을 확인한다.

Docker healthcheck는 모델 준비 후 Kafka 파티션을 할당받은 poll loop의 heartbeat를 검사한다.
배포 후에는 해당 컨테이너 IP의 consumer가 요청 토픽 파티션을 할당받았는지도 확인한다.
이 검사는 과거 이미지로 롤백할 때도 동작하며 운영 토픽에 시험 메시지를 넣지 않는다.
종료 신호는 기존 처리 흐름대로 현재 요청 처리를 마치고 consumer를 닫는다. `stop_grace_period: 60s`를 유지한다.


## 서버 설정

확인된 서버는 SSH 별칭 `choi`, 실제 주소 `apple@chagok.cloud`의 macOS ARM64/Docker Desktop이다.
통합 프로젝트는 `/Users/apple/deploy/chagok-migration-20261021/chagok`, Compose 프로젝트명은 `chagok-stack`이다.
예시는 기존 `compose.yaml`, `compose.production.yaml`, `.env`, 내부 주소 `http://127.0.0.1:18081`, 공개 주소 `https://chagok.cloud`를 사용한다.

`deploy/dimension-deploy.conf.example`을 다음 위치에 설치한다.

```sh
scp deploy/dimension-deploy.conf.example \
  choi:/Users/apple/deploy/chagok-migration-20261021/dimension-deploy.conf
```

설정은 신뢰하는 관리자만 수정하는 Bash 파일이다. 서버에서 권한을 `600`으로 설정한다.
Docker 인증은 기존 `/Users/apple/.docker-chagok`와 Docker Desktop 소켓을 사용한다.
private GHCR 이미지는 기존 서버 계정에 `read:packages` 인증이 필요하다.

다른 저장소가 선택한 이미지 override를 모두 포함해야 이후 배포와 수동 Compose 작업에서도 버전이 유지된다.
새 설정 예시는 기존 frontend/backend와 상대 AI 서비스의 override를 자동으로 포함한다.
기존 `client-deploy.conf`, `server-deploy.conf`에도 다음 파일이 존재할 때 `COMPOSE_FILES`에 추가한다.

```sh
for selection in .agent-deploy/agent.image.yaml .dimension-deploy/worker.image.yaml; do
  if [ -f "$COMPOSE_PROJECT_DIRECTORY/$selection" ]; then
    COMPOSE_FILES+=("$COMPOSE_PROJECT_DIRECTORY/$selection")
  fi
done
```

네 서비스 배포는 기존 `.client-deploy/deploy.lock`을 공유한다.
겹친 배포는 잠금 오류로 실패하므로 앞 배포가 끝난 뒤 다시 실행한다.

## GitHub 설정

저장소 변수 `DEPLOY_ENABLED`를 서버 검증 전에는 `false`, 준비 후 `true`로 설정한다.
`production` Environment를 만들고 배포 브랜치를 `main`으로 제한한다.

| 구분 | 이름 | 값 |
| --- | --- | --- |
| Variable | `DEPLOY_HOST` | `chagok.cloud` |
| Variable | `DEPLOY_USER` | `apple` |
| Variable | `DEPLOY_PORT` | `22` |
| Variable | `DEPLOY_CONFIG` | `/Users/apple/deploy/chagok-migration-20261021/dimension-deploy.conf` |
| Secret | `DEPLOY_SSH_KEY` | 기존 배포 SSH 개인 키 내용 |
| Secret | `DEPLOY_KNOWN_HOSTS` | 기존에 신뢰 확인한 SSH 호스트 공개 키 |

서버 `.env`, LLM 키, DB 비밀번호, 모델과 촬영 데이터는 GitHub나 이미지로 옮기지 않는다.
GHCR 발행 작업에만 `packages: write`를 부여한다.
main 보호 규칙에는 `Dimension CI`의 `check`, 두 아키텍처 `container` 검사를 필수로 지정할 수 있다.

## 사전 확인과 롤백

서비스 교체 없이 현재 연결을 검사한다.

```sh
bash scripts/deploy-dimension.sh check '' \
  /Users/apple/deploy/chagok-migration-20261021/dimension-deploy.conf
```

서버에서 직접 배포하거나 롤백할 수 있다.

```sh
bash scripts/deploy-dimension.sh deploy \
  'ghcr.io/chagokai/item-dimension-api@sha256:<64자리 digest>' \
  /Users/apple/deploy/chagok-migration-20261021/dimension-deploy.conf
bash scripts/deploy-dimension.sh rollback '' \
  /Users/apple/deploy/chagok-migration-20261021/dimension-deploy.conf
```

`.dimension-deploy`에 `worker.image.yaml`, `current-images.json`, `previous-images.json`, `history.jsonl`을 보관한다.
새 이미지를 먼저 다운로드하고 네이티브 사전 검사를 통과해야 컨테이너를 교체한다.
교체는 대상 서비스만 `--no-deps --no-build --pull never --wait`로 수행한다.
실패하면 이전 실제 이미지별 로컬 태그로 복원하고, 복원에 성공해도 해당 배포는 실패로 기록한다.
다운로드·사전 검사 실패는 실행 중 서비스를 변경하지 않는다. 같은 이미지 재배포는 롤백 이력을 보존한다.
롤백 태그와 상태 디렉터리는 정리 작업으로 지우지 않는다.

## 이번 구성에서 준비한 항목

`production` 환경, main 배포 제한, SSH 키·known_hosts, 서버/계정/설정 경로 변수와 서버 설정 파일을 등록했다.
현재 운영 컨테이너에 대한 읽기 전용 연결 검사와 Worker 후보 이미지의 실제 모델 오프라인 로딩을 통과했다.
기존 client/server 서버 설정에도 두 AI 이미지 override를 포함했다.
파이프라인 코드는 `ci/github-actions` PR로 검증하며, main 병합 시 배포 워크플로가 실행된다.
