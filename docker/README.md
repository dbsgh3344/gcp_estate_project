## container 실행 시
flower 설치 시 profile option으로 flower 추가

```shell
docker compose --profile flower up
```
<br><br>

## container 실행 후 추가사항
1. ssh 추가  
    생성된 worker 2개 중 하나로 crawling 결과 파일을 전송받아서 merge하기 때문에 ssh 설정 필요  
    openssh install하여 ssh port 개방  

    ```shell
    apt-get update
    apt-get install openssh-server
    service ssh start
    ```
2. worker public key 추가  
    authorized_keys 값에 remote worker들의 public key 값 추가  

