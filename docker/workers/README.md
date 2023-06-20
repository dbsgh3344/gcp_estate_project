## 컨테이너 run 실행 시
airflow cluster와 동일한 dags folder를 volume mount  
```shell
docker run -it -d -v <cluster dags path>:<container path> <image name> /bin/bash 
```
<br><br>
## 컨테이너 내부에서 실행  

1. 환경변수 설정  
    미리 아래 환경변수들을 각 설정에 맞게 export  

    AIRFLOW__MASTER__IP : master node ip address   
    GOOGLE_CREDENTIAL : google service key (보통 json file)  
    WEBSERVER_SECRET_KEY : webserver에서 worker node의 log를 보기 위해 필요한 secret key. 임의의 값을 주되 모든 cluster node들에서 값이 일치해야 한다.  
    AIRFLOW_UID : airflow user id값  

2. 공통 환경변수 설정  
    복사한 env_setting 파일을 export  
    (개인 환경 setting에 맞게 custom하게 설정)  
    AIRFLOW__CORE__DAGS_FOLDER : container run시 volume mount한 dags folder의 경로를 넣어준다.  
    ```shell
    export $(cat env_setting.txt | xargs)
    ```  
3. ssh key 생성  
    ssh키 생성 후 master node pc의 authorized_keys에 public key값 복사  

4. celery worker 실행  
    ```shell
    airflow celery worker -q <queue name>
    ```




