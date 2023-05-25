# gcp_estate_project

## 부동산 매물 데이터를 활용한 데이터 파이프라인 구축
부동산 일일 매물량과 변화량 관찰을 위해 데이터 수집부터 집계까지의 데이터 파이프라인 구축 프로젝트입니다.


# Data Pipeline Architecture
데이터 파이프라인은 GCP기반의 ELT Process로 구성하였습니다.  
Airflow를 사용하여 데이터 수집부터 집계까지를 자동화하였고 멱등성을 보장하여 재실행에도 중복데이터가 발생하지 않도록 설계하였습니다.

<img src="https://github.com/dbsgh3344/gcp_estate_project/assets/29767578/cca9ac58-2aa1-441d-839b-a4bb050482e5" width="50%" height="50%">

## Data Ingestion
데이터 수집은 네이버 부동산 사이트의 매물 데이터를 이용했습니다. 사이트내 아파트별로 올라온 매물을 확인할 수 있고 매물에 대한 정보 등이 기입되어 있습니다.


