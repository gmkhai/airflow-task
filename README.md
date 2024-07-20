**Documentation**

**STEP**

1. for running this project you can run you must running this command for initialization and migrate data for acccess airflow
```
docker compose up airflow-init
```

2. running this command for create container

```
docker compose up
```

3. please you must install `openpyxl` in each container airflow
    - you can access docker container bash using command
    ```
    docker container exec -it <image id> bash
    ```
    - you can install `openpyxl` using
    ```
   pip install openpyxl
    ```
4. you can running airflow at your browser using `127.0.0.1:8080`


**Note:**
 for visualization i am not using apache superset because i have problem when build image docker. For alternative i am using power BI for create visualization from postgresql