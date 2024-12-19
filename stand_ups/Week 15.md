1. What I have done in the past week:
- Committed the current code for the stock analysis project to my GitHub repository
- Archived the stand-ups in the GitHub repository under the "stand-up" folder of the stock analysis project
- Adjusted DAG tasks and added "pyspark transformation" to execute after "data extraction"
- Researched ways to run the script continuously
1. Challenges encountered in the past week:
Same as the last stand-up: Even though I added the new DAG, the data stream remains empty because the "data extraction" task runs only once a day. I addressed this by changing the schedule to every minute, but this makes it difficult to manage the running logs and histories. To optimize this, Kubernetes deployment is necessary.
2. What I will do in the next week:
- Reconsider the construction of Airflow DAGs and the procedures and relationships of all tasks
- Learn Kubernetes and try to deploy the data extraction part to a Kubernetes container
- Test the current data flow and try to connect the Athena Database with Grafana
- Complete the final research paper