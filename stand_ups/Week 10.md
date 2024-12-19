1. Last Week:

I’ve been working on setting up other Airflow tasks. My code structure was really simple before —just individual functions without much organization. To make it more organized and easier to be added into airflow, I restructured everything into classes for different parts, like API requests, the Spark session, and the Cassandra connection. Now, each part is isolated, and it keeps everything more secure, especially when handling credentials and kafka or cassandra settings.

During this process, I did have an issue, though. When I first tried to run a task, it failed. After debugging, I found out I’d forgotten to add environment variables to the Docker image. Even though I fixed this issue and the task can be operated successfully, now I’m considering a better way to store those variables, maybe by using Airflow variables instead of a .env file

1. Next week:

I’ll keep working on Airflow, focusing on optimizing both the workflow and my code. It’ll probably take a few more weeks, but once it’s done, my whole data pipeline setup will finally be complete.