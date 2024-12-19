Standup:

1. Apache Cassendra

It works perfect and I successfully connect it with my spark streaming.

1. Go back to the data extraction part

I found something wrong after I load the data into my database. So each time I request the API, I use the current time and the stock market closing time of the same day. It works fine during the extraction process. But during the weekends, when the stock market closes, the request will have some errors when producing the kafka msg.

I need to reconsider all the scenarios I might encounter, and use different checking functions to print out the errors.

1. The plan for next week

So the plan for next week is to stop moving forward for a week, go back to the completed parts, and reconsider the scenarios such as when stock market is closing, and adding some error handling functions.