The Logs files are constantly pulling live data from different platforms, while the Alert file keeps an eye on that data and sends out alerts to a specific API. 
The log script saves everything in Parquet format, which the alert script reads to figure out what needs attention. 
It’s all running in a loop, with the timing controlled by a setting in the .env file—so it checks in at regular intervals, just like clockwork.
