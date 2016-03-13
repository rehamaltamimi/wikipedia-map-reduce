# Running Time #
Depending on what jobs you are running the time taken can vary wildly.
  * **CitationCounter** - This job took around 6 hours on 20 extra-large high memory machines on AWS.
  * **CiteDomainCounter** - This job took under 15 minutes on a cluster of about 15 machines at Macalester College

# Arguments #
These arguments are important for stage 1 jobs that process the full revision text.  For smaller jobs, such as jobs that process the output from stage 1, they may not be important.

Arguments beyond the input and output directories can be specified as follows: -D setting=value

Some commonly used arguments to set are as follows:
  * **mapred.task.timeout** - Number of milliseconds until a task will timeout unless it reads input, writes output or updates status. When using the full revision history text as input, we've had some serious problems with tasks timing out without a good reason, and have been setting this to one hour (3600000) to mitigate that problem
  * **mapred.child.java.opts** - Java options for tasks. When using the full revision history text as input, we've been setting the heap space significantly higher than the default (200m), as much as 3072m or so, though that may be a little excessive.  Multiple options can be passed here.
  * **mapred.tasktracker.map.tasks.maximum** - The maximum number of map tasks per computer.
  * **mapred.tasktracker.reduce.tasks.maximum** - The maximum number of map tasks per computer.
  * **mapred.map.tasks** - The number of map tasks for the job.
  * **mapred.reduce.tasks** - The number of reduce tasks for the job.