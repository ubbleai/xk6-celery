import celery from 'k6/x/celery';

export default function () {

  const redisUrl = "redis://127.0.0.1:6379/0";
  const sentinelAddrs = ["host1:26379", "host2:26379"]
  const celeryQueue = "realtime";
  const masterName = "default-master"
  const client = new celery.Redis({
    url: redisUrl,
    sentinelAddrs:  sentinelAddrs,
    queue: celeryQueue,
    mastername:masterName,
    timeout: "10s",
    getinterval: "100ms",
  });

  const taskName = "worker.fake_load_task"
  const taskArg1 = 4000

  // Submit new task
  const taskID = client.delay(taskName, taskArg1);
  console.log(`Sumitted new celery task = ${taskName} ${taskID}`);

  // get task status (non blocking)
  let completed = client.taskCompleted(taskID);
  console.log(`Task completed = ${completed}`);

  // wait for task result to be filled (blocking)
  // fails run if task is not completed after specified client timeout
  completed = client.waitForTaskCompleted(taskID);
  console.log(`Task completed = ${completed}`);

  // This call should timeout and always return false since task does not exist
  completed = client.waitForTaskCompleted("non-existing-task");
  console.log(`Task completed = ${completed}`);
}
