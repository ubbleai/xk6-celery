# xk6-celery
A simple Celery tasks submitter/checker extension for k6.
It relies on `github.com/gocelery/gocelery` library & use redis connection pooling at K6 object level.

## Known current limitations
* This extension is only meant to sumbit Celery tasks and (eventually) check task completion.
* This extension does not validate task success, it only checks if the tasks has a result.
* Redis is the only Celery backend currently supported.
* Tasks using kwargs are not supported.

_Also, we do not intend to support & bind all gocelery functions in this project._

## Build

To build a `k6` binary with this extension, first ensure you have the prerequisites:

- [Go toolchain](https://go101.org/article/go-toolchain.html)
- Git
- [xk6](https://github.com/grafana/xk6)

1. Build with `xk6`:

```bash
xk6 build --with github.com/ubbleai/xk6-celery
```

This will result in a `k6` binary in the current directory.

2. Run with the just build `k6:

```bash
./k6 run example.js
```

## Functions

```javascript
import celery from 'k6/x/celery';

// Create a new Celery Redis Client
const client = new celery.Redis({
  url: "redis://127.0.0.1:6379",
  queue: "taskqueue",
});

// Publish a new task with a three positional arguments
// Task id is returned as a string
const taskID = client.delay("my_task", "text-value", 101, ["any", "arg", "type", "allowed"]);

// Check if task have been completed (whether it's a success or not)
// boolean returned
const processed = client.taskCompleted(taskID);
if processed === True {
  console.log("Task processed");
} else {
  console.log("Task still pending");
}

// Wait for task completion using a blocking func call
// boolean returned (returns false if we hit timeout)
const deadlineCompleted = client.waitForTaskCompleted(taskID);
console.log(`Task completed within a timeframe = ${deadlineCompleted}`);
```

### Javascript client configuration
|   JSON Key    |      Default value       |   Description   |
|---------------|--------------------------|-----------------|
| `url`         | "redis://127.0.0.1:6379" | Celery Client endpoint URL (only Redis supported ATM) |
| `queue`       | "celery"                 | Celery queue where to publish tasks |
| `timeout`     | "30s"                    | Timeout used in `waitForTaskCompleted` function |
| `getinterval` | "50ms"                   | Check interval used in `waitForTaskCompleted` function |

example :
```javascript
const client = new celery.Redis({
  "url": "redis://my-redis:6379/0",
  "queue": "worker-queue-1",
  "timeout": "10s",
  "getinterval": "100ms",
});
```

## Future
* add check success functions
* support AMQP
* clean code/tests & add more opts
