# ray-ease

This package is a convenient Ray wrapper that enables the utilization of Ray decorated functions and actors as if they were regular local functions. With this tool, your program can seamlessly run in both parallel and serial modes without requiring any code modifications. This capability is particularly advantageous during the debugging phase, as parallelizing code may inadvertently introduce unnecessary complexities and overhead.

## Installation

```bash
$ pip install ray-ease
```

## Usage

Effortlessly parallelize your code by simply decorating your functions or classes with the `parallelize` decorator. Retrieve the results using the `retrieve_parallel_loop` function. This enables you to parallelize your code with Ray if it's been explicitly initialized or run it serially without any overhead from Ray.

> [!NOTE]  
> In order to correctly decorate functions and classes when parallelizing, `ray.init(...)` should declared in your script before defining any of the decorated functions or classes. We recommend you to use a environment variable to initialize Ray or not when needed.

### Running a Task

```Python
import ray
from ray_ease import parallelize, retrieve_parallel_loop

ray.init()

# Define the square task.
@parallelize
def square(x):
    return x * x

# Launch four parallel square tasks.
futures = [square(i) for i in range(4)]

# Retrieve results.
print(retrieve_parallel_loop(futures))
# -> [0, 1, 4, 9]
```

See [Ray version](https://docs.ray.io/en/latest/ray-core/walkthrough.html#running-a-task) for comparison.

### Calling an Actor

```Python
import ray
from ray_ease import parallelize, retrieve_parallel_loop

ray.init()

# Define the Counter actor.
@parallelize
class Counter:
    def __init__(self):
        self.i = 0

    def get(self):
        return self.i

    def incr(self, value):
        self.i += value

# Create a Counter actor.
c = Counter()

# Submit calls to the actor. These calls run asynchronously but in
# submission order on the remote actor process.
for _ in range(10):
    c.incr(1)

# Retrieve final actor state.
print(retrieve_parallel_loop(c.get()))
# -> 10
```

See [Ray version](https://docs.ray.io/en/latest/ray-core/walkthrough.html#calling-an-actor) for comparison.

### Comparison with and without Ray

Parallel computation with Ray (see [base example](https://docs.ray.io/en/latest/ray-core/tips-for-first-time.html#tip-1-delay-ray-get)):

```Python
import ray
import time
from ray_ease import parallelize, retrieve_parallel_loop

ray.init(num_cpus=4) # Initialize Ray and specify this system has 4 CPUs.

@parallelize
def do_some_work(x):
    time.sleep(1) # Replace this with work you need to do.
    return x

start = time.time()
results = retrieve_parallel_loop([do_some_work(x) for x in range(4)])
print("duration =", time.time() - start)
print("results =", results)
```

This yields the following output:

```
duration = 1.0233514308929443
results =  [0, 1, 2, 3]
```

As opposed to serial computation, by commenting `import ray` and `ray.init(num_cpus=4)` lines:

```Python
# import ray
import time
from ray_ease import parallelize, retrieve_parallel_loop

# ray.init(num_cpus=4) # Initialize Ray and specify this system has 4 CPUs.

@parallelize
def do_some_work(x):
    time.sleep(1) # Replace this with work you need to do.
    return x

start = time.time()
results = retrieve_parallel_loop([do_some_work(x) for x in range(4)])
print("duration =", time.time() - start)
print("results =", results)
```

The outputs provide confirmation that the execution was carried out sequentially, taking approximately four times longer than before:

```
duration = 4.021065711975098
results =  [0, 1, 2, 3]
```

<p align="right">(<a href="#ray-ease">back to top</a>)</p>

## Contributing

Interested in contributing? Check out the contributing guidelines. Please note that this project is released with a Code of Conduct. By contributing to this project, you agree to abide by its terms.

<p align="right">(<a href="#ray-ease">back to top</a>)</p>

## License

`ray-ease` was created by Arthur Elskens. It is licensed under the terms of the MIT license. See LICENSE for more information.

<p align="right">(<a href="#ray-ease">back to top</a>)</p>

## Useful links

Repository: [https://github.com/aelskens/ray-ease](https://github.com/aelskens/ray-ease)

<p align="right">(<a href="#ray-ease">back to top</a>)</p>