# ray-ease

<div align="left">
    <a href="https://github.com/aelskens/ray-ease">Home</a>
    ·
    <a href="https://github.com/aelskens/ray-ease/issues">Report Bug</a>
    ·
    <a href="https://github.com/aelskens/ray-ease/issues">Request Feature</a>
    ·
    <a href="https://github.com/aelskens/ray-ease/tree/main/examples">Useful Examples</a>
    ·
    <a href="https://github.com/aelskens/ray-ease/blob/main/LICENSE">License</a>
    <br />
    Switch from serial to parallel computing without requiring any code modifications.
</div>

## About the project

This package is a convenient [Ray](https://www.ray.io) wrapper that enables the utilization of Ray decorated functions and actors as if they were regular local functions. With this tool, your program can seamlessly run in both parallel and serial modes without requiring any code modifications. This capability is particularly **advantageous during the debugging phase**, as parallelizing code may inadvertently introduce unnecessary complexities and overhead.

## Installation

```bash
$ pip install ray-ease
```

## Usage

Effortlessly parallelize your code by simply decorating your functions or classes with the `parallelize` decorator. Retrieve the results using the `retrieve` function. This enables you to parallelize your code with Ray, default behavior of the `init` function, or run it serially without any overhead from Ray with `rez.init(config="serial")`.

> [!NOTE]  
> To explore additional useful examples, please refer to the [dedicated folder](https://github.com/aelskens/ray-ease/tree/main/examples) designated for this purpose.

### Running a Task

```Python
import ray_ease as rez

rez.init()

# Define the square task.
@rez.parallelize
def square(x):
    return x * x

# Launch four parallel square tasks.
futures = [square(i) for i in range(4)]

# Retrieve results.
print(rez.retrieve(futures))
# -> [0, 1, 4, 9]
```

See [Ray version](https://docs.ray.io/en/latest/ray-core/walkthrough.html#running-a-task) for comparison.

### Calling an Actor

```Python
import ray_ease as rez

rez.init()

# Define the Counter actor.
@rez.parallelize
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
print(rez.retrieve(c.get()))
# -> 10
```

See [Ray version](https://docs.ray.io/en/latest/ray-core/walkthrough.html#calling-an-actor) for comparison.

### Comparison with and without Ray

Parallel computation with Ray (see [base example](https://docs.ray.io/en/latest/ray-core/tips-for-first-time.html#tip-1-delay-ray-get)):

```Python
import time
import ray_ease as rez

rez.init(num_cpus=4) # Initialize Ray and specify this system has 4 CPUs.

@rez.parallelize
def do_some_work(x):
    time.sleep(1) # Replace this with work you need to do.
    return x

start = time.time()
results = rez.retrieve([do_some_work(x) for x in range(4)])
print("duration =", time.time() - start)
print("results =", results)
```

This yields the following output:

```
duration = 1.0233514308929443
results =  [0, 1, 2, 3]
```

As opposed to serial computation, obtained by specifying to `ray_ease` to use the *serial config* with `rez.init("serial")`:

```Python
import time
import ray_ease as rez

rez.init("serial")

@rez.parallelize
def do_some_work(x):
    time.sleep(1) # Replace this with work you need to do.
    return x

start = time.time()
results = rez.retrieve([do_some_work(x) for x in range(4)])
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