# OCaml Distributed Data Processing

A distributed data processing system developed in OCaml

# Todo

Architecture: study KV store and PUB/SUB

How to express arbitrary DGA? How to express loop?

Need to separate the shuffling from Context as a module/function

Interface to Irmin or HDFS to provide persistent storage

Imply apply function then test

Test delay-bounded and error-bounded barrier

Need to read "transferring files" in ZMQ doc to remove high water marks in the code

# How to compile & run it?

To compile and build the system, you do not have to install all the software yourself. You can simply pull a ready-made container to set up development environment.

```bash
docker pull ryanrhymes/omap
```

Then you can start the container by

```bash
docker run -t -i ryanrhymes/omap:latest /bin/bash
```

After the container starts, go to the home director, clone the git repository.

```bash
git clone https://github.com/ryanrhymes/actor.git
```

Then you can compile and build the system.

```bash
make oasis && make
```
