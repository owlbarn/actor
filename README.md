# OCaml Distributed Data Processing

A distributed data processing system developed in OCaml

# Todo

How to express arbitrary DGA? How to express loop? Apply function.

Interface to Irmin or HDFS to provide persistent storage

Test delay-bounded and error-bounded barrier

Split Context module into server and client two modules

Implement parameter.mli

Implement barrier control in parameter modules

Rename ... DataContext and ModelContext?

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
