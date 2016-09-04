# actor

an actor system in ocaml

# TODO

Architecture: study KV store and PUB/SUB

fold and reduce are very similar to each other, need to simplify the code

How to express arbitrary DGA? How to express loop?

Need to test the performance of apply function

Need to separate the shuffling from Context as a module/function

Interface to Irmin or HDFS to provide persistent storage

# ISSUE

Stack overflow in reduce function (caused by Hashtbl: when there are over 30000 values with the same key)
