DEPS = str,unix,uint,ZMQ,ocamlgraph

EXAMPLES = test_kmeans test_serialise test_wordcount test_context types utils manager worker context memory service dag

all:
	ocamlfind ocamlc -package $(DEPS) -o types types.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o utils types.cmo utils.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o dag types.cmo dag.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o memory types.cmo memory.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o context types.cmo dag.cmo utils.cmo memory.cmo context.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o worker types.cmo utils.cmo worker.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o service types.cmo service.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o manager types.cmo utils.cmo service.cmo manager.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_context types.cmo dag.cmo utils.cmo memory.cmo context.cmo test_context.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_serialise test_serialise.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_wordcount types.cmo dag.cmo utils.cmo memory.cmo context.cmo test_wordcount.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_kmeans types.cmo dag.cmo utils.cmo memory.cmo context.cmo test_kmeans.ml

clean:
	rm -f *.cm*
	rm -f $(EXAMPLES)
