DEPS = str,unix,uint,ZMQ

EXAMPLES = test_serialise test_context types manager actor client context dfs service

all:
	ocamlfind ocamlc -package $(DEPS) -o types types.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o utils utils.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o dfs types.cmo dfs.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o context types.cmo utils.cmo dfs.cmo context.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o actor types.cmo actor.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o service types.cmo service.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o manager types.cmo utils.cmo actor.cmo service.cmo manager.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o client types.cmo utils.cmo client.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_context types.cmo utils.cmo dfs.cmo context.cmo test_context.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_serialise test_serialise.ml

clean:
	rm -f *.cm*
	rm -f $(EXAMPLES)
