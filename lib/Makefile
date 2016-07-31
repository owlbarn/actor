DEPS = str,unix,uint,ZMQ

EXAMPLES = test_serialise test_context types manager actor client context dfs

all:
	ocamlfind ocamlc -package $(DEPS) -o types types.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o context types.cmo context.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o dfs types.cmo dfs.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o actor types.cmo actor.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o service types.cmo service.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o manager types.cmo actor.cmo manager.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o client client.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_context types.cmo context.cmo test_context.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_serialise types.cmo actor.cmo test_serialise.ml

clean:
	rm -f *.cm*
	rm -f $(EXAMPLES)
