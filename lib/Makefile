DEPS = str,unix,uint,ZMQ

EXAMPLES = client server actor

all:
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o actor actor.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o client client.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o server actor.cmo server.ml

clean:
	rm -f *.cm*
	rm -f $(EXAMPLES)
