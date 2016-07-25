DEPS = str,unix,uint,ZMQ

EXAMPLES = client server actor test_serialise

all:
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o actor actor.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o client actor.cmo client.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o server actor.cmo server.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o test_serialise actor.cmo test_serialise.ml

clean:
	rm -f *.cm*
	rm -f $(EXAMPLES)
