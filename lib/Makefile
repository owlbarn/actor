DEPS = str,unix,uint,ZMQ

EXAMPLES = client server

all: $(EXAMPLES)

% : %.ml
	ocamlfind ocamlc -package $(DEPS) -linkpkg -o $@ $^

clean:
	rm -f *.cm*
	rm -f $(EXAMPLES)
