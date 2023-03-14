.PHONY: clean publish build

build: clean
	./scripts/build.sh

install: build
	pip install .

clean:
	./scripts/clean.sh

publish: build
	./scripts/publish.sh $(ARGS)
