.PHONY: clean publish build

build: clean
	./scripts/build.sh

clean:
	./scripts/clean.sh

publish: build
	./scripts/publish.sh $(ARGS)
